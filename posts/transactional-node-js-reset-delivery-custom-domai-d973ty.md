# Transactional Node.js Reset Delivery: Custom Domain Email Setup and DMARC Evidence

Short answer: for a property-management password reset with a short expiry, integrate around one deadline-aware send command and one normalized feedback stream. Configure SPF, DKIM, and DMARC as deployment prerequisites, then keep bounce and complaint suppression outside the request handler. The least integration work is not the fewest initial lines of Node.js; it is the smallest boundary that can still explain an uncertain submission, reject a late retry, and prevent another message after a complaint.

This decision accepts at-least-once execution around the mail transport while demanding exactly-once transitions where the application has authority: token redemption, event ingestion, and suppression changes. A transport timeout cannot prove that no message left. A database constraint can prove that the same complaint was not applied twice.

## How should Node.js custom domain email deliverability setup handle feedback?

Start with time, because the credential's validity window is the failure budget. The server stores one expiration instant and checks it during redemption; queue delay, a worker restart, or a later delivery observation cannot move that instant. A worker must decline to submit once the deadline has passed. It should also stop a retry when the remaining time is too short to be useful under the property's stated recovery policy, rather than deliver a link that is already stale when the resident reads it.

No retry can change that.

Three other invariants narrow the integration surface. The public reset endpoint returns the same response regardless of whether the address belongs to a resident. Logs and message metadata never contain the bearer secret. Every intent, attempt, transport acknowledgment, feedback observation, suppression change, and redemption can be joined by an immutable internal key, while the recipient address is protected according to the organization's retention and access rules.

Sender authentication is a separate invariant. DMARC evaluates alignment between the domain in the visible `From` field and a domain authenticated by SPF or DKIM; Sections 3.1 and 3.2 of RFC 7489 define DKIM and SPF identifier alignment, while Section 6.3 defines the requested receiver policies `none`, `quarantine`, and `reject`. Application code cannot repair a misaligned envelope identity or DKIM signing domain after submission, so the DNS records and the actual production message path belong in a release check.

Delivery is still outside the transaction. Keep that limit explicit.

## Decision record: spend integration effort at the two irreversible boundaries

**Status: accepted.** The Node.js API writes the reset record and an outbox intent in one local transaction. A worker submits the message only while the reset is eligible. A feedback adapter, implemented as either an authenticated webhook consumer or a cursor-based poller, converts external bounce and complaint records into a small internal vocabulary before committing them. Suppression is checked immediately before every submission.

This shape concentrates custom work at two irreversible boundaries: leaving the database to submit mail, and admitting external feedback into local policy. Rendering, transport calls, webhook signatures, polling cursors, and provider-specific status values stay behind adapters. Account-recovery code sees `temporary_failure`, `permanent_failure`, and `complaint`, not a changing catalog of remote strings.

That is the boundary.

| Integration shape | First implementation | Uncertain submission | Bounce and complaint work | Long-term coupling |
|---|---:|---|---|---|
| Send inside the reset HTTP request | Small | Mixed with request failure | Usually added later | Handler knows transport behavior |
| Transactional outbox plus normalized feedback | Moderate | Preserved as an attempt awaiting evidence | One adapter and one policy boundary | Recovery logic stays transport-neutral |
| Operate the complete SMTP and feedback path | Large | Fully owned by the team | Fully owned by the team | Low external coupling, high operational scope |

The middle option minimizes lifetime integration effort for this case. It adds a table, a worker, and feedback ingestion now, but avoids spreading remote semantics across the reset endpoint, retry scheduler, support tooling, and audit queries. This is a trade: more deliberate schema work in exchange for fewer places where transport behavior can alter account security.

The evidence model need not imitate a general-purpose event platform. An intent records the internal message key, recipient reference, template revision, creation time, and reset expiry. Attempts append their start time and outcome without overwriting earlier ambiguity. Observations carry a source identifier, normalized class, observed time, and the minimum retained raw evidence required for investigation. A unique constraint on `(source, source_event_id)` makes replay harmless, while a suppression history records who or what added or released a block.

Polling deserves particular care because convenience can hide data loss. Insert a fetched page of observations and advance its durable cursor in the same local transaction. If the process dies before commit, it fetches the page again; deduplication absorbs the replay. If a source provides neither stable event identifiers nor a durable cursor, record that evidentiary limitation and use a conservative composite key rather than claiming exact processing.

## Put the deadline and suppression check on the critical path

The following Go code specifies the worker behavior even though Node.js owns the public API. This is deliberate: the contract is the architectural artifact, not a package tutorial, and all transport-specific work fits behind `Sender`. `Reserve` must use a lease so a crashed worker can be retried; it must not erase prior attempts.

```go
package resetmail

import (
	"context"
	"errors"
	"time"
)

var ErrNoLongerEligible = errors.New("reset mail is no longer eligible")

type Intent struct {
	Key       string
	Recipient string
	ResetURL  string
	ExpiresAt time.Time
}

type Receipt struct {
	TransportID string
	AcceptedAt  time.Time
}

type Sender interface {
	Submit(context.Context, Intent) (Receipt, error)
}

type Store interface {
	Reserve(context.Context, string, time.Time) (bool, error)
	IsSuppressed(context.Context, string) (bool, error)
	AppendAttempt(context.Context, string, time.Time, error) error
	AppendReceipt(context.Context, string, Receipt) error
	AppendSkip(context.Context, string, time.Time, string) error
}

type Worker struct {
	Now    func() time.Time
	Store  Store
	Sender Sender
}

func (w Worker) Send(ctx context.Context, in Intent) error {
	now := w.Now().UTC()
	reserved, err := w.Store.Reserve(ctx, in.Key, now)
	if err != nil || !reserved {
		return err
	}

	if !now.Before(in.ExpiresAt) {
		_ = w.Store.AppendSkip(ctx, in.Key, now, "expired")
		return ErrNoLongerEligible
	}

	blocked, err := w.Store.IsSuppressed(ctx, in.Recipient)
	if err != nil {
		return err
	}
	if blocked {
		_ = w.Store.AppendSkip(ctx, in.Key, now, "suppressed")
		return ErrNoLongerEligible
	}

	receipt, err := w.Sender.Submit(ctx, in)
	if err != nil {
		_ = w.Store.AppendAttempt(ctx, in.Key, now, err)
		return err
	}
	return w.Store.AppendReceipt(ctx, in.Key, receipt)
}
```

There is an unavoidable gap between `Submit` and `AppendReceipt`. The remote side may accept the message and the worker may then lose its database connection. Reusing the internal key as a transport idempotency key can reduce duplicate submission when the selected transport supports that contract, but the journal must still preserve uncertainty. Do not convert every timeout into “not sent,” and do not delete an ambiguous attempt when a later receipt arrives.

Exactly-once thinking remains useful here, provided it is attached to enforceable state transitions rather than to the network. Redemption updates a single-use reset record conditionally. Feedback insertion has a uniqueness constraint. Suppression changes are append-only and attributable. Those are auditable claims; “the email was sent exactly once” is not.

## Authenticate the domain, then test states rather than opens

Before enabling the worker, resolve the intended SPF, DKIM, and DMARC records from outside the administrative DNS path, send a controlled message through the production route, and inspect the received authentication results. A dedicated transactional subdomain can make ownership and reporting clearer, but its envelope identity, DKIM signing identity, and visible `From` domain still need the intended DMARC alignment. Roll policy out under the organization's risk process and review aggregate reports for legitimate senders that were omitted.

The application test matrix should follow transitions: two workers reserve the same intent; submission times out after possible acceptance; the same complaint arrives twice; a feedback event arrives before its receipt; the poller commits observations but crashes before advancing its cursor; and the worker wakes exactly at expiration. One property matters throughout: reordering or duplicating observations must never make an expired, redeemed, or suppressed reset eligible again. Do not make opens part of that property. Apple states that Mail Privacy Protection prevents senders from learning whether a recipient opened a message and can download remote content in the background. For a password reset, redemption before expiry is the application outcome; bounce and complaint records are delivery-policy evidence. Operational signals should instead point to a boundary: age of the oldest eligible intent, attempt outcomes, feedback lag, polling cursor age, duplicate observation count, suppressions by reason, and redemptions before expiry. Avoid a single synthetic “deliverability score,” because it cannot distinguish a stalled worker from authentication failure or a resident who chose not to reset a password. Auditability also has a compliance limit: email addresses, timestamps, network metadata, and raw feedback payloads may be personal data, so retention, access, and deletion rules must follow the applicable legal and security obligations. An audit trail is a reason to preserve necessary evidence, not permission to retain every payload indefinitely.

An open pixel is neither delivery proof nor security evidence.

## The rejected shortcut still has a valid use case

Sending inline from the Node.js request handler was rejected for resident account recovery. It saves the worker and outbox initially, yet binds HTTP latency and error handling to an operation whose acceptance can remain ambiguous, and it leaves suppression ingestion as a second integration that must later be reconciled with request logs. The short credential lifetime makes delayed retries security-relevant rather than merely inconvenient.

The shortcut is valid for a low-consequence internal notification when loss is explicitly acceptable, duplicate delivery has no security effect, no suppression policy is required, and the team does not need a durable explanation of each outcome. Operating the complete mail stack is also valid when an organization already has the personnel, reputation management, feedback processing, and on-call structure to own it. Neither alternative is intrinsically wrong; each moves work to a boundary that this property-reset workflow cannot ignore.

For the reset flow, keep the accepted boundary narrow and the evidence durable. Review the decision if credential lifetime, feedback guarantees, legal retention, or organizational ownership changes. Until then, the outbox plus normalized feedback adapter is the smallest integration that supports honest reconciliation.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- Apple, Use Mail Privacy Protection: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
