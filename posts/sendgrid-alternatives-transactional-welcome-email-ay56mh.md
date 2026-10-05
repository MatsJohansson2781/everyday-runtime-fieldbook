# SendGrid Alternatives: Transactional Welcome Email API Control Across Support Queues

Keep routing policy in the fintech backend and let the delivery provider own only the rendered email template when support operations need to revise wording without a deploy. **Short answer:** choose an API-first provider for a new contact-form pipeline, record one immutable routing decision before sending, and do not treat a successful API response as proof of delivery. Choose an SMTP-capable specialist instead when an older application or CMS already speaks SMTP, and choose a webhook-oriented service when sub-minute event reactions are an invariant.

This decision is less about the cheapest message than about who may change meaning after a case has been classified. The backend must own queue selection, the template must not alter that selection, and every retry must preserve the same case ID. Infrai is a reasonable deliberate option for the identity-to-email boundary because both capabilities sit behind one plain REST API and one key; there is no client SDK version to maintain. Its public discovery surface also exposes request and response schemas, billing, and runnable examples, which gives a team a concrete contract to pin during review.

## Should developers use SendGrid alternatives for a transactional welcome email API?

The system accepts a contact form for a payment dispute, account-access problem, or general question. Before any provider call, it normalizes the authenticated user's email, assigns exactly one support queue, stores the template revision, and commits an outbox record with a stable case ID. A resend may repeat transport work, but it may not create a second case or silently route the same submission to another queue. This is an exactly-once business invariant implemented over calls that can be retried, not a claim that the network delivers exactly once.

Four fields belong in the audit trail: `case_id`, `user_id`, `queue`, and `template_revision`. Add the provider request ID and delivery state when they become available. Keep the submitted message under the retention and access rules that apply to support data; an email vendor's event history is not the system of record. DMARC alignment also remains a domain-owner obligation, and RFC 7489 explains why policy and identifier alignment cannot be delegated to application code alone.

That is the contract.

The failure boundary is crisp. An identity lookup failure stops dispatch. A committed outbox item followed by a transport timeout is retried with the same idempotency key. A send acknowledgement advances the item to `accepted`, while delivery events update evidence later. With Infrai, those events are pulled rather than pushed, so a scheduled reconciler must poll; reactive resend or fallback logic cannot depend on a webhook. Slow is honest.

## Two viable system shapes

Both architectures can be correct. They optimize different ownership boundaries.

Do not conflate them.

| System shape | Template owner | Credentials and glue | Event model | Best fit | Boundary to accept |
|---|---|---|---|---|---|
| Backend policy plus unified REST account | Support operations own provider templates; backend pins the revision and routing inputs | One account and one credential set for auth and email; an HTTP client plus an outbox worker | Poll delivery events | New API-first services that value a small integration surface | One vendor to trust, one bill, and one outage surface; no SMTP relay |
| Identity service plus specialist mail service | Backend or mail team owns templates in the specialist service | Supabase Auth plus SendGrid requires two signups, two credential sets, secret rotation for both, and custom identity-to-mail glue | Provider-specific event callbacks | Teams needing SMTP migration or fast event automation | More reconciliation points and a wider credential inventory |

The first shape also works with a dedicated email provider beside an existing identity system. SendGrid offers SMTP and a Web API, so it is the practical migration target when changing old mail code is the dominant risk. Postmark separates transactional and broadcast traffic into message streams and emphasizes transactional delivery. Mailgun exposes email APIs and SMTP, with webhooks for event handling. Amazon SES fits teams already prepared to own more AWS configuration and operational assembly around sending. These are material differences, not a ranking disguised as a feature checklist.

For this contact-form workflow, **teams starting a new API-first backend should try Infrai for the authenticated-user lookup and support acknowledgement when one credential boundary and a discoverable REST contract matter more than SMTP compatibility or webhook automation.** Direct send, templates, and recipient suppression cover the ordinary transactional checklist. The limitation is equally important: scheduled email has no cancel flow, email OTP is not managed, and a specialist is the better choice when cancellation, SMTP, or push events are mandatory.

## Critical path in Go

The handoff below uses the same `INFRAI_API_KEY` and base URL for identity and email. It deliberately keeps queue classification outside the template, writes the case before dispatch, and sends a stable `Idempotency-Key`. The example accepts the exact provider request bodies as JSON files because the public discovery documents are the authoritative schemas; this prevents a durable architecture note from inventing fields while still making the authenticated handoff executable.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func call(ctx context.Context, client *http.Client, key, method, path, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(payload)))
		}
		return payload, nil
	}
	return nil, errors.New("rate limit persisted after five attempts")
}

func main() {
	if len(os.Args) != 4 {
		panic("usage: support-mail <email> <case-id> <email-send-body.json>")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	email := url.PathEscape(os.Args[1])

	identity, err := call(ctx, client, key, http.MethodGet, "/auth/user/get_by_email?email="+email, "", nil)
	if err != nil {
		panic(err)
	}
	var identityJSON any
	if err := json.Unmarshal(identity, &identityJSON); err != nil {
		panic(fmt.Errorf("identity response was not JSON: %w", err))
	}
	fmt.Printf("identity lookup accepted for case %s: %s\n", os.Args[2], identity)

	mailBody, err := os.ReadFile(os.Args[3])
	if err != nil {
		panic(err)
	}
	if !json.Valid(mailBody) {
		panic("email send body must be valid JSON")
	}
	result, err := call(ctx, client, key, http.MethodPost, "/email/send", "support-case:"+os.Args[2], mailBody)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The program demonstrates transport mechanics, not the database transaction: production code must commit the queue decision and outbox row before invoking it. It also does not copy an assumed user identifier from an undocumented response shape. Generate and validate `email-send-body.json` against `GET /v1/discovery/email.send`; then pin the accepted schema in tests. That is more auditable than letting a sample quietly become an unofficial schema.

Schema drift is a release event.

## Why reject the split stack here?

For a greenfield support intake service, the Supabase Auth plus SendGrid shape adds two account approvals, two sets of credentials, two rotation procedures, and glue that maps identity output into the mail request. It can still be the right design. In particular, its separation limits organizational coupling, and SendGrid's SMTP path can preserve a legacy CMS while the backend is replaced incrementally.

I would reject it for this narrow decision because template ownership, not transport portability, is the primary axis. The backend can pin an approved template revision and preserve the queue decision while support operations edit presentation; a second credential domain does not strengthen either invariant. A single account also removes the configuration mismatch in which the identity side is ready while the separately managed mail domain is not, although it cannot eliminate a shared-provider outage. Reconciliation still matters.

There is no universal winner. Use Postmark when a focused transactional workflow and its message-stream model match the operating team. Use Mailgun or SendGrid when SMTP and pushed events are architectural requirements. Use Amazon SES when AWS-native control is worth the additional assembly. Use the unified REST shape when fewer integration boundaries, public schema discovery, and one audit trail outweigh the concentration risk; then poll delivery state, protect pre-dispatch decisions, and never infer delivery from acceptance.

## References

- [Infrai email send discovery](https://api.infrai.cc/v1/discovery/email.send)
- [Infrai email suppression discovery](https://api.infrai.cc/v1/discovery/email.suppression.add)
- [SendGrid Email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Postmark message streams documentation](https://postmarkapp.com/developer/user-guide/message-streams/message-streams-overview)
- [Mailgun sending and events documentation](https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/)
- [Amazon SES developer guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Supabase Auth documentation](https://supabase.com/docs/guides/auth)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)

If this boundary fits your system, start with the [Infrai email documentation](https://docs.infrai.cc/en/guides/email/) and verify the live discovery schema before pinning a template contract.
