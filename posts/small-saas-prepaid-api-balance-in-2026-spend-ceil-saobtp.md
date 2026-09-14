# Small SaaS Prepaid API Balance in 2026 (Spend Ceilings Before Recharge)

Use bounded automatic recharge for ordinary demand, retain a hard per-day spend ceiling, and require manual approval only after that ceiling would be crossed. For a small gaming SaaS that issues one scoped key per tenant, this hybrid policy accepts a measurable amount of refused traffic in exchange for containing a leaked key; a manual-only policy makes the opposite trade and usually puts an operator on the availability path.

That is the architecture decision. The recharge threshold is not a finance-team preference or a round balance number. It is the amount of prepaid capacity needed to survive detection delay, provider settlement delay, and a conservative traffic burst. The daily ceiling is different: it is the maximum loss the business has authorized before the system must refuse more work. Mixing those two controls produces either needless denials or an open-ended replenishment loop.

## Decision record: separate continuity from loss containment

The concrete system is a multiplayer game backend serving many small studios. Each tenant receives a scoped key for a defined set of API capabilities; the upstream credential remains inside the gateway. Key issuance, rotation, and revocation are recorded as security events, while every accepted unit of usage is charged to a tenant sub-ledger before it can consume the shared prepaid balance. A tenant key can therefore be revoked without changing every other tenant's credential, and a noisy tenant can be refused without pretending that the shared provider balance has disappeared.

Three invariants govern the design:

1. A usage authorization never succeeds unless both the tenant budget and the platform's funded balance can cover it.
2. One logical recharge command can create at most one ledger credit, even if workers retry it.
3. Every key lifecycle change and recharge decision has an actor, reason, policy version, correlation ID, and immutable timestamp.

The second invariant is an exactly-once *effect*, not a claim of exactly-once networking.

Retries happen.

Requests can be duplicated, acknowledgements can be lost, and workers can restart between a provider response and a local commit. The defensible mechanism is an idempotency key derived from the accounting day and recharge sequence, a uniqueness constraint at the ledger boundary, and reconciliation against the provider's statement. HTTP itself defines idempotent methods, but payment-like commands still need application-level deduplication because method semantics cannot make two separate business records identical.

Keep the boundaries sharp. The balance observer may be stale and is therefore advisory; the append-only ledger is authoritative for the daily ceiling. The authorizer owns admission. The key service owns issue and revoke operations. The recharge worker may request funds, but it cannot raise its own ceiling. That division matters during an incident — the same component that detects falling capacity must not be able to redefine acceptable loss.

## How should a small SaaS set prepaid API balance recharge thresholds and daily ceilings?

Start with time, not currency. Let `burn_rate` be a high but plausible rate of balance consumption, `recharge_latency` the measured time from a valid command to spendable balance, `detection_delay` the monitoring interval plus alert processing time, and `reserve` the capacity held for critical operations such as session settlement. A useful policy shape is:

`trigger_threshold = burn_rate * (recharge_latency + detection_delay) + reserve`

This isn't a universal constant. I'm not sure a single percentile is defensible across launch day, a routine weekday, and a tournament weekend; replaying several weeks of timestamped usage against candidate thresholds is what resolves that uncertainty. Use the smallest threshold that meets the documented refusal budget under those traces, then test a delayed recharge and a sudden burst. Do not quietly widen it after an alert.

Set the daily ceiling independently from a loss review. It should cover expected legitimate consumption plus an explicitly approved margin, while remaining below the amount the business is prepared to expose to a compromised tenant key. Define the accounting timezone and rollover rule in the policy; UTC avoids daylight-saving ambiguity, but it does not remove the need to decide what happens to a request racing midnight. The ledger transaction should assign that request to one day exactly once.

For illustration, suppose a tenant's test fixture uses a threshold of 250,000 usage units, a fixed recharge of 1,000,000 units, and a 2,000,000-unit daily ceiling. Those are example values, not market facts. If the first recharge has already consumed half the daily allowance, a second equal recharge may proceed; a third must be refused or held for approval. The public error should be stable, such as `TENANT_DAILY_CEILING`, while internal telemetry records the policy version and remaining allowance.

No ambiguity.

Threshold crossing also needs hysteresis. Without it, two observers can see the same low balance and enqueue duplicate work, or a balance oscillating around the boundary can generate repeated commands. Electing one worker helps, but the ledger uniqueness constraint is the actual financial control. A recharge intent moves through `pending`, `confirmed`, or `declined`; retries reuse its identifier, and reconciliation can later prove whether the provider credit and local credit agree.

## Compare the operating models on the real decision axis

The relevant choice is not “automation or control.” Each model places a different bound on refused traffic and authorized spend.

| Model | Continuity behavior | Spend boundary | Operator burden | Best fit |
| --- | --- | --- | --- | --- |
| Manual top-ups only | Traffic may be refused until a person responds | Every increase receives human approval | High and time-sensitive | Low-volume, noncritical workloads with staffed funding windows |
| Uncapped automatic recharge | Ordinary demand continues while funding succeeds | No local hard stop on repeated recharge | Low before an incident, potentially high during one | Not suitable for tenant keys whose compromise can create unbounded consumption |
| Bounded automatic recharge plus manual exception | Ordinary demand continues below the ceiling; excess is refused | Enforced per accounting day, with a separate approval path | Moderate and predictable | Small SaaS workloads that need continuity but cannot accept unlimited loss |

The hybrid model wins here because gameplay traffic has an availability cost, yet a leaked key can turn availability automation into a loss multiplier. The catch is deliberate: once the hard ceiling is exhausted, some valid calls will be refused until the next accounting day or an authorized exception. A team that cannot tolerate that refusal should not erase the ceiling; it should fund a larger pre-approved envelope, isolate critical calls into a separately governed reserve, or move to a contractual credit arrangement whose exposure has been reviewed.

Manual-only funding is the rejected option for this system, but it remains the cleaner choice when usage is rare, deadlines are soft, and a finance operator is reliably available. Stick with it when one delayed batch is cheaper than building and auditing an automated money-moving path. An architecture decision record should preserve that valid use case so a future team does not mistake today's workload assumptions for a permanent truth.

## Put the critical path in one transaction

The following Go sketch models policy evaluation, not a provider integration. Units are deliberately abstract: the same control works with calls, tokens, or internal credits, provided the ledger and provider statement use a documented conversion. The caller must run `Decide` inside the same serializable transaction that writes the recharge intent; the unique intent ID then makes a retry return the existing decision rather than add another credit.

```go
package funding

import (
	"errors"
	"fmt"
	"time"
)

var ErrDailyCeiling = errors.New("TENANT_DAILY_CEILING")

type Policy struct {
	ThresholdUnits int64
	RechargeUnits  int64
	DailyCeiling   int64
	Version        string
}

type Snapshot struct {
	TenantID       string
	AvailableUnits int64
	FundedToday    int64
	AccountingDay  string
}

type Intent struct {
	ID             string
	TenantID       string
	Units          int64
	PolicyVersion  string
	Reason         string
	CreatedAt      time.Time
}

func Decide(p Policy, s Snapshot, now time.Time) (*Intent, error) {
	if s.AvailableUnits > p.ThresholdUnits {
		return nil, nil
	}
	if p.RechargeUnits <= 0 || s.FundedToday+p.RechargeUnits > p.DailyCeiling {
		return nil, ErrDailyCeiling
	}

	return &Intent{
		ID:            fmt.Sprintf("%s:%s:%d", s.TenantID, s.AccountingDay, s.FundedToday),
		TenantID:      s.TenantID,
		Units:         p.RechargeUnits,
		PolicyVersion: p.Version,
		Reason:        "balance_at_or_below_threshold",
		CreatedAt:     now.UTC(),
	}, nil
}
```

The sample ID is readable for teaching purposes; production code should enforce a database uniqueness key over the tenant, accounting day, and sequence rather than trust formatting alone. A successful provider acknowledgement does not immediately justify serving traffic. First persist the confirmed external reference, append the matching ledger entry, and make the updated funded balance visible atomically. If the process loses an acknowledgement, query by the same idempotency key or reconcile before sending another command.

Key operations belong beside this path but not inside the recharge worker. Issuance stores only a hash or identifier needed for lookup, displays secret material once, applies the tenant's scopes, and emits an audit event. Revocation changes authorization state before cache invalidation is broadcast, so a stale edge cache cannot extend access by design. OWASP's secrets guidance supports central lifecycle management, rotation, expiration, revocation, and auditable access; it also argues against logging secret values. An audit record should therefore name the key ID, never the key.

## Failure drills, observability, and compliance limits

Test the controls as state transitions rather than happy-path handlers. Run concurrent workers against the same threshold crossing and verify one ledger credit. Drop an acknowledgement and verify the retry resolves to the existing intent. Race a request against the UTC rollover. Revoke one tenant key while another tenant continues. Replay a day at ten times the normal request arrival rate — an intentionally synthetic stress case, not a capacity claim — and verify that accepted usage never exceeds the recorded authorization envelope.

Observability must connect an authorization decision to its consequences without exposing credentials. Useful dimensions include tenant ID, key ID, policy version, accounting day, intent ID, decision code, observed balance age, and reconciliation status. Keep key material, payment data, and request payloads out of metric labels and logs. OpenTelemetry trace context can connect the admission, ledger, and recharge spans, but the trace is diagnostic evidence; the ledger remains the financial record.

Auditability has limits. An append-only event stream can show which policy made a decision, yet it does not prove the upstream provider posted the same amount. Only reconciliation supplies that evidence. Likewise, idempotency prevents a duplicated logical command from creating two local credits, but it cannot repair an incorrectly configured ceiling or an overbroad key scope. Configuration changes need review, versioning, and a before-and-after record. Depending on what the platform stores or transmits, contractual and regulatory obligations may impose retention, access-control, and payment-data requirements beyond this design; counsel and the relevant compliance owner must define those boundaries.

The deployment order should preserve refusal safety: ship ledger fields and read compatibility first, then shadow policy decisions without moving funds, compare shadow results with historical demand, enable bounded recharge for a small tenant cohort, and only then make denial codes visible to clients. Rollback disables new recharge intents but leaves reconciliation running. Otherwise the team can lose the very evidence needed to close the accounting day.

For this gaming SaaS, the final rule is concise: automate below a reviewed loss boundary, refuse above it, and make exceptions explicit. Availability remains measurable, spend remains bounded, and every scoped-key or balance transition leaves enough evidence to reconstruct the decision.

## References

- OWASP, “Secrets Management Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- RFC 9110, “HTTP Semantics”: https://www.rfc-editor.org/rfc/rfc9110.html
- OpenTelemetry, “Trace API”: https://opentelemetry.io/docs/specs/otel/trace/api/
- NIST SP 800-53 Rev. 5, “Security and Privacy Controls for Information Systems and Organizations”: https://csrc.nist.gov/pubs/sp/800/53/r5/upd1/final
