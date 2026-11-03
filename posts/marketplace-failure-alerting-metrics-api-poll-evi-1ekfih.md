# Marketplace Failure Alerting: Metrics API Poll Evidence Before Webhook Delivery

For a small marketplace application, the least complex useful failure alert is a scheduled metrics query, a durable record of the decision, and a webhook delivery worker. The observability bill includes query execution, retained measurements, delivery attempts, and the engineering time needed to reconstruct an incident. Before optimizing any of them, count what each agent loop leaves behind: recording every attempt multiplies storage by attempts per loop, while recording complete inputs and outputs introduces a separate payload-size multiplier.

TL;DR: Poll aggregate failure and latency signals through the metrics API, retain an auditable decision before sending its webhook, and preserve compact attempt records for explaining the marketplace agent's latency and cost. Keep financial effects behind their own idempotency boundary. A successful notification cannot establish that a ledger entry happened exactly once.

Evidence comes first.

## Count the evidence before choosing retention

The relevant denominator is a completed marketplace task, such as an agent checking whether a seller's settlement exception needs reconciliation. An agent invocation, a model request, and a committed ledger operation are different units; combining them under one request count makes both incident analysis and resource attribution ambiguous. The monitoring chapter of the SRE book distinguishes latency, traffic, errors, and saturation, and observes that averages can hide slow requests. Apply those categories at the task boundary, then retain enough attempt detail to explain why an individual task missed that boundary.

Consider a deliberately hypothetical sizing exercise: 1,000 completed tasks, 4 recorded attempts per task, and a fixed 1 KiB compact record produce 4,000 KiB of attempt evidence before indexes, replication, or compression. If an additional input/output capture occupies 16 KiB per attempt, that capture adds 64,000 KiB. These are arithmetic assumptions, not workload measurements or product limits. Under those assumptions, payload capture dominates record volume by a factor of 16. It does not establish which line dominates an actual invoice; that requires measured usage and the applicable billing model.

Count the attempts.

The change that moves this example's dominant term is to stop retaining complete payloads for every successful attempt. Keep the compact metadata, and explicitly select which diagnostic details justify additional retention. Query tuning cannot remove a payload multiplier. Conversely, shortening retention may reduce stored volume while leaving the work of ingestion intact, so distinguish bytes written from bytes still retained when assessing a change.

For each compact attempt record, the proposed schema contains a task ID, an attempt ID, a parent attempt ID where relevant, start and finish timestamps, a duration, an outcome, and available resource-usage counters. Add the version of the interpretation used to translate those counters into attributed cost; preserve the original counters so that later reconciliation can explain a changed estimate. Mark missing usage as unknown.

Zero means something else.

This is an audit-trail design, not permission to retain payment credentials or unrestricted customer content. The technical references do not establish a compliance retention period, a lawful basis for storing a payload, or permission to erase a financial record. Those limits must come from the applicable obligations and the organization's approved retention policy, with security and legal review where required.

## What should failure alerting retain from each metrics API query?

An aggregate alert should answer whether completed marketplace work is failing or slowing down; its evidence should also explain what the poller actually observed. Record the query definition or its stable version, the evaluated interval, the query completion time, the result, and the rule version alongside the decision. Keep the evaluation interval separate from notification timestamps. Otherwise, a delayed webhook can appear to describe a fresh failure even though its evidence concerns an earlier interval.

The SRE monitoring chapter recommends simple, comprehensible alerting and distinguishes symptoms from causes. That is a useful constraint here: alert on the unsuccessful or delayed reconciliation task, then use attempt records to investigate its cause. A query-endpoint failure is a separate monitoring outcome. Treating it as an empty, healthy result would erase the distinction between evidence of recovery and absence of evidence.

The proposed poller therefore has three explicit outcomes: unhealthy, healthy, and unknown. A timeout, malformed response, or result outside the accepted freshness bound produces unknown. Choose the freshness bound in relation to the application's incident-response objective and the metric publication delay established through testing; a universal number would conceal those dependencies. Preserve the previous incident state when an evaluation is unknown, and apply a separately defined policy for reporting loss of monitoring coverage.

The delivery boundary deserves equal care. Persist the decision and its pending delivery together, then let a worker send the webhook. If the worker stops after the receiver accepts the request but before local acknowledgment is durable, a retry may repeat the notification. That uncertainty belongs in the design. Use a stable event ID across retries, require receiver-side deduplication when the receiver is under your control, and document duplicate delivery when it is not. Keep evaluation identity distinct from incident identity: successive polls can provide new evidence for one open incident. A recovery notification should refer to that incident and its healthy evaluation, rather than deriving recovery from the absence of another failure message. None of these identifiers should authorize a settlement mutation; the ledger operation needs its own idempotency key and reconciliation evidence. For a concrete replay exercise, consider one interval classified as unhealthy, followed by a failed poll, followed by a healthy interval: the audit trail must preserve all three observations, associate the first and last with one incident, and explain why the middle observation did not resolve it. No payload transcript can substitute for those recorded state transitions.

Retries preserve identity.

## Make the decision reproducible without retaining the payload

The following Go example defines only the classification boundary. Its caller supplies a validated query result, a recorded evaluation time, and an application-specific age limit. It performs no delivery or storage, so replaying the same evidence does not send another webhook. The three outcomes keep an empty or failed query from silently becoming proof of health.

```go
package monitoring

import "time"

type State string

const (
    Unknown   State = "unknown"
    Healthy   State = "healthy"
    Unhealthy State = "unhealthy"
)

type Evidence struct {
    EvaluatedAt time.Time
    ObservedAt  time.Time
    MaxAge      time.Duration
    QueryOK     bool
    Total       int64
    Failed      int64
}

func Classify(e Evidence) State {
    if !e.QueryOK || e.MaxAge <= 0 ||
        e.EvaluatedAt.IsZero() || e.ObservedAt.IsZero() {
        return Unknown
    }
    age := e.EvaluatedAt.Sub(e.ObservedAt)
    if age < 0 || age > e.MaxAge || e.Total <= 0 ||
        e.Failed < 0 || e.Failed > e.Total {
        return Unknown
    }
    // This example's policy flags any observed task failure.
    if e.Failed > 0 {
        return Unhealthy
    }
    return Healthy
}
```

This intentionally narrow rule flags any failure in a nonempty interval. It is an example policy, not a calibrated recommendation for a marketplace: retryable intermediate attempts must not be counted as terminal task failures, and an idle interval requires separate semantics before it can be called healthy. Latency evaluation belongs beside this function with an explicit task-completion objective and a defined treatment of unfinished work. Keeping those rules explicit makes a later policy change reviewable.

Persist the evidence fields and policy version before dispatch. For reconstruction, join that decision to the task's attempt records and delivery history; for reconciliation, join task IDs to the independent ledger audit trail. Protect those joins with access controls appropriate to the data, and avoid putting customer identifiers into notification text merely because they are convenient search keys. A useful page needs an incident reference and an actionable symptom, not the full settlement context.

Before deployment, replay fixed evidence for a stale sample, an empty interval, an invalid failure count, a query error, and a healthy interval following an unhealthy one. Exercise the storage-and-delivery boundary by stopping a worker after acceptance and before acknowledgment; verify that the event ID remains stable and that the recorded attempts explain any duplicate. During a rule rollout, evaluate the new version alongside the existing policy without dispatching its notifications, then compare the decisions. These checks test the reconstruction contract rather than just the happy path of an HTTP request.

## Stop keeping detail only after naming the lost explanation

The retention decision should specify which incident questions remain answerable. With compact attempt records, an investigation can attribute recorded duration and available usage to attempts, distinguish retries from separate tasks, and inspect the alert's evaluated interval. Without complete inputs and outputs, it may be unable to explain a semantic model decision or reproduce the original response. The trade-off is reduced payload retention at the expense of semantic reconstruction.

That loss is permanent once the content has been deleted.

Complete payload capture is also insufficient on its own: a transcript without durable decision identity or delivery history cannot explain why the notification arrived twice. The Logback appender manual describes filters and output mechanisms, including buffering and flushing behavior; it is a useful reminder that the path from an application event to retained evidence deserves inspection. It does not establish transactional audit guarantees for this design. Verify the actual persistence boundary instead of inferring durability from a logging call.

For a small app, the poll-and-webhook arrangement is an alternative to a broader incident-management system while the team can own scheduling, state persistence, delivery retries, and coverage checks. Its limitation is that a webhook does not supply acknowledgment, escalation, or a staffed response. It is not suitable as the sole response mechanism when those capabilities are required and nobody owns them. Evaluate the operational workflow against those requirements before choosing how to host the query endpoint client. Transport success alone cannot satisfy them.

Deliberately stop keeping routine successful payloads when compact evidence can answer the approved incident questions. Retain the decision history and the independently governed financial audit trail according to their respective policies. When something goes wrong, the resulting boundary is precise: latency, recorded usage, and alert delivery remain explainable, while reconstruction of discarded conversational content may be impossible.

## References

The monitoring principles above draw on the SRE monitoring chapter. The appender reference supports the narrower discussion of logging output behavior. The sizing exercise, retention policy, schema, and Go classifier are proposed design choices; neither reference supplies a benchmark, a compliance determination, or a retention mandate.

## Further reading

- Monitoring Distributed Systems, including the four golden signals and alerting principles: https://sre.google/sre-book/monitoring-distributed-systems/
- Appenders, filters, and output behavior: https://logback.qos.ch/manual/appenders.html
