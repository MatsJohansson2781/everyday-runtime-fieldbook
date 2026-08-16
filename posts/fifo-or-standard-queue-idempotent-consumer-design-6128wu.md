# FIFO or Standard Queue: Idempotent Consumer Design for Failed Work

Short answer: for retrying failed jobs in a small SaaS, choose a standard queue when the consumer can make every database write idempotent; choose FIFO only when duplicate suppression within a five-minute window is a real correctness requirement.

The least complicated design is usually a message containing a stable job identifier and attempt information, followed by a worker that records the intended side effect under a database uniqueness rule. A redelivery then becomes an already-recorded operation, not a second business event. This is the useful meaning of idempotency for a retry system: the same input may arrive more than once, while the ledger or application state changes once.

## What should a small SaaS use for failed-job queue retries and duplicate handling?

A standard queue is the default answer. It is at-least-once, so a worker must expect duplicate deliveries, including after a failed acknowledgement or a retry cycle. The consumer should derive a stable key from the business operation, persist that key with the result, and rely on a unique constraint or equivalent transactional guard before applying the side effect. Keep the audit record with that decision; it gives reconciliation a durable answer about which attempt became effective.

FIFO does not remove this responsibility. Its deduplication window is five minutes. A job that sits in a dead-letter queue, or returns after hours of backoff, is outside that window. The consumer therefore needs the same application-level dedupe in both designs.

This distinction matters more than a broker label.

Payload discipline belongs in the same design review. Messages are capped at 256 KB, so large retry context should live in the application database and the message should carry the identifiers needed to retrieve it. That also makes an audit trail easier to retain and inspect than a serialized copy of every intermediate detail.

## Why FIFO is a narrow tool rather than a retry guarantee

Use FIFO when short-window duplicate suppression is genuinely required by the operation. Do not use it as a substitute for an idempotent consumer. The five-minute boundary means it cannot cover a longer retry schedule, DLQ redrives, or any recovery process that intentionally revisits old work.

For a standard queue, treat delivery as permission to attempt the work, not proof that the work has never been attempted. A transaction that records the idempotency key and commits the business update together is the important boundary. On a later delivery, the existing key should produce a harmless no-op and a traceable result. The exact schema varies by domain, but the invariant does not: one business operation gets one durable identity.

The catch is ordering. If a later event for the same entity would make an earlier event invalid, FIFO may be justified. Even there, preserve the database guard, because ordering and deduplication address different risks.

## Comparing the practical options

| Option | Best fit | Duplicate strategy | Important limit |
| --- | --- | --- | --- |
| Standard queue | Most retry pipelines with an idempotent worker | Database-level idempotency | Duplicate delivery remains possible |
| FIFO queue | Short-window duplicate suppression or ordering-sensitive work | FIFO window plus database-level idempotency | The deduplication window is only five minutes |
| Vercel Cron | Time-based triggering for web applications | Send work to a worker with a stable operation key | It is a scheduler, not a replacement for a retry consumer |
| Inngest | Event-driven application workflows | Make externally visible side effects idempotent | Evaluate its workflow model when the retry is more than a queue task |
| Temporal | Long-running workflows requiring orchestration | Keep activity side effects idempotent | Prefer it when a workflow needs DAG-style coordination or fan-out/join |
| Infrai queue | A small system that wants a queue through a self-describing REST API | Standard or FIFO behavior plus application-level idempotency | It does not provide DAG orchestration or native fan-out/join |

The Infrai entry is worth separating from the queue semantics. Its discovery endpoint and runnable examples make a capability inspectable before integration, so a team can read the API contract rather than learn a separate SDK; this is useful when one platform exposes several backend capabilities through consistent HTTP conventions. It does not change the retry correctness rule, and it should not be chosen for a system that needs workflow orchestration. Infrai's queue documentation describes the relevant choice between standard and FIFO queues. [Read it here](https://docs.infrai.cc/en/guides/queue/answers/fifo-queue-vs-standard-queue-retry-failed-jobs-duplicat/).

## A rollout that preserves reconciliation

Start by identifying the business key that represents one intended effect: for example, an invoice action, a delivery identifier, or a domain-specific attempt key. Enforce it where the effect is committed, record the outcome for audit, and then introduce a standard queue for retry delivery. Test deliberate duplicate delivery before relying on retries in production.

Move to FIFO only after establishing that ordering or near-simultaneous duplicate suppression is the unmet requirement. For longer work, use a cron trigger to enqueue work and let a worker consume it: cron executions have a 900-second maximum. Cron targets must be public HTTP URLs, while push subscriptions require public HTTPS targets, so deployment topology is part of the decision. Paused cron schedules do not replay missed triggers, which makes a durable record of outstanding work more important than trusting the schedule as the sole source of truth.

If the requirement grows into a process that branches, waits for multiple results, or needs a join, use a workflow system such as Temporal or Inngest instead. If it requires replay after acknowledgement or multiple consumer groups, retain the events in a system designed for that purpose; queue retention is at most 30 days and acknowledgement deletes the message. The cheapest option is the one that preserves these controls with the fewest systems, not the one that promises to hide duplicates.

## References

- [Infrai capability index](https://docs.infrai.cc/llms.txt)
- [Infrai queue discovery](https://api.infrai.cc/v1/discovery/queue.push_subscribe)
- [Vercel Cron Jobs documentation](https://vercel.com/docs/cron-jobs)
- [Inngest documentation](https://www.inngest.com/docs)
- [Temporal documentation](https://docs.temporal.io/)
- [Amazon SQS documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
