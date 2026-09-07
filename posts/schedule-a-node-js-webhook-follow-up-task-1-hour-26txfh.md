# Schedule a Node.js Webhook Follow-up Task 1 Hour Later: Delayed Queue or Cron

Short answer: use one durable delayed message for each webhook event, backed by a database row and an idempotent consumer; use cron as the recovery sweep, not as a newly created schedule for every event. A one-hour delay is an event deadline, so the design should preserve the event's intent even when a worker, process, or broker is restarted.

The bill is mostly operational attention, not the timer itself. A per-event cron entry creates a record that must be created, observed, deleted, and reconciled for every webhook. A delayed queue keeps one message until it becomes visible, then a worker acknowledges it. The durable database row is the retained audit fact; the queue message is a delivery hint. Keeping both costs storage and a small sweep job, but dropping the row makes a missing follow-up indistinguishable from a successfully completed one.

I approach this from payment and ledger systems, where “exactly once” is a business requirement even though transport is usually at least once. The useful target is therefore exactly-once effect: a duplicate delivery may happen, but it cannot produce a duplicate state transition or charge.

## The one-hour deadline is an accounting record

Choose a delayed queue message when the rule is “one hour after this webhook.” Choose cron when the rule is “check every hour for all work that is due.” Those are different clocks. A cron schedule per event turns an unbounded stream of customer actions into an unbounded set of control-plane objects, while a queue gives each event a bounded lifecycle.

The ordering matters. In the webhook transaction, insert `scheduled_follow_ups` with the provider event ID as a unique key, a `due_at` timestamp, and a state such as `pending`. Commit that transaction. Only then publish the delayed message. If publishing fails, the row remains visible to the recovery sweep; if publishing is retried, the same idempotency key prevents a second logical follow-up where the broker supports deduplication.

Here is the shape in Go. The endpoint is intentionally generic: the transport can be Redis-backed, cloud-hosted, or self-managed, while the application contract stays the same.

```go
package followup

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type DelayedPublisher interface {
	Publish(ctx context.Context, key string, payload []byte, delay time.Duration) error
}

func RecordWebhook(ctx context.Context, db *sql.DB, p DelayedPublisher, eventID string, payload []byte) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// The unique event_id makes webhook retries harmless at the database boundary.
	_, err = tx.ExecContext(ctx, `
		INSERT INTO scheduled_follow_ups (event_id, due_at, state, payload)
		VALUES ($1, $2, 'pending', $3)
		ON CONFLICT (event_id) DO NOTHING`,
		eventID, time.Now().UTC().Add(time.Hour), payload)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	if err := p.Publish(ctx, "followup-"+eventID, []byte(eventID), time.Hour); err != nil {
		return fmt.Errorf("publish wake-up for %s: %w", eventID, err)
	}
	return nil
}
```

The worker reads the row and claims it in a short transaction. It should check the state before doing side effects, record an outcome, and make the transition conditional on the prior state. A redelivery then becomes a no-op. Keep payloads small; store a document or rendered reply elsewhere and queue its identifier.

## What makes a one-hour delayed webhook task auditable?

Treat the database as the source of truth. Record who or what created the follow-up, the event ID, `due_at`, attempt count, completion time, and an immutable outcome. An audit trail should answer three questions without consulting queue retention: what was owed, what was attempted, and what changed.

The recovery query can run every hour or every few minutes. PostgreSQL's row locks let several workers divide the work without a central coordinator:

```sql
WITH due AS (
    SELECT id
    FROM scheduled_follow_ups
    WHERE state = 'pending' AND due_at <= now()
    ORDER BY due_at
    FOR UPDATE SKIP LOCKED
    LIMIT 100
)
UPDATE scheduled_follow_ups s
SET state = 'claimed', attempts = attempts + 1
FROM due
WHERE s.id = due.id
RETURNING s.id, s.event_id, s.payload;
```

This is a recovery mechanism, not permission to ignore delivery metrics. Alert on rows past `due_at`, publish failures, consumer lag, and the gap between “row committed” and “message published.” Reconciliation should compare the intended state with the external side effect; a green queue dashboard alone cannot prove that a customer-support reservation expired.

## How do delayed queues, cron, and workflow timers differ?

The choice is mostly about the shape of time and the failure you are prepared to operate.

| Mechanism | Best fit | Operational boundary |
| --- | --- | --- |
| Delayed queue message | One follow-up per webhook event | Delivery is commonly at least once; consumer idempotency is required |
| Fixed cron sweep | Periodic scan of durable `due_at` rows | Precision depends on sweep interval and query/index health |
| Workflow timer | Multi-step process with waits, joins, and retries | More runtime concepts, state, and operational cost |
| Database-only polling | Small volume and few dependencies | Polling load and lock contention become the scaling concern |

Several mainstream implementations expose the same conceptual trade-off. BullMQ stores delayed jobs in Redis, so Redis memory and worker recovery are part of capacity planning. Celery's `countdown` schedules a task through its broker and workers, which still leaves acknowledgment and duplicate-effect handling to the application. Cloud Tasks can schedule HTTP delivery for much longer than an hour, but its queue dispatch quotas and IAM become part of the design. Raw SQS delay queues cap `DelaySeconds` at 900 seconds, so a one-hour deadline needs another timer or a database sweep. These are boundaries, not defects; select based on the infrastructure your team already monitors.

## Where is this pattern the wrong pick?

The catch is retention. A queue that deletes a message after acknowledgment is not an audit archive. If a support reservation can be disputed for 120 days, retain the business event and outcome in a database or append-only log for that period, even if the delayed message lived for only an hour.

It is also unsuitable for long horizons beyond the queue's delay ceiling, for payloads that approach common message-size limits, or for workflows that wait, fan out, join results, and then post a ledger entry. Use a durable `due_at` row plus cron for a 90-day deadline. Use a workflow engine for a graph of dependent actions. Keep the delayed message for the flat, one-hop case.

Your mileage may vary on timing precision. A queue's “one hour” is normally a visibility guarantee, not a hard real-time promise; measure lateness at the worker and choose a tighter sweep interval when customer policy requires it.

## A decision rule for delivery guarantees

Start with the effect, then choose the timer. If repeating the follow-up is harmless, at-least-once delivery with an idempotent handler is sufficient. If repeating it could alter a balance, send a notification twice, or close a case incorrectly, make the state transition conditional and retain an audit record before acknowledging the message.

I once assumed that a successful webhook response implied a scheduled follow-up. It did not: the response only acknowledged receipt. The correction was simple but consequential—persist the obligation first, publish second, and reconcile both facts. That three-word rule is worth keeping visible: persist, publish, reconcile.

## References

- https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- https://www.postgresql.org/docs/current/sql-select.html
- https://docs.bullmq.io/guide/jobs/delayed
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html
- https://cloud.google.com/tasks/docs/creating-http-target-tasks

## Further reading

- https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- https://www.postgresql.org/docs/current/sql-select.html
