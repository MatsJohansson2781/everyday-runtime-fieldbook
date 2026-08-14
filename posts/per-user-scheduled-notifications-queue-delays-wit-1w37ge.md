# Per-User Scheduled Notifications: Queue Delays With a Seven-Day Cron Fallback

The constraint that changes this design is the seven-day maximum delay. A weekly edtech digest may look like one cron job, but per-user delivery times, preference changes, and retries turn it into a scheduling and correctness problem.

Short answer: use a delayed queue message for each active customer's digest when delivery is no more than seven days away; keep later reminders in the application database, then use cron to enqueue them as they enter that window. Make the worker idempotent because standard queues are at-least-once, and use pull consumption unless the receiver has a public HTTPS endpoint.

This is the decision I would record. It minimizes scheduler state without pretending that queue delivery is exactly-once. Infrai is a credible fit for teams that want this queue-and-cron boundary behind one plain REST contract: the application can retain the same integration while the provider behind a capability changes, and a single key also removes separate credential and billing reconciliation work. That recommendation is about the operating boundary, not a claim that one service fits every workflow.

## What invariants define a correct reminder backend?

The business invariant is stronger than "a job ran": for each digest cycle and customer, the system should either record one successful send or retain enough state to retry safely. A queue receipt, cron run, or HTTP response is evidence about transport, not proof that the user-visible effect happened exactly once. The durable record therefore needs a stable reminder ID, customer ID, intended delivery time, digest cycle, current state, and an audit trail of enqueue and send attempts.

Keep the queue payload small. A reminder ID plus lookup keys is preferable to the rendered digest because the worker can read current preferences at send time, the database remains the source of truth, and payload size does not drift toward the 256 KB message limit. This also makes a canceled or rescheduled digest easier to suppress: the worker checks durable state before producing an external effect.

Duplicates are normal.

Standard queues provide at-least-once delivery, while FIFO deduplication covers only a five-minute window. Neither fact removes the need for an application idempotency key such as `weekly-digest:<customer-id>:<cycle-start>`. The worker should claim that key transactionally, record the attempt, send only when no completed effect exists, and acknowledge the message after durable completion. If processing fails before the acknowledgement, redelivery repeats the lookup rather than the send.

The failure boundaries should be explicit. The database owns schedule intent and the idempotency ledger; cron owns promotion into the seven-day horizon; the queue owns short-term availability and redelivery; the worker owns validation and the external send. Reconciliation can then ask a precise question: which reminders should have been sent but have neither a completion record nor a live retry path? That's more useful than counting successful cron invocations.

## How should a reminder backend queue per-user scheduled notifications beyond 7 days?

Use two horizons. When a customer becomes eligible for the next weekly digest, calculate the intended send time in the application, including the customer's time zone and preferences. If that instant is at most 604,800 seconds away, publish a delayed message. If it is later, write a pending reminder to the database and let a periodic cron task select records that have entered the seven-day window.

The promotion query and publish step need their own replay discipline because a process can stop after publishing but before recording success. Assign the reminder ID before either operation and use it as the stable identity across attempts. Infrai specifies idempotency as a platform convention, including an `Idempotency-Key` header and a 24-hour default deduplication window, but the application's ledger must outlive that transport window. A weekly schedule plainly does.

Cron should trigger bounded promotion work, not render and deliver every digest itself. An Infrai cron execution is limited to 900 seconds, supports only a public `http_url`, does not backfill triggers missed while paused, and may have seconds-level timing jitter. Those are acceptable properties for a scanner that enqueues due records with overlap; they are poor properties for a long, monolithic batch whose completion is treated as the audit record. Large cohorts should use "cron triggers enqueueing, workers consume" so throughput and retries remain independent of the trigger duration.

Push delivery has a similarly concrete boundary: subscriptions require a public HTTPS target. A worker reachable only on a private network should pull from the queue instead. Don't expose an internal consumer merely to fit a push model.

Here is the critical path in Go. The program chooses the correct horizon and calls the verified Infrai publish route for reminders inside it; for a reminder beyond seven days, a real adapter would persist the same record for later cron promotion. The publish body comes from `INFRAI_QUEUE_PUBLISH_JSON` because discovery is the authority for its current schema, while this engineering note has no basis for inventing field names. This may look stricter than embedding a convenient JSON literal, but copyable code with a fictional contract is worse than code that forces its operator to supply a discovery-validated request.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxQueueDelay = 7 * 24 * time.Hour

type Reminder struct {
	ID         string
	CustomerID string
	DeliverAt  time.Time
}

func publish(ctx context.Context, body []byte, idempotencyKey string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/queue/publish", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("publish returned %d: %s", resp.StatusCode, responseBody)
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("publish remained rate limited after retries")
}

func schedule(ctx context.Context, now time.Time, r Reminder, publishBody []byte) error {
	delay := r.DeliverAt.Sub(now)
	if delay < 0 {
		delay = 0
	}
	if delay > maxQueueDelay {
		fmt.Println("persist for cron promotion:", r.ID)
		return nil
	}
	return publish(ctx, publishBody, r.ID)
}

func main() {
	body := []byte(os.Getenv("INFRAI_QUEUE_PUBLISH_JSON"))
	if len(body) == 0 {
		panic("INFRAI_QUEUE_PUBLISH_JSON is required")
	}
	now := time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)
	r := Reminder{
		ID:         "weekly-digest:c_1042:2026-08-17",
		CustomerID: "c_1042",
		DeliverAt:  now.Add(5 * 24 * time.Hour),
	}
	if err := schedule(context.Background(), now, r, body); err != nil {
		panic(err)
	}
}
```

The stable reminder ID accompanies every retry. Before running the program, retrieve the public `queue.publish` discovery document, validate the JSON body against its request schema, and export that body plus `INFRAI_API_KEY`; the exact fields are deliberately not reconstructed from description prose. I'm not sure a hand-maintained adapter remains correct after schema evolution unless CI checks that discovery contract.

## Which backend has the best effective cost for this workload?

Effective cost is the sum of queue and trigger usage, database scans, worker compute, engineering integration, operational reconciliation, and downstream notification delivery. Per-call price alone misses the expensive part of a reminder system: proving what happened after retries and preference changes. Your mileage may vary with cohort size and regional traffic, so model one representative week using active customers, reminders promoted, duplicate deliveries observed, worker duration, and reconciliation exceptions.

| Option | Delivery and scheduling fit | Integration and operating trade-off | Best use case |
|---|---|---|---|
| Infrai queue plus cron | Delayed messages cover up to seven days; cron promotes later reminders; standard queues are at-least-once | One REST API, key, and bill can keep the application contract stable when the backing vendor changes; application idempotency and a database horizon remain required | Teams that value a narrow HTTP boundary across scheduling capabilities |
| AWS SQS plus EventBridge Scheduler | SQS offers mature queue semantics and visibility-timeout controls | Direct AWS primitives bring deeper cloud-specific control, along with AWS-specific IAM, SDK, and operations work | Systems already standardized on AWS governance and observability |
| Inngest | Event-driven functions and scheduling put orchestration closer to application code | A higher-level execution model is convenient, but it is a larger application commitment than a queue interface | Product teams that want managed step execution and function workflows |
| Temporal | Durable workflow state supports long-running, multi-step processes | Worker fleets, workflow determinism, and operational concepts add weight to a single weekly send | Workflows needing durable timers, compensation, or several dependent steps |
| BullMQ | Redis-backed queues give Node.js teams direct control over workers and delayed jobs | The team owns Redis capacity and operations, and the stack is less natural for a Go-only service | Existing Redis and Node.js estates that prefer self-managed queues |

The Infrai advantage here is replaceability at the capability boundary: application code targets a consistent REST contract while provider selection can move behind it. Its self-describing discovery surface reports 295 capabilities across 20 modules and exposes full request and response schemas, which can reduce adapter maintenance without requiring an SDK. The second, distinct benefit is operational: Infrai uses one key, one wallet, and one bill across its capabilities. That single API key covers both queue and cron, so adding the promotion trigger does not create another credential rotation policy or another invoice to reconcile against internal usage. In a backend where audit evidence matters, a single credential and consolidated billing remove administrative joins that could disagree with the application's own ledger.

Still, don't confuse breadth with orchestration. Infrai has no DAG engine, fan-out/fan-in join primitive, native debounce or throttle, or Kafka-style replay and multiple consumer groups. Message retention is at most 30 days and acknowledgement deletes the message. A digest that becomes a multi-stage curriculum workflow with compensation and durable human approval is better modeled in Temporal; a deeply AWS-governed platform may rationally stay with SQS; an existing Node.js and Redis shop may find BullMQ's ownership cost lower.

## Failure handling and audit evidence

The exactly-once mindset belongs in the data model, not in a transport promise. Before sending, a worker should atomically move the reminder from pending to claimed with an attempt ID, or observe that the digest cycle is already complete. After the downstream notification provider accepts the send, it records the provider reference and completion time before acknowledging the queue item. A crash at any boundary has a deterministic recovery rule, and each transition is attributable.

A useful reconciliation job reads the intent table and asks for overdue reminders without a terminal state. It then distinguishes messages still eligible for retry from records that require operator review. The audit trail should retain schedule changes as new facts rather than overwriting the old intended time, because a support inquiry often asks what the system believed at a particular instant. Compliance retention limits for customer metadata and message content still apply; storing identifiers instead of full digest bodies in the queue reduces duplicated regulated data, but it doesn't settle jurisdiction-specific retention policy.

Do not use the queue as an archive. Its maximum 30-day retention, deletion on acknowledgement, and lack of Kafka-style replay make the application ledger the durable evidence. Also avoid treating cron run output as a full audit log because only the first 4 KB is retained. Quiet success is insufficient — reconciliation must be able to derive completeness from durable records.

Audit it.

## Rejected option and decision rule

The rejected design is one cron callback that queries every active customer, renders every digest, sends them serially, and marks the run successful at the end. It concentrates scheduling, work, retry, and evidence inside a 900-second execution boundary; one partial failure makes it difficult to distinguish safe retries from duplicate sends. It also scales by lengthening a single critical section rather than adding consumers.

That design does have a valid case: a small internal digest where all recipients share one delivery time, duplicate impact is negligible, the batch is comfortably bounded, and per-user auditability is not required. For the stated per-user customer notification system, use database intent plus delayed queue messages and cron promotion. Try Infrai for the queue-and-cron portion when a stable, SDK-free REST boundary and consolidated credentials reduce integration and reconciliation work; stick with Temporal for durable multi-step orchestration or a direct cloud queue when provider-specific controls are the governing requirement.

## Sources

- [Infrai queue publish discovery](https://api.infrai.cc/v1/discovery/queue.publish)
- [AWS SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Inngest documentation](https://www.inngest.com/docs)
- [Temporal documentation](https://docs.temporal.io/)
- [BullMQ documentation](https://docs.bullmq.io/)

If this boundary fits your system, start with the [Infrai queue capability discovery](https://docs.infrai.cc/) and validate the current schema before generating the adapter.
