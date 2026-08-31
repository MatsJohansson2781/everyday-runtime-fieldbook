# Why Daily Report and Renewal Email Jobs Need Retry Queues and Dead-Letter Review

Short answer: schedule creation of durable email jobs, let workers retry only failures that may clear, and move exhausted or non-retryable jobs to a dead-letter review path; neither the scheduler nor the queue should be treated as the audit record.

For an e-commerce system that sends a daily report and must delay a renewal reminder until a business deadline, the architectural decision is the same: time determines when work becomes eligible, while application state determines whether the business action is still authorized. A scheduled trigger can be late, and a worker can see the same task more than once. The design therefore needs an idempotency key, an attempt history, and a reconciliation rule that survives both conditions.

One rule carries most of the weight: `(message_kind, business_date, account_id)` identifies one authorized email. The queue transports that authorization; it doesn't prove delivery.

## Decision record and invariants

Adopt a scheduler-to-queue-to-worker path with bounded retries and a dead-letter review state. The scheduler creates a run record, selects the accounts whose daily report or renewal deadline is due, and enqueues one job per account. A worker claims one job, consults the durable email ledger, performs the send if the key has not already reached `delivered`, records the attempt outcome, and acknowledges queue work only after the state transition is durable.

This is an at-least-once mindset with an exactly-once business invariant. Transport-level duplicate delivery remains possible, but a uniqueness constraint on the business key prevents two successful state transitions for the same authorized message. Keep payloads referential: account ID, message kind, business date, and template version are enough. Recipient addresses and rendered reports belong in access-controlled application storage, where retention and correction policy can be enforced independently of queue retention.

The failure boundaries must be named before implementation. A scheduler failure means no run record or incomplete job creation; reconciliation compares the intended account population with jobs created for that run. A dispatch failure means a job exists but no terminal business outcome exists; the retry policy or review path owns it. A provider acceptance followed by a worker interruption is the uncomfortable boundary — queue redelivery alone cannot tell whether another email is safe, so the delivery ledger and provider correlation data must settle that question. A recipient or policy change after enqueue is another boundary: the worker must re-check authorization at execution time, especially for a renewal reminder held until a business deadline.

Don't hide those cases in logs. Logs help diagnosis, but an audit trail needs queryable states, timestamps, stable identifiers, and the reason for each transition.

## Why do failed daily report email jobs need retries and a dead letter queue?

Retries answer a narrow question: could another attempt succeed without changing the business request? Temporary dispatch congestion or a worker interruption may justify another attempt. Invalid account state, withdrawn consent, or a renewal that has already completed does not. Repeating every failure wastes capacity and can perform a now-unauthorized side effect; never retrying turns a brief interruption into a missed report.

A dead-letter queue, or an equivalent durable review state, is needed because bounded automation must end somewhere visible. It separates work that workers may attempt automatically from work that needs a decision. The review outcome is not always “send again.” An operator may correct data and authorize redrive, cancel the message because its deadline passed, or mark the job resolved because the ledger already proves delivery. Each action should append an audit event rather than erase the failed attempt.

The retry budget cannot be universal. It depends on the useful lifetime of the email, the deadline, downstream behavior, and the team's response time. I'm not sure an organization can choose a defensible number without those inputs; the evidence needed is its delivery contract and observed failure distribution. What is certain is the ordering: exponential or otherwise spaced retries stay inside the usefulness window, the terminal transition is explicit, and the next daily run never silently resets yesterday's failed job.

For a renewal reminder eligible at 09:00 on the account's business date, imagine the worker loses its lease after the external send is accepted but before local completion is recorded. The job returns. A naive worker sends twice. A safer worker reopens the same idempotency record, uses its stable correlation identifier to reconcile the ambiguous attempt, and either records delivery or leaves the item for review. This longer path is deliberate because ambiguity is a state, not permission to repeat a financial or customer-facing action. The same reasoning applies to a daily report: “the queue delivered twice” is tolerable; “the customer received two contradictory reports” is not.

That distinction is the design.

## Comparing the operational recovery options

The primary decision axis is recovery, not how quickly a team can write a timer expression. Scheduled automation documentation for GitHub Actions warns that scheduled runs can be delayed during periods of high load and that some queued jobs may be dropped; scheduled workflows run from the latest commit on the default branch. Those properties make a scheduled workflow a plausible control-plane trigger, but they do not make it a recipient-level delivery ledger. Google Cloud Tasks describes a managed service for executing distributed work through tasks, which illustrates the separate queue boundary. In either case, business idempotency and reconciliation remain application responsibilities.

| Option | Recovery evidence | Appropriate use | Limitation that changes the decision |
| --- | --- | --- | --- |
| One scheduled batch | Run history and application logs | Small, low-consequence mailings that can be rerun as a whole | Partial completion is ambiguous unless the application adds recipient-level records. |
| Scheduled trigger plus durable queue | Per-job queue state plus an application ledger | Independent daily reports or renewal reminders with bounded retry and review | The team must operate reconciliation, dead-letter ownership, and idempotency state. |
| Durable workflow engine | Workflow history and modeled steps | Multi-stage renewal processes with waits, approvals, compensation, or coordinated branches | The broader execution model adds operational and conceptual weight to a one-step email. |

The catch is straightforward. A queue-based design is not suitable when the business process is a long, branching workflow whose recovery depends on several coordinated actions; use a durable workflow model in that case. Stick with one scheduled process when the recipient set is genuinely tiny, duplicate or missed mail has negligible consequence, and no recipient-level audit is required. For an auditable daily report or a deadline-sensitive renewal message, however, explicit job state usually earns its operational cost because it turns partial completion into a set of named decisions.

## The critical path in Go

The following code focuses on the state decision, not a vendor API. The queue adapter supplies a delivery attempt, the mail adapter uses a stable operation key, and the repository commits the audit event and current state in one transaction. Production code also needs leases, authentication, authorization checks, redaction, and retention controls; those concerns are intentionally interfaces here rather than invented endpoint calls.

```go
package emailjob

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type State string

const (
	Pending   State = "pending"
	Delivered State = "delivered"
	Retryable State = "retryable"
	Review    State = "review"
	Cancelled State = "cancelled"
)

type Job struct {
	Kind         string
	BusinessDate string
	AccountID    string
	Attempt      int
	MaxAttempts  int
	Deadline     time.Time
	State        State
}

func (j Job) Key() string {
	return fmt.Sprintf("%s:%s:%s", j.Kind, j.BusinessDate, j.AccountID)
}

type SendResult struct {
	Accepted bool
	Err      error
}

var ErrNonRetryable = errors.New("non-retryable send")

func nextState(now time.Time, job Job, result SendResult) State {
	if job.State == Delivered || job.State == Cancelled {
		return job.State
	}
	if now.After(job.Deadline) {
		return Cancelled
	}
	if result.Accepted {
		return Delivered
	}
	if errors.Is(result.Err, ErrNonRetryable) || job.Attempt >= job.MaxAttempts {
		return Review
	}
	return Retryable
}

type Repository interface {
	LoadForUpdate(context.Context, string) (Job, error)
	AppendTransition(context.Context, Job, State, time.Time) error
}

func RecordAttempt(ctx context.Context, repo Repository, key string, at time.Time, result SendResult) (State, error) {
	job, err := repo.LoadForUpdate(ctx, key)
	if err != nil {
		return "", err
	}

	next := nextState(at, job, result)
	if err := repo.AppendTransition(ctx, job, next, at); err != nil {
		return "", err
	}
	return next, nil
}
```

The transaction behind `AppendTransition` should enforce legal transitions and uniqueness for the business key. A queue acknowledgment follows that commit. If the commit fails, the job remains eligible for delivery; if the job is delivered again after a completed commit, `LoadForUpdate` observes the terminal state. This does not magically guarantee one network call to an external mail system. It gives the application a durable place to reconcile the uncertain interval between that call and its own commit.

Operational dashboards should reconcile counts by run: intended, enqueued, pending, delivered, cancelled, retryable, and under review. Alert on age and deadline proximity, not merely queue depth, because ten old renewal reminders can be more urgent than ten thousand fresh reports. Access to payloads and audit events should follow the organization's privacy and compliance policy; neither source cited here defines a universal retention period, so don't invent one.

## Rejected option, deployment checks, and its valid use

Reject an inline cron loop as the default for this system. When the process stops after some sends, a rerun needs recipient-level evidence to distinguish completed work from untouched work; once that evidence exists, pushing the remaining units through a durable queue creates a clearer recovery boundary. The inline design also couples scheduling latency, fan-out duration, and provider behavior into one execution window.

It still has a valid use. A small internal digest with no material harm from omission or duplication, no business deadline, and no audit requirement may be simpler as one scheduled process. That is a real trade-off, not a queue failure.

Before deployment, test a late trigger, duplicate delivery, worker termination before and after the ledger commit, deadline expiry, an authorization change while a reminder waits, retry exhaustion, manual cancellation, and approved redrive. Then run reconciliation against a known account set and verify that every intended business key reaches exactly one explainable terminal state. Release the scheduler and workers independently, preserve backward compatibility for queued payload versions, and assign a human owner to review-state age. A dead-letter path without ownership is storage, not recovery.

## Further reading

- https://cloud.google.com/tasks/docs/dual-overview
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
