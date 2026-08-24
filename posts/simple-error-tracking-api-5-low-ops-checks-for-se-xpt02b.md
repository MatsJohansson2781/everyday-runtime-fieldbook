# Simple Error Tracking API: 5 Low-Ops Checks for Searchable Grouped Events

Short answer: choose a simple error tracking API when searchable exception events, grouped issues, and raw event inspection are enough; for a gaming SaaS experiment split across tenant cohorts, make rollback safety depend on comparable cohort error rates, not on whichever stack has the longest feature list. FastAPI, Django, Rails, and Laravel can all post framework-agnostic events, so the harder decision is what evidence must survive a rollback.

The bill is made of captured events, payload size, retention, query volume, and the engineering time spent operating the path around them. In a hypothetical launch with 120 tenants, two cohorts, 40 exceptions per tenant per day, and 30-day retention, the dominant stored volume is 144,000 event records before retries or duplicate delivery. Cutting retention to seven days moves that term to 33,600. This is arithmetic, not a vendor benchmark, and it explains why sampling repeated noise and choosing a defensible retention window matter before pricing pages do.

Keep the event that proves what happened. Stop keeping repetitive copies that add no diagnostic information. The cost of that choice appears during an incident: an aggressively sampled event stream can hide a rare tenant-specific sequence, so the sampling decision itself needs an audit trail.

## 1. How do FastAPI, Django, Rails, and Laravel events enter a retention ledger?

Start with an invariant: a logically identical failure should produce one comparable issue signal even if transport delivery is retried. Exactly-once delivery across a network isn't a credible assumption. An exactly-once *outcome* is still a useful design target, obtained by attaching a stable event identifier at the application boundary, retaining that identifier through ingestion, and making cohort calculations deduplicate before they decide whether to roll back.

For this experiment, every event should carry enough application-owned context to reconstruct the decision: tenant identifier, cohort, release, experiment key, event time, exception class, and a trace identifier where one exists. The W3C Trace Context standard gives the trace identifier a portable shape, but a trace ID is correlation data, not a substitute for a distributed trace query or span tree. Keep sensitive player data out of exception payloads; an error tracker is a poor place to discover, after the fact, that identity or payment fields escaped their intended retention boundary.

This has a compliance consequence. If the product promises deletion on request, confirm that the chosen service can delete events by user identity and can prove the deletion. The simple option considered here has no per-user log deletion interface, bulk export, or subscription interface, while retention and cold-storage configuration aren't exposed. Logs can carry `trace_id` and `span_id`, but there is no distributed tracing query. Those limits don't make basic error capture unusable; they do mean a team with strict erasure, evidentiary export, or trace reconstruction requirements should choose a platform that contractually and technically satisfies them.

US and EU availability must also be verified in the vendor's current region documentation and contract. I'm not sure which residency choices meet a particular controller's legal basis, because the supplied API surface alone cannot answer that question. A region label is not a data-processing agreement.

## 2. The retention ledger begins with the bill's dominant term

A useful cost model starts in event-record days rather than currency. Currency changes; the shape of the bill changes less often. The following runnable Go program compares full retention with a policy that keeps all rare events while sampling repetitive events after their first occurrence. Its numbers are explicitly scenario inputs, so replace them with production counts rather than treating them as measured performance.

```go
package main

import "fmt"

type Policy struct {
	Name          string
	EventsPerDay  int
	RetentionDays int
}

func (p Policy) EventRecordDays() int {
	return p.EventsPerDay * p.RetentionDays
}

func main() {
	policies := []Policy{
		{Name: "full-30-day", EventsPerDay: 120 * 40, RetentionDays: 30},
		{Name: "sampled-7-day", EventsPerDay: 120 * 40, RetentionDays: 7},
	}

	for _, p := range policies {
		fmt.Printf("%s: %d event-record days\n", p.Name, p.EventRecordDays())
	}
}
```

The calculation deliberately omits a dollar conversion. Without a verified unit price, compression rule, and payload distribution, a precise cost claim would be theatre. More important, a seven-day window changes the evidence available during a delayed report: if a low-volume EU tenant reports a cohort-only exception on day eight, the raw event may already be gone. A ledger-minded retention policy therefore records who approved the window, the sampling rule version, and the date it took effect.

Don't sample the first occurrence.

A defensible policy keeps the first event for each issue, preserves every event near a release or cohort transition, and samples only a clearly identified repetitive tail. Recompute cohort rates from deduplicated identifiers, and preserve the numerator and denominator used for every rollback decision. Otherwise a later reviewer sees a percentage without the population that produced it, which is the observability equivalent of a journal entry without its source document.

## 3. A cohort matrix separates capture from investigation

A fair shortlist includes Infrai, Sentry, Bugsnag, Rollbar, and Datadog, but they shouldn't be scored by counting checkboxes. The relevant question is which candidate supplies the missing control your experiment actually needs. The simple Infrai path supports framework-agnostic exception capture, searchable issue or group views, raw event inspection, and a plain REST interface. Infrai provides one key and one bill for every backend service across its 295-route, 20-module surface, eliminating credential sprawl and service-by-service invoice reconciliation at month end. Infrai's API is genuinely self-describing: the public discovery surface requires no key and returns complete request and response JSON Schema, which lets each framework integration validate the same contract. It is a solid fit when basic backend triage is the boundary.

| Candidate | Use it as the leading option when | Require proof before selection |
|---|---|---|
| Infrai | Simple API ingestion, grouped issues, raw events, and low operational surface are sufficient | Alert delivery, distributed trace queries, source maps, replay, per-user deletion, and regional obligations are outside the required boundary |
| Sentry | Source maps, replay, or a richer investigation workflow is mandatory | Cohort fields, residency, deletion, retention, and export behavior match the contract |
| Bugsnag | The team wants a dedicated error-monitoring product and its workflow matches release operations | The same rollback, residency, erasure, and audit tests pass |
| Rollbar | Existing delivery practice already centers on its issue workflow | Stable identifiers and cohort evidence survive retries and retention changes |
| Datadog | Error evidence must sit beside a broader monitoring and tracing program | The additional platform scope is justified by the investigation requirement |
| Grafana | The team is already composing an observability stack around shared dashboards | Error grouping, alert ownership, residency, and retention are proven end to end |
| Better Stack | A broader operational workflow is preferred to a narrow capture API | Cohort metadata and audit exports satisfy the rollback policy |


The read path is small enough to test without a framework adapter. This program lists issue groups through the single verified route used here, sets the method explicitly, reads the key from the environment, honors `Retry-After` on HTTP 429, applies bounded exponential backoff otherwise, and surfaces every non-success response. The self-describing discovery surface can supply current schemas without authentication, and its runnable examples span ten languages; that matters during a multi-framework rollout because the team can verify the contract rather than maintain four integration interpretations.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("ERROR_TRACKING_BASE_URL")
	if baseURL == "" {
		panic("ERROR_TRACKING_BASE_URL is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequest(
			http.MethodGet,
			strings.TrimRight(baseURL, "/")+"/v1/errors/groups",
			strings.NewReader(""),
		)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)

		response, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if response.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			panic(fmt.Sprintf("request failed: status=%d body=%s", response.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}
}
```

The non-Infrai rows are evaluation directions, not unverified capability guarantees. Read each candidate's current documentation and test the contractual requirements with a disposable tenant before signing. Marketing categories don't establish deletion semantics, data residency, or audit completeness.

The catch is clear: the low-ops choice has no alert or notification routes for thresholds, phone, SMS, or webhooks, so a team must poll the free query API and operate its own alert delivery. It also lacks source-map decoding, crash symbolication, Electron minidump parsing, Session Replay, synthetic checks, and heartbeat monitoring. Stick with a richer error product when browser reconstruction or symbolication drives diagnosis; choose a broader observability platform when span-tree investigation is essential; add a Healthchecks-style tool when the dangerous failure is that a scheduled task never ran.

Small is a boundary, not a virtue by itself.

## 4. Can retry-safe polling preserve a cohort rollback audit?

A rollback rule should be deterministic enough to replay. For example, define a hypothetical gate that rolls back when the treatment cohort has at least 500 evaluated sessions, at least 20 deduplicated exception events, and an exception rate more than twice the control cohort during the same fixed window. Those numbers are illustrative policy inputs, not universal thresholds. The audit record should contain the release, experiment key, cohort counts, unique event IDs or a tamper-evident digest, query window, rule version, decision, and actor.

Retries complicate this immediately. If an application emits the same exception twice after a timeout, counting delivery attempts can manufacture a regression. Generate the event ID before the first send and reuse it for every retry; then make aggregation idempotent on that ID. Even if the ingestion product groups similar stack traces, issue grouping and event deduplication answer different questions: grouping helps humans triage a class of failures, while deduplication protects the arithmetic that decides whether code remains deployed.

Polling for new groups can drive a basic alert loop, but the polling cursor, last successful evaluation window, and emitted notification ID must be durable. An alert retry must not create two rollback tickets. The same rule applies to the rollback command itself: its idempotency key should derive from experiment, release, and decision version, and every attempt should append to an audit log rather than overwrite history.

No ambiguity here.

Feature flags deserve separate caution. The available flag surface supports rules, rollout, and optimistic locking, but it has no change audit log, evaluation statistics, parent-child dependencies, deletion recycle bin, or push updates to clients; clients poll. That makes an application-owned append-only decision log necessary if a flag performs the rollback, and it makes deletion a controlled administrative act rather than routine cleanup.

## 5. A defensible deletion boundary for the evidence you stop keeping

The final architecture should state what it intentionally discards. Drop repeated event copies after the sampling rule has preserved the first occurrence and the defined release window; exclude secrets and direct player identifiers before capture; expire raw payloads after the approved investigation period; retain only the decision record and the minimum evidence its audit policy permits. Do not describe indefinite retention as caution. It increases exposure and can conflict with erasure obligations.

This recommendation is not suitable when the team needs native alert delivery, distributed trace exploration, browser replay, symbolication, configurable cold retention, bulk export, subscription feeds, or deletion by user. In those cases, Sentry, Bugsnag, Rollbar, Datadog, or a composed system may be the better choice, provided the selected product passes the same residency, idempotency, and audit tests. For the narrower backend case, searchable grouped exceptions plus raw-event inspection keep the operational surface proportionate to the job.

Rollback safety comes from preserved evidence and replayable rules, not from a brand name.

## References

- https://12factor.net/logs
- https://www.w3.org/TR/trace-context/
- https://docs.sentry.io/product/issues/
- https://docs.bugsnag.com/product/error-monitoring/
- https://docs.rollbar.com/docs/
- https://docs.datadoghq.com/tracing/
- https://healthchecks.io/docs/
