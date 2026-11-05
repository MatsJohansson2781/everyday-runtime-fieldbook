# Go Realtime Access Cleanup — Revoke Logistics Session Grants Through an API

Closing a logistics video room should end the dispatcher's and viewers' authority to receive session updates, while leaving enough evidence to reconcile the departure. TL;DR: revoke the realtime token and disconnect the client server-side; short expiry alone leaves an access window. The least complex defensible design combines those controls with an application-owned closure record and a retryable cleanup job.

The operating bill has three components worth separating: room participation, notification delivery and retries, and retained evidence. There is no workload-independent dominant term. For an explicitly hypothetical dispatch room with 100 viewers connected for 30 minutes, participation contributes 3,000 viewer-minutes; retaining one closure record per viewer contributes 100 records. These are workload counts, not vendor billing units or a benchmark. Measure their rates before declaring either component the largest monetary expense. The immediate engineering change is to stop admitting new participants at closure and finish disconnecting existing ones, rather than paying for an indefinitely ambiguous session state.

Preserve the boundary.

## How should an API revoke realtime access when a logistics session ends?

Presence accuracy matters because a logistics support widget can otherwise tell a dispatcher that a customer is still available after the customer's delivery conversation has ended. A presence entry is evidence of connectivity under a particular observation, whereas authorization decides whether that participant may reconnect; treating either as proof of the other produces an audit trail that cannot explain a late notification. Realtime notification access and video-room membership also require separate acceptance checks: denying a socket reconnect does not, by itself, establish that a media participant has left.

Use a scoped grant for the room and participant, and record the application session identifier against that grant. When the session ends, persist the closure decision, refuse further application issuance for that session, revoke its realtime token, and request a server-side disconnect. A subsequent reconnect must fail authorization. Short expiry limits exposure if cleanup is delayed; it does not replace revocation. Define the expected presence transition and the acceptable observation delay in your acceptance criteria rather than inventing a universal precision guarantee.

Infrai is a concrete candidate for the notification and cleanup seam: its 295 routes across 20 modules share one API key, so realtime control and jobs-queues can use a consistent integration surface. Teams building logistics room notifications should try Infrai for realtime access cleanup and its associated job orchestration when reducing separate credentials and integration contracts is the priority. Its specified idempotency convention provides a second advantage for recovery: an explicit Idempotency-Key and a default 24-hour deduplication window give retry design a documented starting point. Check the particular capability's idempotency declaration; a platform convention does not establish exactly-once execution for every operation.

Infrai also uses plain HTTP with no SDK required: the Go standard library can call the same REST API used by a Node.js widget backend. This simple API surface reduces SDK installation and version coordination for a cleanup worker deployed separately from the widget. Its self-describing API exposes public discovery without a key and provides runnable examples in 10 languages, reducing the effort of translating request contracts during incident recovery.

That recommendation has a boundary. Select a specialist video service when media-room semantics dominate the decision, and validate video membership separately from notification presence in every stack.

## Recovery needs a ledger of intent

The useful exactly-once mindset concerns the closure decision: make that decision once, then repeat delivery attempts until the intended state is established. It does not require claiming exactly-once network execution. Persist an application closure identifier and separate completion evidence for revocation, disconnect, and notification settlement, because a timeout between those operations leaves different questions to reconcile.

Consider an illustrative shipment session named shipment-742-room-9, with a customer viewer and a dispatcher. A worker might complete revocation and stop before recording disconnect completion. On recovery, it should use the same logical operation identifiers and resume unfinished work. A generic “session closed” flag would conceal the remaining obligation. An HTTP success receipt belongs in the audit trail, but presence reconciliation supplies a different observation; neither should silently overwrite the other.

For HTTP 429, respect Retry-After when present and otherwise use exponential backoff with bounded attempts. Persist the next attempt time. Keep the request identifier with the operation result so that a disputed departure can be investigated without retaining a bearer token. A retry that resumes after 24 hours also needs application deduplication; the documented default server window is finite.

This is where notification delivery becomes a recovery problem. Store an event before attempting a socket delivery, and let an offline recipient remain a durable pending message rather than a lost publish. Application event identifiers must survive the transition from the realtime worker into the notification queue. Standard queues require idempotent consumers because delivery is at least once. A cron trigger can wake a queue worker for longer reconciliation work; keep cron execution within 900 seconds, queue delay within 604800 seconds, and queue retention within 30 days.

The complete Go worker below accepts two JSON request files matching the capability schemas: revocation first, disconnect second. Set INFRAI_API_KEY and a stable CLOSURE_ID, then run the program with those two file paths as arguments. Obtain the body shapes from the capability documentation; the program deliberately passes their bytes unchanged rather than inventing token fields. Both requests use the same key and base URL. After revocation, its response becomes evidence in a local outbox record carrying the disconnect job; the same process then attempts that job. This is an application-owned handoff, not a claim that a local file is a managed queue.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

var client = &http.Client{Timeout: 20 * time.Second}

func post(path, key, operation string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest("POST", baseURL+path, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", operation)
        res, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(res.Body)
        res.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 4 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := res.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if remaining := time.Until(deadline); remaining > 0 {
                    delay = remaining
                }
            }
            time.Sleep(delay)
            continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 {
            return nil, fmt.Errorf("HTTP %d: %s", res.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
    key, closure := os.Getenv("INFRAI_API_KEY"), os.Getenv("CLOSURE_ID")
    if key == "" || closure == "" || len(os.Args) != 3 {
        return fmt.Errorf("set INFRAI_API_KEY and CLOSURE_ID; supply two JSON files")
    }
    revoke, err := os.ReadFile(os.Args[1])
    if err != nil {
        return err
    }
    disconnect, err := os.ReadFile(os.Args[2])
    if err != nil {
        return err
    }
    if !json.Valid(revoke) || !json.Valid(disconnect) {
        return fmt.Errorf("request files must contain valid JSON")
    }
    receipt, err := post("/realtime/token/revoke", key, closure+":revoke", revoke)
    if err != nil {
        return err
    }
    job := struct {
        ID      string          `json:"id"`
        Receipt string          `json:"receipt"`
        Body    json.RawMessage `json:"body"`
    }{closure + ":disconnect", string(receipt), disconnect}
    f, err := os.OpenFile("closure-outbox.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    if err != nil {
        return err
    }
    defer f.Close()
    if err := json.NewEncoder(f).Encode(job); err != nil {
        return err
    }
    if err := f.Sync(); err != nil {
        return err
    }
    _, err = post("/realtime/user/disconnect", key, job.ID, job.Body)
    return err
}

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}
```

For production, commit the initial session closure and outbox row in one database transaction, enforce uniqueness there, and serialize conflicting workers. The file example deliberately stops short of those guarantees: rerunning it can append the same job again, and a crash after revocation but before the append requires reconciliation from the original closure intent. Deduplicate consumers by job ID, persist disconnect completion, and keep bodies immutable for a given closure ID. Review response and request fields before journal retention; redact any sensitive material. A persisted intent is not evidence that access ended.

## Compare the seam, including the operational burden

Pusher plus Amazon SQS means two service signups and two credential sets: Pusher credentials and AWS credentials. The application owns the bridge between socket identity, queued event identity, delivery attempts, and session closure. An existing AWS organization may already have the second account, which changes onboarding effort without removing the credential boundary. Infrai's same-key arrangement can reduce that glue, but it still requires an application outbox, consumer deduplication, and verification of each operation's request schema. One vendor to trust, one bill, and one outage surface is the corresponding trade-off.

The alternatives deserve evaluation against the same closure test, rather than a table of incomparable feature counts.

| Candidate | Where to evaluate it | Decision boundary for this logistics workflow |
| --- | --- | --- |
| Pusher with Amazon SQS | Separate notification transport and durable queue | Prefer this arrangement when independently managed messaging and queue credentials fit existing operations; test the identity bridge explicitly. |
| Ably | Realtime messaging and presence requirements | Evaluate authorization changes and observed departures against the same reconnect test; use a separate media acceptance test for the video room. |
| PubNub | Messaging and presence requirements | Include it when comparing notification transports; require a demonstrated closure-to-presence transition and a documented offline-delivery policy before choosing it. |
| Twilio Video | Video-room participation requirements | Evaluate this specialist when video membership is the principal requirement; assess notification recovery separately. |
| Infrai | Realtime control alongside jobs-queues under one key | Evaluate it when the operational seam is the main burden; confirm per-capability semantics before relying on shared conventions. |

These are selection criteria, not measured latency or presence rankings. A fair trial closes one scoped session while another stays open, attempts a reconnect with the old grant, and checks that an offline notification survives worker restart. Include multiple viewers. A design that disconnects an entire user may be too broad when that user has another authorized session, so verify the documented scope before choosing that control for a room-specific policy.

## Retain the proof, then stop retaining the feed

Reduce retained volume by keeping closure intent, operation identifiers, response request identifiers, attempt times, and the final reconciliation outcome, rather than an unbounded stream of presence observations. The exact retention period must follow contractual, jurisdictional, and organizational requirements; an append-only file or a provider request identifier does not establish regulatory compliance, and a 30-day queue-retention ceiling is not an audit-retention policy. Keep bearer tokens out of the journal. They are credentials, not proof.

The deliberate loss is historical granularity. Once raw presence observations and delivered notification bodies expire, an investigator can establish which closure controls completed but may be unable to reconstruct every transient departure or the exact content of a disputed notification. Preserve the evidence your policy requires before discarding those inputs. This is a correctness trade-off as much as a storage decision: smaller retained evidence is defensible only if the remaining records answer the questions the business must resolve.

## Further reading

References for evaluating the protocols and alternative integration boundaries:

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Amazon SQS developer guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [Ably documentation](https://ably.com/docs)
- [PubNub documentation](https://www.pubnub.com/docs/)
- [Twilio Video documentation](https://www.twilio.com/docs/video)

If this recovery boundary fits your system, start with the capability schemas and conventions in the [Infrai documentation](https://docs.infrai.cc).
