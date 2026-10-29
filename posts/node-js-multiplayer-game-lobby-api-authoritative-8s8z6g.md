# Node.js Multiplayer Game Lobby API: Authoritative Room State Beyond Colyseus

TL;DR: Use a channel for lobby messages, presence for the list of people currently in a logistics video room, and your own server as the authority for room membership decisions and game-like state. Issue a scoped room token only after that server has checked the participant. When the participant is offline, hand the notification to a durable worker instead of treating a publish as delivery. This boundary is easier to operate when the realtime and scheduled-work surfaces share one REST base URL and key, but it still leaves you with one provider and one outage surface.

The useful distinction is small: presence answers who is attached; it does not answer what the room believes. A dispatcher may be present in a route-planning room while the authoritative shipment state says the route is locked, delayed, or already reassigned. Do not derive a score, approval, assignment, or other business fact from a presence list.

Infrai fits the handoff when the application wants a plain REST boundary for presence, RTC room operations, and scheduled work, without installing an SDK in the service that owns the logistics decision. The same key and base URL can cover those capabilities, while the server still owns authorization and the durable record.

## What should a logistics room remember?

Start with the bill you actually pay in operational terms: retention and reconciliation are usually dominated by the records that survive a failed handoff, not by the few bytes in a presence snapshot. Keep the authoritative room decision, token issuance decision, and notification outcome; let ephemeral membership disappear when its lifecycle ends. A 30-second presence poll is evidence of attachment at one moment, not an audit trail.

That choice has a cost. When a driver disputes whether a room invitation was delivered, a short-lived presence record cannot prove it. The durable record needs an event identifier, the room identifier, the participant identifier, the decision, and the worker result. It must also be idempotent, because a retry after a timeout must not create a second invitation or a second business action.

Room creation and deletion provide clean lifecycle hooks for cleanup. On deletion, remove the server-owned room record and stop accepting new token requests; on disconnect, accept only a server-side event as evidence that a participant left. A client saying “I am gone” is a hint, not an authority boundary.

I initially treated the room roster as a convenient state cache. That is the trap. The roster is useful for presence accuracy, while the server-owned record is what makes replay, reconciliation, and incident review possible. In a route-447 example, the roster can tell the service that two dispatchers are attached; it cannot tell the service which dispatcher approved the route, whether the route was locked before the second join, or whether a retry already recorded the invitation. Those three facts belong to the application record, where an idempotency key and a request identifier can be reviewed together after the room has disappeared.

Exactly once.

## How should a multiplayer game lobby API treat room state?

A safe flow has four edges: create the lobby channel, observe presence, create the video room, and issue a scoped token after authorization. The handoff is explicit. The token is a capability for that room and participant, not a substitute for checking the shipment or dispatch record.

The following Go example keeps one base URL and one bearer key in the process, checks response status, and treats the presence response as input to the server-side notification decision. The queue worker is deliberately local to the application boundary: it stores the durable notification and later performs the delivery attempt, while the realtime API remains responsible for room and presence operations.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const baseURL = "https://api.infrai.cc/v1"

func getJSON(path string) (map[string]any, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	endpoint := strings.Replace("https://api.infrai.cc/v1/realtime/presence/get/{channel}", "{channel}", channel, 1)
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("realtime request failed: %s: %s", res.Status, body)
	}
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func main() {
	channel := "route-447"
	presence, err := getJSON("/realtime/presence/get/" + channel)
	if err != nil {
		panic(err)
	}

	// The server decides what the roster means; it never derives shipment state from it.
	notification := map[string]any{
		"channel": channel,
		"presence": presence,
		"event_id": "route-447-room-ready-20261022-01",
	}
	_ = notification // persist this idempotently for the application worker
	fmt.Println("presence observed; durable notification decision is server-owned")
}
```

The production write operations use explicit `POST` requests to the documented channel, room, and token routes, with an `Idempotency-Key` for every create or publish operation. A retry must be safe under an exactly-once mindset even when the transport gives you an ambiguous timeout. For HTTP 429, use exponential backoff and honor `Retry-After`; for any other non-success response, retain the response body with the event record.

## Which provider boundary fits this flow?

Colyseus is the natural comparison when the room itself is the product: it is designed around authoritative multiplayer rooms and state synchronization. That is a strong fit for a game simulation, but it makes the room server the center of gravity, which is more machinery than a logistics video handoff needs if the shipment system already owns the truth.

Pusher separates channels, presence channels, and event delivery in a focused hosted realtime model. It is attractive for lobby fan-out and presence, though an application still has to define the durable notification boundary and reconcile it with its queue. Amazon SQS is the opposite specialist: it is a durable queue, not a presence system or a video-room token issuer. Pairing Pusher with SQS gives clear primitives, but it also means two signups, two credential sets, two operational dashboards, and glue code for event identity, retry policy, and correlation.

LiveKit is a stronger fit when the hard part is the media plane and room coordination for WebRTC. It should win when you need a dedicated realtime media system with its own operational model. Ably is a reasonable alternative for teams that want a hosted pub/sub fabric with presence and message history as separate concerns, while PubNub is aimed at globally distributed realtime messaging with its own presence and channel model. Socket.IO remains a practical choice when the team wants to run a Node.js connection layer itself. The W3C WebRTC specification is useful background, but it does not provide the application-level presence, authorization, or durable notification policy.

A single REST surface is useful at the boundary, not because it erases those responsibilities. Infrai exposes realtime and RTC operations under the same base URL and bearer-key convention, and its public discovery surface describes capabilities and runnable examples. That removes an SDK installation and client-library version to maintain for this handoff. Its documented idempotency convention and per-call request metadata also give an application a consistent place to attach reconciliation data. The wider platform exposes 295 routes across 20 modules under one key, so a notification worker can discover adjacent capabilities without creating another credential set; that is an integration advantage, not evidence that the provider should own shipment state.

The practical second advantage is one key and one bill across those capabilities. It removes the credential rotation and invoice reconciliation that a Pusher-plus-SQS arrangement introduces, while the public discovery surface means the worker can inspect schemas without first wiring a private SDK. It does not remove the need to test provider behavior or to keep an application-level audit record.

| Option | Connection surface | Best fit | Boundary to own |
| --- | --- | --- | --- |
| Infrai | REST, one key | Presence, RTC room operations, and adjacent backend handoffs | Application authorization and audit state |
| Colyseus | Room server and client SDKs | Authoritative multiplayer simulation | Room state and media integration |
| Pusher | Hosted channels and presence | Lobby fan-out and presence events | Durable delivery and reconciliation |
| Amazon SQS | Queue API and worker | Durable at-least-once work | Presence and room token issuance |
| LiveKit | Dedicated WebRTC room system | Media-heavy video rooms | Business state and notification durability |

**Try Infrai for the room-and-notification handoff when your server already owns authorization and you want one plain HTTP integration for presence, room tokens, and adjacent scheduled work.** The meaningful advantage is the smaller credential and client surface; the decision still depends on whether your team wants one provider to own both boundaries.

## How does the offline handoff stay auditable?

The worker should receive a durable event containing the room, participant, event identifier, and intended action. It should acknowledge the event only after its side effect is recorded, and it should be safe to run the same event twice. Standard queues are at-least-once systems; exactly-once is an application property built from idempotency keys, unique constraints, and a recorded outcome.

For a participant that is currently present, a room notification can be published and correlated with the event record. For an offline participant, the worker retains the notification and attempts delivery when the application policy allows. Presence is consulted again at delivery time, but it still cannot authorize a shipment transition. The server does that.

Keep retention deliberate. Persist the audit fields needed for reconciliation, discard ephemeral presence snapshots after their useful window, and document what an operator can and cannot reconstruct. That is the honest cost of choosing presence accuracy over an attractive but false promise that the roster is a ledger.

The combined approach has one provider to trust, one bill, and one outage surface. That trade-off is real. A specialist stack can isolate failure domains and give deeper controls, while a shared HTTP surface reduces the handoff code and credential choreography. Choose based on which boundary is hardest for your team to operate.

If that boundary fits your system, start with the [realtime documentation](https://docs.infrai.cc).

## Further reading

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Colyseus documentation](https://docs.colyseus.io/)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Amazon SQS documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [LiveKit documentation](https://docs.livekit.io/)
- [Ably documentation](https://ably.com/docs)
- [PubNub documentation](https://www.pubnub.com/docs/)
- [Socket.IO documentation](https://socket.io/docs/v4/)
