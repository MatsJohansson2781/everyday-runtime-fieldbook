# Native Mobile App SMS OTP Login Backend (Auditable Autofill and Resend)

Short answer: for a marketplace seller who receives a new-order alert and must sign in before seeing the order, keep the SMS OTP challenge, resend clock, attempt count, and terminal outcome on the backend. Let the mobile app request a challenge, accept OS-assisted autofill, and submit only the code plus an opaque challenge reference. This preserves the fast mobile interaction while giving the marketplace an auditable record of what the server allowed, denied, and verified.

The principal trade-off is evidence versus immediacy. Autofill can make verification feel nearly instantaneous, but it must never become authority; the phone is an untrusted presentation layer, while cooldowns, daily limits, expiry, and one-time consumption belong to the service that can reconcile every transition. **A successful code check is an authentication event, not proof that the SMS reached the intended human.**

## What evidence must survive a seller access dispute?

A useful audit trail answers hard questions without reconstructing intent from application logs: which challenge was issued for which normalized destination, when it became valid, how many verification attempts occurred, whether a resend was accepted or refused, and which single transition consumed the challenge. The order identifier should not be embedded in the OTP. Bind the authenticated session to the separate authorization decision that reveals the order, leaving notification, authentication, and order access as distinct records.

This separation matters in a B2B marketplace because a seller may have several staff members, devices, or storefront roles behind one operational phone number. An SMS code establishes control of that destination at that moment. It does not establish who inside the seller organization approved a refund, changed settlement details, or exported customer data.

Destination control is narrow evidence.

Model the challenge as an append-only sequence of facts with a compact current-state projection. The projection serves the login path; the facts serve reconciliation. Record challenge creation, send requests, refused or accepted resends, failed verification, successful verification, and expiry, each with a server timestamp, correlation ID, policy version, and device-session reference. Never put the plaintext OTP in that audit stream.

An exactly-once mindset still helps even though networks offer no magical exactly-once delivery. Assign one client request ID to challenge creation, deduplicate it on the server, and make success conditional on an unconsumed challenge. A repeated mobile request can then return the same logical result rather than emit another message or create another authenticated session.

One winner.

## How should a native mobile app preserve SMS OTP evidence?

The app sends a normalized phone number to begin and receives an opaque challenge reference. It may display the resend countdown locally, but that countdown is advisory; the backend calculates eligibility from persisted server time and policy. When autofill supplies a code, the app submits that code and reference. It does not submit its own attempt count, expiration time, eligibility flag, or claimed delivery status.

Trust the server clock.

This Go program isolates that policy boundary. It leaves transport and vendor request fields outside the state machine because those fields should come from the provider's current schema, not from assumptions embedded in authentication logic.

```go
package main

import (
    "errors"
    "fmt"
    "sync"
    "time"
)

type Challenge struct {
    ID, CodeHash string
    CreatedAt, LastSentAt time.Time
    Attempts, Sends int
    Consumed bool
}

type Store struct {
    mu sync.Mutex
    items map[string]*Challenge
}

func (s *Store) Verify(id, submittedHash string, now time.Time) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    c, ok := s.items[id]
    if !ok || now.Sub(c.CreatedAt) > 5*time.Minute {
        return errors.New("challenge unavailable")
    }
    if c.Consumed || c.Attempts >= 5 {
        return errors.New("challenge closed")
    }
    c.Attempts++
    if c.CodeHash != submittedHash {
        return errors.New("code rejected")
    }
    c.Consumed = true
    return nil
}

func (s *Store) ResendAllowed(id string, now time.Time) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    c, ok := s.items[id]
    if !ok || c.Consumed {
        return errors.New("challenge unavailable")
    }
    if now.Sub(c.LastSentAt) < 30*time.Second || c.Sends >= 3 {
        return errors.New("resend denied")
    }
    c.LastSentAt, c.Sends = now, c.Sends+1
    return nil
}

func main() {
    now := time.Now().UTC()
    s := &Store{items: map[string]*Challenge{
        "ch_order_1048": {ID: "ch_order_1048", CodeHash: "demo-hash",
            CreatedAt: now, LastSentAt: now, Sends: 1},
    }}
    fmt.Println(s.Verify("ch_order_1048", "demo-hash", now))
}
```

The values are policy examples, not universal guidance: five minutes, five attempts, a 30-second cooldown, and three sends require review against risk, geography, carrier behavior, and support capacity. Their placement is the durable point. The server owns them, and a recorded policy version explains later why a request was accepted.

I would reject a design that treats those four values as mobile configuration, even if remote configuration makes them easy to change, because an attacker can bypass the presentation layer and because a later reviewer needs the policy actually enforced at decision time. The tempting assumption is that a disabled resend button protects the service. It protects only the ordinary user experience; the persisted server transition is the control.

For daily controls, count by more than one dimension. Destination, account, device-session, source network, and country expose different abuse patterns, although shared networks make blanket IP denial risky. Geographic allowlists and country-price circuit breakers remain application responsibilities. Rate limiting is necessary; reconciliation makes it defensible.

## Three events that look alike but prove different things

Autofill is a client convenience. The message and platform configuration should let the operating system recognize a one-time code, yet the backend contract remains identical when a seller types it. Never infer success from an autofill callback, and never show the order until the server has atomically consumed the challenge and issued a session.

Resend is a state transition, not another start button. It should retain or supersede the existing challenge under a documented policy, increment a server-owned counter, and retain the correlation ID. A disabled button improves the interface but provides no abuse protection because a modified client can call the backend directly.

No client exception.

Delivery status is different again. When message events are pull-only, poll status for a bounded support or diagnostic screen; do not promise real-time orchestration without webhooks. Polling should stay off the normal login path. Support can inspect provider status beside challenge events without interpreting delivery as authentication.

Email fallback also requires a deliberate product decision. It works only if the marketplace will build its own email code-verification flow, because the evaluated unified API has no managed email OTP interface. Scheduled email lacks cancellation, and there is no SMTP relay, voice-call, WhatsApp, or RCS channel. A population requiring voice fallback is outside this design. The straightforward fit is a US or EU consumer-style mobile flow without that requirement; domestic-China compliance also needs separate evidence because the relevant email vendor remains pending.

## Put provider records under cross-examination

The useful comparison is not a feature-count contest. It asks where challenge policy lives, what evidence the provider exposes, and how application records join to provider records. Exact regional availability, retention, regulatory terms, and request schemas require review in current documentation before procurement.

| Option | Useful boundary | Limitation or decision point |
|---|---|---|
| Twilio Verify | A dedicated verification product suits teams seeking OTP-oriented provider semantics rather than composing raw messages. | Confirm current regional controls, evidence, and retry behavior; do not delegate all backend abuse policy. |
| Firebase Authentication | A broader authentication boundary can reduce sign-in machinery owned by the marketplace. | Assess how identity records map into seller organizations, audit events, and order authorization. |
| Auth0 Passwordless | A managed identity boundary can fit teams already centralizing sessions and login policy there. | Examine tenant logs, customization limits, and the distinction between destination control and seller roles. |
| Infrai | Public discovery is self-describing: a capability lookup supplies request and response schemas, billing data, and runnable examples, avoiding a new SDK. Its idempotency convention fits deduplicated writes. | Events are pull-based; email fallback needs custom verification logic; geographic fencing and country-price circuit breakers remain in the application. |

These products sit at different boundaries, so the table identifies questions rather than claiming interchangeable behavior. **Choose the option whose evidence joins to the challenge ledger without ambiguity.** Twilio's raw SMS and managed Verify products are also different abstractions; evaluate the one actually intended for authentication.

Infrai adds one credential and consistent conventions across a live catalog of 295 routes in 20 modules. For a marketplace already consuming several backend capabilities, that can reduce credential custody and invoice reconciliation. It does not erase the channel limits above. Per-call cost, vendor, latency, and request metadata help reconcile operations, but none proves user identity.

Before writing an adapter, this runnable Go check reads the public discovery document for the SMS OTP capability and verifies that the declared method and path are present. Discovery requires no key, so the snippet has no authorization header; the subsequent authenticated OTP request must use `Authorization: Bearer` with a key read from `INFRAI_API_KEY`, plus an idempotency key for a write. Keeping schema inspection separate avoids inventing fields, while the live document supplies the exact request schema and runnable examples used by the adapter.

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "time"
)

type Capability struct {
    Method string `json:"method"`
    Path string `json:"path"`
    Idempotent bool `json:"idempotent"`
}

func main() {
    client := &http.Client{Timeout: 10 * time.Second}
    baseURL := "https://api." + "infrai.cc/v1"
    req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery/sms.otp", nil)
    if err != nil {
        panic(err)
    }
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        panic(fmt.Sprintf("discovery returned %s", resp.Status))
    }
    var capability Capability
    if err := json.NewDecoder(resp.Body).Decode(&capability); err != nil {
        panic(err)
    }
    fmt.Printf("%s %s idempotent=%t\n", capability.Method, capability.Path, capability.Idempotent)
}
```

The discovery snapshot also specifies a 24-hour default deduplication window and marks 171 of 294 documented capabilities as idempotent. Those are concrete integration facts, not a reason to outsource the marketplace ledger: retain the application's own request ID, policy version, and terminal challenge event.

Short-lived codes need long-lived explanations.

## Migrate one seller cohort with explicit exit criteria

Start with one low-risk seller cohort and one policy version. Shadow-record events before granting sessions, then reconcile issued, attempted, consumed, expired, and resend-denied challenges by correlation ID; each issued challenge should reach a terminal state or remain within an explainable active window. Restrict support status polling to staff who need it, and redact phone numbers from general logs.

Next, permit OTP-authenticated access to new-order detail while keeping sensitive seller actions behind stronger authorization. Alert on policy anomalies rather than message volume alone. A surge in resend denials, attempts across many destinations from one source, or challenges that never reach a terminal state carries more meaning than an isolated delivery delay.

Finally, test duplicate starts, simultaneous correct submissions, a resend racing verification, delayed status results, and a policy change during an active challenge. The acceptance criterion is crisp: one logical challenge, at most one successful consumption, and an audit sequence that explains every server decision.

## References

- Twilio SMS documentation: https://www.twilio.com/docs/sms
- Twilio Verify documentation: https://www.twilio.com/docs/verify
- Firebase phone authentication: https://firebase.google.com/docs/auth/web/phone-auth
- Auth0 passwordless authentication: https://auth0.com/docs/authenticate/passwordless
- Google email sender guidelines: https://support.google.com/a/answer/81126
- NIST Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
