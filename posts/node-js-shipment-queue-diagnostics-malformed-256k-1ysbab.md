# Node.js Shipment Queue Diagnostics (Malformed 256KB JSON, Parse Errors, Schema Rejection)

**Short answer:** recover a malformed or oversized shipment-update job only when the system can prove what bytes were admitted, which contract judged them, and which subscriber effects already committed; a 256KB rejection, a JSON parse error, and a schema validation error require separate dispositions, not blanket retry in a Node.js background job queue.

For a media business that fans one shipment update out to many subscribers, the immediate fix is to keep the queued envelope bounded: assign an immutable event identifier, store a versioned audience snapshot outside the job, and publish the snapshot reference rather than a growing recipient array. Measure the exact UTF-8 serialization before publication. At consumption, classify syntax before schema, quarantine deterministic rejections, and reserve retry for failures that can plausibly change without changing the message.

Operational recovery is the governing constraint. Throughput matters later.

## How should incident command handle a malformed 256KB JSON payload in a Node.js background job queue?

Start with evidence, not another delivery attempt. The recovery record needs a payload digest, measured byte count, event identifier when one can be obtained safely, schema version when parsing succeeds, failure class, transport delivery identity, timestamps, and disposition. It does not need an unrestricted copy of every subscriber address. This record creates a finite state machine: admitted, rejected for size, unparsable, contract-invalid, execution-pending, effect-ambiguous, or complete. Each state admits a small set of legal next actions.

The 256KB figure must be an explicit application or transport constraint, not folklore. If the stated ceiling is 256 KiB, the corresponding byte boundary is 262,144; if a transport documents a different meaning of KB or counts metadata toward its envelope, its documented accounting rule controls. The producer should serialize once, measure those actual bytes, and pass the same byte slice to the publisher. Don't measure object fields, JavaScript string length, or a pre-compression estimate and assume that number describes what crosses the boundary.

Classification then becomes mechanical:

| Observed state | Can unchanged bytes succeed? | Recovery action |
|---|---:|---|
| More than the configured byte ceiling | No | Reject before publish; move expanding data behind a versioned reference |
| JSON syntax failure | No | Quarantine the original delivery and notify the contract owner |
| Valid JSON, invalid schema | No | Quarantine with validation paths and schema version |
| Valid job, transient execution dependency | Possibly | Retry with a bound and the same business identity |
| External effect has an unknown outcome | Unknown | Reconcile before any further send |

That final row is the expensive one. An acknowledgement says something about delivery processing, while the shipment notification itself is a business effect; treating the former as proof of the latter creates a gap precisely where an operator most needs certainty. RabbitMQ's acknowledgement documentation describes acknowledgements as a protocol mechanism for confirming delivery processing and explains that unacknowledged deliveries may be requeued when a channel or connection closes. The application still has to decide, durably, what happened to each subscriber effect.

## Freeze the fan-out before repairing the producer contract

Queue position is transport history. The recoverable business history is an immutable shipment `event_id`, a versioned `audience_snapshot_id`, and one recipient-effect identity such as `(event_id, subscriber_id, channel)`. Put a uniqueness constraint around that identity in the effect ledger. Duplicate delivery may repeat computation, but it must not create a second committed intent for the same update and subscriber. This is an exactly-once mindset applied to state transitions, not a promise that a transport delivers exactly once.

Consider a concrete interruption. A snapshot contains ten pages; pages one through six have completed, page seven has 83 committed recipient effects and one outbound intent whose result is unknown, while pages eight through ten have not been claimed. Re-enqueuing the whole snapshot under a new event identifier is indefensible because it discards the identity attached to the 83 known effects. Blindly acknowledging page seven is equally weak because it may abandon the uncertain subscriber. The recovery controller should freeze that single ambiguous intent, look for downstream evidence keyed by the original recipient-effect identity, record either confirmed completion or confirmed absence, and permit another send only after absence is established. If the downstream channel cannot provide decisive evidence, the item remains ambiguous for authorized review; I'm not sure a universal timeout can settle that question, because the necessary interval depends on the channel's evidence and the business's duplicate-notification tolerance.

No blind replay.

Only after page seven has a durable disposition should its cursor advance. Separately claimed later pages may proceed when ownership rules prevent overlap, but their progress cannot erase the unresolved effect. The audit trail should connect the accepted envelope digest, snapshot version, claim, attempt, effect identity, acknowledgement decision, and final disposition. That linkage gives reconciliation a question it can actually answer: “Which subscribers lack a proven outcome for shipment event E?” A generic failed-job counter cannot answer it.

Acknowledgement belongs after the durable decision represented by the ledger. If the worker acknowledges before committing its disposition, interruption can lose work; if it invokes an external channel before recording an intent, interruption can leave an effect with no local evidence. An outbox-style intent narrows that uncertainty, and a downstream idempotency key narrows it further when the channel supports one, but neither justifies inventing a fresh event identity during recovery.

## Repair the contract from the captured bytes

Although the production services in the question use Node.js, the contract should not depend on a JavaScript runtime. The following Go code expresses the boundary logic as a small, testable component: a producer admits only the exact serialized bytes under the configured ceiling, and a consumer distinguishes malformed JSON from a structurally invalid shipment event. A Node.js implementation should preserve this order with `JSON.stringify`, `Buffer.byteLength(serialized, "utf8")`, `JSON.parse`, and its selected JSON Schema validator.

```go
package shipmentjob

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const MaxMessageBytes = 256 * 1024

type Envelope struct {
	EventID            string `json:"event_id"`
	SchemaVersion      int    `json:"schema_version"`
	ShipmentID         string `json:"shipment_id"`
	AudienceSnapshotID string `json:"audience_snapshot_id"`
	OccurredAt         string `json:"occurred_at"`
}

type Publisher interface {
	Publish(body []byte) error
}

type ContractValidator interface {
	Validate(value any) error
}

type Rejection struct {
	Code        string
	Digest      string
	ActualBytes int
	Cause       error
}

func EncodeAndPublish(p Publisher, job Envelope) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode shipment job: %w", err)
	}
	if len(body) > MaxMessageBytes {
		return fmt.Errorf("PAYLOAD_TOO_LARGE: actual=%d limit=%d", len(body), MaxMessageBytes)
	}
	return p.Publish(body)
}

func Classify(body []byte, contract ContractValidator) (any, *Rejection) {
	sum := sha256.Sum256(body)
	base := Rejection{
		Digest:      hex.EncodeToString(sum[:]),
		ActualBytes: len(body),
	}

	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&value); err != nil {
		base.Code = "JSON_PARSE_ERROR"
		base.Cause = err
		return nil, &base
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		base.Code = "JSON_PARSE_ERROR"
		base.Cause = errors.New("trailing content after JSON value")
		return nil, &base
	}
	if err := contract.Validate(value); err != nil {
		base.Code = "SCHEMA_VALIDATION_ERROR"
		base.Cause = err
		return nil, &base
	}
	return value, nil
}
```

The decoder's second read rejects trailing JSON values by requiring `io.EOF`. The essential contract is independent of that implementation detail: the consumer accepts exactly one JSON value and then validates its shape. A quoted JSON document created by accidental double serialization is a useful test because it can be syntactically valid while still violating an object schema.

Never truncate. Truncation can turn one explicit admission error into incomplete yet parseable business data, and no recovery controller can infer the omitted recipients or fields afterward.

## Draw the compliance boundary around recovery evidence

Detailed evidence can itself become a liability. Hashing the rejected bytes supports correlation without spraying raw subscriber data across logs, while validation paths and stable identifiers usually reveal more about the fault than a full body dump. Access to quarantine, reconciliation, and replay should be audited because those controls can reproduce effects across an audience.

The catch is that reference-based envelopes are not suitable when workers cannot reliably read shared durable storage, or when the referenced snapshot can mutate. In that setting, an immutable inline event can be the better design if it remains within the documented envelope, and a transport with an appropriate documented size model may be necessary when the complete immutable record cannot fit. References bound queue payloads and centralize retention, but they add snapshot availability, read amplification, access control, and garbage-collection coordination. The correct choice depends on the recovery objective, not on publish throughput alone.

Auditability does not mean retaining personal data forever. GDPR Article 17 defines a right to erasure and lists exceptions; deciding the legal basis, retention period, or applicability of an exception requires qualified review. An engineering design can support that review by separating subscriber contact data from non-personal event evidence, retaining references rather than copying contact details into every job, and making deletion or irreversible unlinking an explicit lifecycle operation.

## How can a team migrate the queue contract without stranding old messages?

Deploy the observer first. Record size, digest, schema version, and classification without changing disposition; this establishes which producers and retained messages exist. Next, enforce producer-side byte admission while consumers continue to read every schema version still present within queue and quarantine retention. Then enable deterministic quarantine and the recipient-effect ledger. Automatic replay comes last, and only for states whose transition rules have been exercised under interruption.

Boundary tests should serialize bodies of 262,143, 262,144, and 262,145 bytes for a configured 256 KiB cap. Add malformed syntax, a scalar JSON root, an extra trailing value, missing required fields, unknown fields according to the chosen contract policy, and an unsupported schema version. Recovery tests should interrupt before ledger commit, after ledger commit but before acknowledgement, and after external intent creation with no confirmed result. The assertion is never just that the worker turns green; every accepted event and subscriber effect must end with one explainable disposition under its original identity.

Keep rollback asymmetric. Turning off a new producer schema is easy; removing its reader while messages of that version remain is not. A rollout is complete only when retained work, quarantine evidence, and reconciliation tooling agree that the old reader can be retired without making an earlier shipment update undecodable.

## References

- https://www.rabbitmq.com/docs/confirms
- https://gdpr-info.eu/art-17-gdpr/
