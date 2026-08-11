# Reconciliation-First Text Extraction: JSON, Long Documents, Timeouts, Token Limits

Short answer: make text-to-JSON extraction a bounded, replayable projection in which stable chunks produce schema-checked claims with source coordinates; use embeddings and reranking only to schedule likely evidence, never to define truth, and give each model call a deadline smaller than the job's timeout.

The useful unit of recovery is not the document. It is one deterministic claim attempt against one immutable span. A long document otherwise couples token limits, network deadlines, output validation, and final persistence into a single failure domain: when the call times out, the coordinator cannot know whether remote work continued, whether a response was lost, or whether retrying will duplicate a side effect. For a Node.js service, the remedy is architectural rather than a larger timeout: persist intent before dispatch, preserve evidence after return, and reconcile before publication.

Keep it boring.

## How should Node.js handle long-document JSON extraction when token limits cause timeouts?

Start with two budgets that must never be conflated. The context budget is the model's input allowance minus instructions, schema, expected output, and explicit safety headroom; the deadline budget is the caller's remaining time minus validation, durable write, and response overhead. Token estimation answers whether an attempt fits. Deadline propagation answers whether there is still time to finish it correctly. Increasing either number without measuring the other merely moves the failure.

A coordinator should assign a run identity from the source digest, extraction schema version, chunking policy version, prompt digest, and model configuration. Each work item then derives an idempotency key from that run identity plus stable source coordinates. This resembles a ledger because the same properties matter: entries are append-only, corrections preserve lineage, and replay must not silently reinterpret an earlier event under a newer schema. Node.js is a good orchestration runtime for this design, but its event loop doesn't change the contract; cancellation must flow through the model client, bounded concurrency must prevent queue pressure from becoming memory pressure, and a worker must not mark success until validated claims and attempt state are committed atomically.

RFC 9110 defines idempotent request methods in terms of their intended server effect and explains why a client may automatically retry an idempotent request after a communication failure. A model invocation made through `POST` does not acquire exactly-once behavior from HTTP. The application still needs a stable operation key, a uniqueness constraint, and a reconciliation path for the indeterminate case in which the request crossed the network but its response did not return. Don't call an unbounded retry policy resilience; it is duplicate-work amplification with a friendly label.

Timeouts should therefore become explicit states such as `ready`, `leased`, `succeeded`, `retryable`, and `terminal`, with attempt number, configuration digest, start time, deadline, and result digest recorded for audit. A lease may expire and permit another worker to proceed, but publication must compare-and-set against the operation key. This is an exactly-once mindset applied honestly: queues and transports commonly redeliver, while the durable state machine makes repeated delivery converge on one visible result.

## Chunk boundaries are correctness boundaries

Split on semantic structure before packing to a token estimate. Headings, paragraphs, table rows, transcript turns, and page regions retain source meaning better than arbitrary character windows; only after those units exist should a packer combine adjacent units under the conservative budget. When one unit is itself too large, subdivide it while retaining its parent coordinate and enough local context to interpret the target fields. Overlap can protect a clause that crosses a boundary, but every overlapped byte needs an origin label so aggregation can recognize duplicate evidence rather than count it twice.

The extraction response should be narrower than the final business object. Ask each chunk for candidate claims containing a schema field, typed value, source start and end, and a disposition such as `found`, `absent`, or `ambiguous`. Then validate JSON syntax, allowed fields, types, ranges, and provenance before storage. A final assembler can union set-valued fields, apply an explicit precedence rule to scalars, or surface conflict; it must not let whichever chunk completed last overwrite a contradictory amount, date, or account identifier.

This distinction matters under compliance review. Prompt and response bodies may reproduce regulated or personal data, so trace retention, access control, deletion schedules, and geographic processing limits apply to observability data as well as the source document. An audit trail need not retain unrestricted payloads forever: it can preserve digests, versions, coordinates, decisions, and access-controlled evidence according to policy. The governing requirement is that an authorized reviewer can explain which immutable text supported a published field and which program version accepted it.

Consider a payment agreement whose first section defines settlement as two business days after capture, while an annex makes a particular transaction class settle on the next eligible banking day and a later amendment changes the agreement's effective date. Three chunks can each return valid JSON and still create an invalid contract-level object. Concatenation loses precedence; last-write-wins makes network timing decide legal meaning; majority voting treats repeated boilerplate as stronger than one controlling exception. The assembler instead needs typed candidates with direct spans, a deterministic rule for amendment precedence, calendar semantics supplied by trusted application code, and an unresolved state when the documents do not establish an answer. If retrieval selects only the opening definition, the JSON may look cleaner while being less correct. This is why the source map, retrieval candidate set, schema version, and merge decision belong in the same audit narrative: reconciliation is not a cleanup step after extraction but the operation that determines whether several locally plausible claims may become one authoritative record.

The following Go example isolates the commit rule. A production Node.js worker can implement the same interfaces with its database transaction and cancellation primitive; the language is incidental, while deterministic identity and atomic visibility are not.

```go
package projection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type Span struct {
	RunID string
	Start int
	End   int
	Text  string
}

type Claim struct {
	Field string          `json:"field"`
	Value json.RawMessage `json:"value"`
	Start int             `json:"start"`
	End   int             `json:"end"`
}

type Model interface {
	Extract(context.Context, string) ([]byte, error)
}

type Journal interface {
	Reserve(context.Context, string) (bool, error)
	Commit(context.Context, string, []Claim) error
}

func Project(ctx context.Context, span Span, schemaDigest string, model Model, journal Journal) error {
	sum := sha256.Sum256([]byte(span.RunID + "\x00" + schemaDigest + "\x00" + span.Text))
	key := hex.EncodeToString(sum[:])
	reserved, err := journal.Reserve(ctx, key)
	if err != nil || !reserved {
		return err
	}

	raw, err := model.Extract(ctx, span.Text)
	if err != nil {
		return err
	}
	var claims []Claim
	if err := json.Unmarshal(raw, &claims); err != nil {
		return err
	}
	for _, claim := range claims {
		if claim.Start < span.Start || claim.End > span.End || claim.Start > claim.End {
			return errors.New("claim evidence falls outside its immutable span")
		}
	}
	return journal.Commit(ctx, key, claims)
}
```

One subtlety is deliberate: a reservation is not proof of completion. The journal needs an attempt record that can be leased again after its bounded deadline, while `Commit` must be transactional and unique on the key. Holding a database transaction open during inference would turn model latency into lock contention, so reserve briefly, perform inference outside the transaction, then commit with a compare-and-set. If a late worker races a replacement, one commit wins and the other result remains an auditable attempt rather than a second published fact.

## Retrieval is a scheduling policy, not an extraction guarantee

Embeddings and reranking help when a large corpus contains sparse evidence for a small number of fields. Derive a query from the field definition, retrieve a candidate set, rerank candidates for that field, and extract from the selected spans. Record the query text or digest, embedding configuration, candidate identifiers, scores, reranker configuration, and final selection. Without that lineage, a missing field cannot be classified as absent evidence, retrieval miss, or extraction miss.

The catch is recall. A reranker can reorder only candidates supplied to it, and no extractor can recover a clause excluded upstream. Retrieval-first processing is therefore not suitable when the contract requires every fee, every exception, or every named party; use an exhaustive chunk scan for completeness-sensitive fields, perhaps letting retrieval prioritize the queue without allowing it to prune work. For exploratory lookup over a huge archive, retrieval can be proportionate. I'm not sure a given corpus has adequate recall until a labeled evaluation set includes awkward tables, OCR noise, cross-references, negations, and decisive passages near chunk boundaries.

| Method | Appropriate constraint | Principal risk | Evidence to retain |
| --- | --- | --- | --- |
| One validated call | Source and output fit with measured headroom | One deadline couples all work | Source, configuration, response, validation result |
| Exhaustive stable chunks | Omission is more costly than extra inference | Boundary context and duplicate claims | Span map, per-chunk attempts, merge decisions |
| Embeddings plus rerank | Target evidence is sparse and some recall loss is acceptable | Relevant evidence never reaches extraction | Query, candidates, scores, selection, claims |
| Hierarchical compression | The output is a non-authoritative synthesis | Early summaries erase exact qualifiers | Full summary lineage and original references |

Costs follow the same boundary analysis. Exhaustive scanning spends inference on irrelevant spans but makes coverage legible; retrieval adds indexing, evaluation, and operational state while potentially reducing calls. A single request has fewer components and may be the best design for short, low-volume documents. Chunking is not suitable when a field depends on global structure that cannot be represented by attached context or a deterministic merge rule. There is no universally superior pipeline — choose the least complex one whose omissions, retries, and reconciliation behavior meet the business obligation.

## Validation, observability, and replay decide whether the design works

Syntactically valid JSON is only the first checkpoint. Enforce a closed schema, distinguish absent from null, reject unknown enum values, verify source offsets against immutable bytes, and run domain invariants after assembly: currency and amount must travel together, effective dates must form permitted intervals, identifiers must pass their checksum or format rule, and totals must reconcile where the source supplies components. A semantic rejection should never be rewritten as a model timeout, because those failures demand different action and carry different audit meaning.

Observe the stages separately: queue delay, lease age, estimated and reported tokens, inference latency, cancellation, JSON decoding, schema rejection, provenance rejection, conflicts during merge, retrieval recall on labeled cases, and publication lag. Avoid one aggregate success percentage. A system that emits plausible JSON for 99 requests and silently omits the controlling clause on the hundredth has a different risk profile from one that rejects that document for review, even if a dashboard assigns both the same nominal failure rate.

Test with fault injection rather than prompt examples alone. Cancel calls just before their deadline, deliver the same work item twice, reorder chunk completion, return truncated JSON, introduce overlapping contradictory claims, rotate configuration between queued and running work, and replay old attempts after a schema revision. The expected invariant is compact: no unvalidated claim becomes visible, no retry changes the interpretation version of an existing run, and every published field has a traceable evidence path.

No magic here.

A self-hosted gateway may centralize authentication, routing, retry policy, and telemetry across model providers, while direct adapters reduce one network and policy boundary. Neither arrangement supplies application idempotency or evidence provenance. Treat the gateway as a replaceable transport interface, pin its effective configuration in the run record, and keep chunk identity independent of provider routing so a controlled migration can compare results rather than erase lineage.

## Roll out by replaying immutable runs

Begin with shadow publication into a separate namespace, compare candidate fields against accepted records, and classify differences as parsing, chunking, retrieval, extraction, aggregation, or validation errors. Canary by a deterministic source key so every retry for one document stays on the same pipeline version. Promotion should switch routing for new runs; it should not mutate prior claims in place.

Keep rollback equally plain. Retain the previous reader while downstream consumers learn the new schema, stop new assignments to the canary version when an invariant fails, and replay immutable source spans after correction under a new run identity. The final release criterion is not that the model usually answers. It is that duplicate delivery, timeout, cancellation, and version change all leave a reconcilable record.

## References

- RFC 9110, *HTTP Semantics*: https://www.rfc-editor.org/rfc/rfc9110
- LiteLLM source repository: https://github.com/BerriAI/litellm

## Further reading

- https://www.rfc-editor.org/rfc/rfc9110
- https://github.com/BerriAI/litellm
