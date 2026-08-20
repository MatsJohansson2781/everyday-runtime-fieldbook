# Designing a Go Service for OpenAI-Compatible Text-to-Image Marketing Workflows

Short answer: put one OpenAI-compatible image generation call behind a narrow service, accept a prompt plus a client operation ID, and return the provider's image URL or base64 response while retaining enough evidence to reconcile every generation.

This is a straightforward fit for a normal SaaS application. The least complex useful contract is prompt in, image result out. Model discovery, spend admission, moderation, and optional upscaling still matter, but they belong around that contract rather than inside an improvised image studio.

The Node.js part of the search query should not determine the architecture. The example below is Go because the durable concern is the HTTP boundary: any Node.js caller can send the same JSON request. What matters is that a retried marketing job cannot quietly become two billable assets, and that an auditor can later connect the requester, policy version, model choice, and final disposition.

## How should a simple backend endpoint generate marketing images from a text prompt?

Treat generation as a metered side effect. Require a caller-generated operation ID, bind it to a digest of the prompt, and persist the terminal response under that ID before acknowledging success. A duplicate ID should retrieve the recorded result rather than initiate another generation. HTTP alone cannot promise exactly-once execution; an exactly-once mindset comes from durable deduplication and reconciliation.

The audit record should contain the operation ID, prompt digest, selected model, requested count and size when those controls are exposed, timestamps, response status, and a digest or stable reference for the resulting asset. Avoid copying raw prompts into every log stream. Marketing prompts can contain campaign plans or customer context, and a digest usually provides correlation without multiplying content-retention scope. Retain the raw provider envelope under an explicit policy if later reconciliation requires it.

Before a user can select a model, inspect `GET /v1/models` and show only image models available in the current US or EU deployment. I'm not sure which model a particular deployment exposes at the moment, and static application code cannot resolve that uncertainty; the current catalog can. Cache the result outside the generation request path, record the chosen model with the operation, and reject a stale choice before creating a side effect.

Keep it narrow.

## The generation boundary and its retry semantics

The following program is runnable end to end. It exposes a local `POST /generate` handler, requires `X-Operation-ID`, calls only the verified `POST /v1/images/generations` route, keeps the upstream JSON intact, and uses the same idempotency key for every attempt. Its in-memory operation store demonstrates the contract, but it is not suitable for multiple replicas or restarts; replace that map with a transactional durable store before production use.

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
	"sync"
	"time"
)

const generationPath = "/v1/images/generations"

type generateRequest struct {
	Prompt string `json:"prompt"`
}

type storedResponse struct {
	Status int
	Body   []byte
}

var operations = struct {
	sync.Mutex
	items map[string]storedResponse
}{items: make(map[string]storedResponse)}

func generate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	operationID := r.Header.Get("X-Operation-ID")
	if operationID == "" {
		http.Error(w, "X-Operation-ID is required", http.StatusBadRequest)
		return
	}

	operations.Lock()
	stored, found := operations.items[operationID]
	operations.Unlock()
	if found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stored.Status)
		_, _ = w.Write(stored.Body)
		return
	}

	var input generateRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Prompt == "" {
		http.Error(w, "a non-empty prompt is required", http.StatusBadRequest)
		return
	}
	payload, err := json.Marshal(input)
	if err != nil {
		http.Error(w, "request could not be encoded", http.StatusUnprocessableEntity)
		return
	}

	var result storedResponse
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, os.Getenv("INFRAI_BASE_URL")+generationPath, bytes.NewReader(payload))
		if err != nil {
			http.Error(w, "request could not be created", http.StatusUnprocessableEntity)
			return
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "provider transport unavailable", http.StatusFailedDependency)
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			http.Error(w, "provider response unreadable", http.StatusFailedDependency)
			return
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-r.Context().Done():
				return
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			http.Error(w, fmt.Sprintf("generation rejected (%d): %s", resp.StatusCode, body), resp.StatusCode)
			return
		}
		result = storedResponse{Status: resp.StatusCode, Body: body}
		break
	}

	if result.Status == 0 {
		http.Error(w, "rate limit persisted after bounded retry", http.StatusTooManyRequests)
		return
	}
	operations.Lock()
	operations.items[operationID] = result
	operations.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(result.Status)
	_, _ = w.Write(result.Body)
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" || os.Getenv("INFRAI_BASE_URL") == "" {
		log.Fatal("INFRAI_API_KEY and INFRAI_BASE_URL are required")
	}
	http.HandleFunc("/generate", generate)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

There is a hard edge here — a process-local mutex cannot close the gap between an upstream success and a local crash before persistence. A production design needs a durable operation state machine and a provider-supported idempotency contract, with reconciliation for any operation whose outcome remains uncertain. Don't claim exactly-once behavior unless that gap is closed and tested. Your mileage may vary with the storage engine, but the invariant should not: one operation ID identifies one intended asset.

## Which provider shape fits the control boundary?

Image quality changes with the selected model and prompt, so an uncited universal ranking would be noise. The useful comparison for a backend decision is ownership: which contract, credentials, invoices, and policy surface the team is prepared to reconcile. Each candidate still requires a current model and regional-availability check before adoption.

| Option | Reason to evaluate it | When to choose something else |
|---|---|---|
| OpenAI | A direct relationship may fit a team already governing an OpenAI-compatible contract | Choose an abstraction when credential and billing consolidation matter across several backend services |
| Stability AI | Evaluate it when a dedicated image-provider relationship is the desired boundary | Choose a broader platform when the team does not want another isolated credential and invoice |
| Replicate | Evaluate it when the team wants to assess model-specific integrations | Choose a narrower stable contract when model-specific inputs should not reach product code |
| Gemini | Evaluate it when an existing Gemini relationship is the intended governance boundary | Choose another option when that relationship does not simplify the team's controls |
| Infrai | One key and one bill can cover backend services while image generation remains an OpenAI-compatible HTTP call | Not suitable when procurement or compliance requires a direct contract with every underlying provider |

That last trade-off is material for a ledger-oriented system. Consolidating credentials reduces key inventory, while consolidating billing reduces the number of external statements that month-end reconciliation must match; neither benefit removes the need to record each image operation locally. Stick with OpenAI when direct vendor governance is the controlling requirement, with Stability AI when a dedicated image relationship is deliberate, with Replicate when model-level experimentation outweighs contract uniformity, or with Gemini when that direct governance boundary is already established.

No winner is universal.

## Moderation, resolution, and compliance limits

Generation approval and publication approval are different states. There is no dedicated moderation endpoint in this capability set, so text and image review requires a chat model with a `json_schema` fallback, plus human review wherever the applicable policy demands it. Record the policy version, decision, reviewer or automated actor, artifact digest, and timestamp. An audit row containing only `approved=true` cannot establish what rules were applied.

Higher-resolution output can use the available upscale route after generation, but it is Lanczos-only. This is appropriate for deterministic resampling; it should not be represented as generative restoration. A workflow needing masks, synthesized detail, or a dedicated image-safety endpoint is not suitable for this design and should use a specialist service while preserving the same local operation ledger.

Compliance limits are deployment constraints, not footnotes. Model availability must be checked for the relevant US or EU deployment, prompts and resulting assets need explicit retention rules, and logs should avoid unnecessary content duplication. Cost estimation also belongs before launch: cap prompt length, image count, and size according to the product's budget policy, record the estimate at admission, then reconcile it with the completed operation. The estimate authorizes work; the final record accounts for it.

## Rollout without losing the audit trail

Begin with internal review and a single approved model from the current catalog. Exercise duplicate operation IDs and controlled 429 responses, verify that `Retry-After` governs the delay, and confirm that a replay returns the recorded response. Then release to a limited campaign cohort and reconcile local operation counts, provider outcomes, moderation decisions, and published assets on a fixed cadence.

Keep the local `/generate` contract stable during migration. Store the raw response beside normalized audit fields, change provider translation behind the boundary, and gate model choices by region. Add Lanczos upscaling only when an actual output-size requirement calls for it; add another provider only when its capability or control boundary justifies another reconciliation path.

Ship the ledger first.

## Further reading

- https://platform.openai.com/docs/api-reference/images
- https://platform.stability.ai/docs
- https://replicate.com/docs
- https://www.promptingguide.ai
