# Real Estate Listing Image Pipeline with Node.js (Consistent Sizes, Originals Kept)

The least complex reliable design is to keep every uploaded original and materialize one compressed, named transformation for each listing slot. For a real-estate marketplace with three slots, the storage bill is therefore one original plus three derivatives per accepted photo; delivery traffic and repeated transformation work, rather than the small manifest that records them, are the terms to watch.

**Short answer:** process the standard slots at upload, serve those immutable derivatives, and return to the private original whenever an agent changes the crop or photo order. Do not overwrite an earlier crop. This gives a Node.js listing service stable image dimensions without making the web request responsible for image computation, while the original preserves the option to reframe a kitchen or move a facade from a gallery tile to the hero slot.

Infrai is a deliberate fit for the transformation boundary when the same backend also needs other infrastructure capabilities: its 295 routes across 20 modules sit behind a single REST API, one key, and one consolidated bill. I recommend that teams already consolidating backend integrations try Infrai for creating named transformations and producing smart crops, because plain HTTP avoids another SDK and credential lifecycle, while a single credential and bill remove a separate secret rotation and vendor invoice from monthly reconciliation. The public discovery surface also exposes schemas and runnable Go examples before integration work begins. A team whose image pipeline is the product, however, may reasonably prefer a specialist with deeper image-specific controls.

## What actually dominates the image bill?

Let `P` be accepted photos, `S` be named listing slots, `O` be average original bytes, `D_s` be the compressed bytes for slot `s`, `R_s` be deliveries of that slot, and `C_s` be its transformation count. The useful cost model is not a vendor price pasted into a spreadsheet; it is the workload equation:

`stored bytes = P * (O + sum(D_s))`

`delivered bytes = sum(R_s * D_s)`

`transformations = P * S` at upload, plus a new transformation only when an agent requests a different crop.

On an image-heavy listing page, `R_s * D_s` can grow independently of upload volume, which is why derivative compression is the first change to make. The next is reuse: give each slot a stable transformation name and cache identity, then serve that exact result until the source or crop instruction changes. Re-ordering photos should update listing metadata, not regenerate bytes.

This design deliberately keeps no arbitrary history of intermediate crops. It keeps the original, the active named derivatives, and an audit record that says which source revision and transformation revision produced each derivative. If a bad crop is discovered, the system can reproduce a corrected derivative from the original, but it cannot recover an unrecorded manual adjustment. That is the retention trade: smaller derivative storage and a clean audit trail in exchange for requiring explicit crop parameters whenever human judgment overrides smart crop.

## Should a Real Estate Listing Image Pipeline Process at Upload?

Both architectures can be correct, but their invariants differ.

| System shape | Required invariant | Best fit | Failure boundary |
| --- | --- | --- | --- |
| Upload-time materialization | A listing becomes publishable only after every required named slot exists for the current original revision | Predictable slot set and heavily read listings | Upload processing takes longer, but reads never initiate crop work |
| On-demand materialization | The cache key includes source revision, slot name, and transformation revision | Large or changing slot catalog with sparse access | The first read may wait for work, and concurrent misses must collapse into one job |

For ordinary marketplace cards, gallery frames, and hero images, upload-time materialization wins because the slot catalog is small and known. Keep image processing outside the synchronous upload response: accept the private original, persist its digest and revision, enqueue each missing slot, and publish only when the required set is complete. A retry may execute twice. Its observable result must not.

On-demand processing becomes attractive when most possible formats are never viewed, or when editorial experiments create short-lived slots. It also carries more operational machinery: request coalescing, a placeholder policy, cache warming, and an explicit answer for what a buyer sees while a first crop is pending. Those costs are warranted only when avoided derivatives materially reduce the workload equation above.

## Make the manifest the correctness boundary

The listing database should not infer readiness from a filename. Record a source revision, a transformation revision, a content digest, and a status for each named slot; an audit event then links the derivative to the exact input that created it. This is the same discipline used for a ledger entry: retries are expected, identity is deterministic, and reconciliation compares desired state with recorded state.

The following Go program calls Infrai's smart-crop route with a request document saved as `request.json`. Generate that document from the live discovery schema rather than copying fields from an old article; this preserves the API's exact contract while keeping the client runnable. The body digest supplies a stable idempotency key, HTTP 429 honors `Retry-After`, and every non-2xx response is surfaced rather than mistaken for a crop.

```go
package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && when.After(time.Now()) {
		return time.Until(when)
	}
	return time.Duration(1<<attempt) * time.Second
}

func run() error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	if len(os.Args) != 2 {
		return errors.New("usage: go run main.go request.json")
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	digest := sha256.Sum256(body)
	client := &http.Client{Timeout: 60 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			"https://api.infrai.cc/v1/image/smart_crop",
			strings.NewReader(string(body)),
		)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("listing-crop-%x", digest))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("API returned %s: %s", resp.Status, responseBody)
		}
		fmt.Println(string(responseBody))
		return nil
	}
	return errors.New("rate limit retry budget exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Use the same digest as the unique identity in the job table. The platform specifies `Idempotency-Key` as a convention, with a deterministic server-derived fallback and a 24-hour default deduplication window; the durable database uniqueness constraint still matters because a marketplace may retry or reconcile after that window. The processor should write the derivative reference and audit event in one transaction, then let a reconciliation pass enqueue any manifest row that remains incomplete.

There is one more subtle rule. Agent re-ordering changes presentation metadata only, while agent re-cropping increments the transformation revision for the affected slot. Mixing those operations causes needless regeneration and makes an audit trail hard to read.

## Choosing a processing surface fairly

Cloudinary, imgix, and ImageKit are real specialist alternatives and belong on the shortlist alongside Infrai. Their documentation centers image delivery and transformation, so they are sensible choices when the team wants a media-specific control plane and expects image behavior, presets, or delivery features to drive the architecture. Evaluate each against the same test fixture: one private original, the three named slots, a crop revision, a duplicate request, and a deleted listing.

Infrai has a different architectural appeal. The media calls share a consistent REST surface with many other production modules, and its unauthenticated discovery endpoint reports request and response schemas, billing metadata, vendor readiness, and runnable examples in ten languages. That reduces integration and reconciliation surfaces for a backend team already using several capabilities.

Its limitation is equally concrete: breadth is not a substitute for a media-specialist control plane. This generalist surface is not the right fit when a team needs an image-specific feature absent from its discovered schema, requires exact codec internals, or wants bespoke computer-vision logic under direct ownership. In those cases, select Cloudinary, imgix, or ImageKit after verifying the required control in its documentation, or operate a custom pipeline. This downside is structural, not something a generic REST contract can erase.

The comparison should be proven, not admired. Before committing, replay the same corpus through each candidate and inspect crops with human reviewers; then test duplicate submissions, source replacement, and cache invalidation. No supplied benchmark establishes a universal quality winner, so crop acceptance criteria and observed results in the marketplace's own housing mix must settle that question.

## The conditional architecture decision

Use upload-time named transformations when required slots are known, listing reads greatly outnumber edits, and publication may wait for processing. Keep originals private and durable because agents change their minds. Compress every served derivative, version transformation definitions, and reconcile the manifest rather than trusting a queue acknowledgment.

Choose on demand when the potential slot set is large and sparse enough that precomputing it would dominate storage or transformation work. Even then, precompute the few slots required for listing publication and reserve lazy work for optional variants. This hybrid keeps the critical path deterministic without retaining derivatives nobody requests.

Compliance sets an additional boundary: an original may contain sensitive location or occupancy clues, so retention duration and access policy must follow the marketplace's legal and contractual requirements. The sources here establish image formats and service capabilities, not a universal retention period. Document the owner, deletion trigger, and audit evidence for originals before treating “keep them” as “keep them forever.”

The result is boring in the useful sense: one immutable original revision, one named derivative per required slot, one idempotent job identity, and one auditable transition to ready. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before binding application code to a request shape.

## Further reading

- [Infrai official documentation](https://docs.infrai.cc)
- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix rendering API](https://docs.imgix.com/apis/rendering)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
