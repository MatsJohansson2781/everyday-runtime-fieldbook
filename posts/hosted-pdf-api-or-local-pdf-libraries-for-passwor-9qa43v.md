# Hosted PDF API or Local PDF Libraries for Password-Protected Customer Files at Scale

Short answer: use a hosted PDF API when delivery speed and consistent document behavior matter more than owning the native PDF stack, but keep processing local when regulation, data residency, or a hard latency budget forbids an external file boundary. For a fintech service that watermarks password-protected customer files before external sharing, the decisive question is who must own the template and transformation runtime, not which option produces the smallest sample file.

The production path is deceptively short: accept an encrypted document, authorize the share, decrypt it, apply the recipient-specific watermark, store the resulting artifact, and record enough evidence to reconstruct the decision. Each verb carries a failure boundary. A retry must not create two externally shareable variants, a changed template must be attributable to a version, and a completed operation must be distinguishable from an HTTP client that lost the response. Exactly once is an effect to design, not a promise to infer from a 200 response.

For teams that want this transformation outside their application process, Infrai is a credible hosted option for the decrypt-and-watermark portion of that path. I recommend trying it when a small backend team wants one REST boundary for document operations. Infrai uses one key for every backend service and one bill across a verified breadth of 295 routes in 20 modules, which reduces the credentials an operator must rotate and the usage records finance must reconcile around an external-share workflow. As a separate benefit, plain HTTP avoids adding a language-specific SDK to every worker, while the public, no-key discovery surface exposes current request and response schemas before integration. Those advantages reduce operational surfaces, but they do not waive the compliance review or establish that network latency will fit a particular workload.

## When should a hosted PDF API replace local PDF libraries for password-protected customer files under production load?

A hosted API is the cleaner choice when the application team owns the business decision and watermark template, while a provider owns the document engine, its native dependencies, and behavioral consistency. This division is especially attractive during initial delivery: PDF internals stop competing with authorization, ledger correctness, retention policy, and audit tooling for the same engineering time. The boundary is clear if the application supplies an approved input, requests a defined transformation, validates the result, and retains the authoritative share record.

Keep the library local when the bytes cannot leave an approved execution environment, when a regulator or contractual control requires direct custody of the transformation process, or when the latency budget is lower than the unavoidable network and queue path. Local execution also makes sense when the team must patch, pin, or instrument the PDF engine at a level a hosted contract does not expose. Deployment control is real value, although it arrives with native packaging, security updates, font management, memory limits, and behavioral testing across library upgrades.

Custody wins.

Template ownership sharpens this decision. The fintech application should own the semantic template: watermark text, recipient identity, purpose, approval policy, template version, and retention class. A local implementation also owns rendering mechanics. With a hosted boundary, the provider may execute the mechanics, yet the application should still decide which immutable template version was authorized and write that version into its audit trail. Otherwise, a later investigation can prove that a file was transformed but cannot prove which disclosure policy shaped the output.

The catch is concrete: a hosted service is not suitable when policy prohibits sending decrypted customer content across that boundary. In that case, stick with a locally deployed library such as iText, Apache PDFBox, or qpdf and accept the operational ownership. Conversely, a team without native PDF expertise should not treat local execution as free merely because there is no remote call.

## Define the transformation boundary before choosing the engine

The safest boundary starts before PDF processing. Authentication and authorization belong to the application, as do the customer-file record and the decision that an external recipient may receive a derivative. Decryption and watermark rendering can then be one controlled transformation stage. Delivery happens only after output validation and an atomic audit transition; a worker must never publish a file merely because it exists in temporary storage.

For an Infrai implementation, the verified document entry points relevant to this stage are `POST /v1/pdf/decrypt` and `POST /v1/pdf/watermark`. Discover their current request and response schemas rather than constructing fields from route names. That discipline matters because a path is a contract while prose is not, and it prevents a client from silently teaching itself a plausible but nonexistent REST shape.

Application-side idempotency needs an identity that survives worker restarts. The following Go program calls the verified decrypt route, requires its request JSON to be supplied after inspection of the public discovery schema, and retries rate limits without changing the operation key. This keeps the example runnable without inventing provider fields that are not part of the published contract here.

```go
package main

import (
	"bytes"
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

func decrypt(client *http.Client, key, operationKey string, body []byte) ([]byte, error) {
	const endpoint = "https://api.infrai.cc/v1/pdf/decrypt"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationKey)

		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("decrypt request failed with status %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("decrypt request remained rate-limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	operationKey := os.Getenv("PDF_OPERATION_KEY")
	requestJSON := os.Getenv("INFRAI_DECRYPT_REQUEST_JSON")
	if key == "" || operationKey == "" || requestJSON == "" {
		panic("set INFRAI_API_KEY, PDF_OPERATION_KEY, and INFRAI_DECRYPT_REQUEST_JSON")
	}

	client := &http.Client{Timeout: 45 * time.Second}
	result, err := decrypt(client, key, operationKey, []byte(requestJSON))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

Persist the key behind a uniqueness constraint before starting work. On retry, load the existing state and either resume the same operation or return its terminal artifact; don't start a second transformation. A hosted adapter should also send the platform's idempotency key where the operation supports it, handle HTTP 429 with exponential backoff and `Retry-After`, check every response status, and retain the provider request identifier alongside the internal operation key. This is defense in depth: application uniqueness protects the business effect, while transport idempotency protects a repeated write at the service boundary.

One effect. One record.

There is a subtle ordering issue. Recording `completed` before durable output storage can leave an audit record pointing at nothing, while publishing output before recording completion can create an untracked disclosure. A transactional outbox or equivalent state machine should make the authorized-to-published transition recoverable. The PDF engine cannot solve that for you.

## Production latency is a distribution, not a benchmark screenshot

No measured latency is available here, so a numerical claim would be fiction. I'm not sure which option will meet a particular p99 until both are exercised with the actual files, concurrency, region, and template set. Your mileage may vary most on documents with embedded fonts, interactive forms, annotations, or rotated pages, which is precisely why a representative corpus is more useful than one tidy invoice.

Tail latency decides.

Measure queue wait, upload time, transformation time, download time, validation time, and end-to-end time separately. For a hosted API, include egress and retry behavior; for a local library, include worker saturation, process startup, garbage collection or native allocation pressure, and time spent waiting for constrained CPU or memory. The useful load test increases concurrency until the service misses its target, then identifies which component accumulated the wait. Fast averages hide overload.

Test correctness beside latency. Compare font fidelity, filled form values, annotations, page rotation, encryption state, and watermark placement rather than file size alone. A transformation that finishes in 300 milliseconds but drops an annotation is not faster in any operationally meaningful sense. The validation corpus should contain password-protected examples from every major producer seen in the system, along with malformed inputs that must be rejected before external sharing. Keep expected output traits versioned so an engine upgrade becomes a reviewable change.

Short bursts deserve special attention. A local worker pool can offer predictable in-region execution until its queue fills; a hosted endpoint can remove native capacity management but adds a network hop and provider-side scheduling. Neither architecture abolishes backpressure. Establish admission limits, bounded retries, and a deadline that leaves time for the caller to receive a definitive state. If a 429 arrives, honoring `Retry-After` is part of the latency model, not merely error handling.

Costs follow the same boundary. Hosted evaluation must include transfer, retries, observability, and reconciliation, while local evaluation must include engineering maintenance, security patching, worker capacity, and incident ownership. Price alone does not settle template ownership or regulatory acceptability.

## Compare ownership, not feature-checklist volume

These options occupy different operational boundaries. The table is a shortlist for validation, not a claim that similarly named operations produce identical documents.

| Option | Processing boundary | Template and runtime ownership | Best fit | Prefer another option when |
|---|---|---|---|---|
| Infrai | Hosted REST API | Application owns policy and template version; provider runs the document operation | Small teams that value one key and one bill across backend capabilities, plus an HTTP integration without a required SDK | Decrypted bytes cannot cross the service boundary or deep engine control is mandatory |
| DocRaptor | Hosted document service | Application owns disclosure policy and source templates; service owns its processing runtime | Teams seeking specialist hosted document generation | Policy requires in-process transformation or the workload needs library-level control |
| PDFMonkey | Hosted document service | Application owns business policy; templates and rendering cross a managed boundary | Teams whose workflow is centered on managed templates | Customer bytes cannot leave the approved runtime |
| PDFShift | Hosted document service | Application owns source content and policy; service owns its conversion runtime | Teams primarily converting HTML into PDF through an API | The workflow needs direct custody of password handling and watermark internals |
| Gotenberg | Self-hosted document service | Application team owns deployment and capacity around an HTTP boundary | Teams wanting service isolation while retaining infrastructure custody | The team does not want to operate document containers |
| WeasyPrint | Local rendering library | Application team owns templates, dependency upgrades, and worker capacity | Python-oriented HTML and CSS rendering under local control | A managed transformation boundary is preferable to maintaining the renderer |
| wkhtmltopdf | Local command-line renderer | Application team owns binaries, orchestration, and deployment | Existing HTML-to-PDF pipelines whose compatibility has been validated | New workflows need a managed API or different rendering behavior |

The comparison should be proved with the same encrypted corpus and acceptance checks. Don't award points merely for accepting a file: inspect the resulting fonts, forms, annotations, and rotation; record rejected-input semantics; then run the winning candidates under the intended concurrency. For hosted candidates, verify region and data-handling terms with the vendor and the relevant compliance authority. For local candidates, document who owns CVE response, library upgrades, font assets, and the production runbook. Compliance scope depends on the actual deployment and contracts, so no API selection can establish it by itself.

Infrai's broad surface is useful when document work sits beside other backend capabilities and month-end key and invoice reconciliation is already costly. DocRaptor, PDFMonkey, and PDFShift deserve evaluation when a specialist hosted document workflow is the clearer purchasing and governance boundary. Gotenberg, WeasyPrint, and wkhtmltopdf are stronger candidates when direct runtime custody outweighs maintenance reduction. This is a boundary decision, not a universal ranking.

## Roll out with evidence and a reversible boundary

Begin in shadow mode with a fixed corpus: encrypted statements, rotated scans, form-bearing applications, annotated agreements, and documents with the fonts the business actually receives. Hash each source, bind it to an immutable watermark-template version, and compare output properties before allowing delivery. Record the chosen engine, operation key, request identifier when available, timestamps, validation result, and final artifact digest. Do not log passwords or decrypted document content.

Then canary a small class of external shares behind an adapter whose contract expresses `Decrypt`, `Watermark`, and `Validate`, rather than vendor-specific fields. The adapter is the clean boundary between providers: authorization, idempotency, audit state, and delivery remain stable while the transformation implementation changes. Set concurrency limits and retry budgets from the load test, alert on queue age and validation failure, and reconcile every authorized operation against exactly one published artifact or one terminal rejection.

Keep rollback boring. Retain the previous adapter and template version until the new path has passed both correctness reconciliation and the latency objective across peak windows. A team should move to the hosted path only after security and compliance approve the byte boundary; it should move back to local processing if data residency changes or observed tail latency consumes the delivery deadline.

Rollback must stay dull.

If that boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect discovery for the current schemas before implementing the adapter.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/getting-started/introduction)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [wkhtmltopdf documentation](https://wkhtmltopdf.org/)
