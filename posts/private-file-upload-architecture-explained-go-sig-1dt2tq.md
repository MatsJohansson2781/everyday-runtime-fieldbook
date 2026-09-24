# Private File Upload Architecture Explained: Go Signed Download and Storage Retention

Keep training artifacts private, transfer bytes directly between the browser and object storage, and make retention an explicit server-owned record. Short answer: issue narrow, short-lived upload and download grants only after checking the user, artifact, region, and lifecycle state; a signed URL delegates a transfer, not the decision to keep or disclose the object. For a US/EU developer-tools service, the hard question is whether a deleted artifact can still be reached through an old grant, a replica, or a backup after its declared deadline.

## How should private file upload architecture handle signed download access?

Treat the application database as the authority for artifact ownership, region, and deletion deadline. A manifest row can hold an opaque object key, tenant ID, expected size and checksum, region, state, and retention cutoff. The object store holds bytes, not permissions. A browser never chooses an arbitrary bucket, key prefix, region, or expiry. Upload authorization is tied to a pending row and a single object key; download authorization requires a fresh read of that row. Suppose a training run produces a model checkpoint and its manifest: deletion of the checkpoint alone does not meet the policy if the manifest still exposes private file names, tenant identifiers, or links to another copy. Define the complete artifact set at reservation time, and reconcile every member against its own deletion deadline.

Three transitions deserve separate audit events: grant issued, upload verified, and deletion completed. A successful browser PUT alone proves neither that the expected bytes arrived nor that a usable training artifact exists. An attacker may also upload unexpected content under an allowed name; apply size limits, content validation, and malware screening appropriate to the accepted file types before promoting the row to ready, consistent with OWASP's file-upload guidance. Keep signing credentials outside browser code and rotate and scope them according to secrets-management practice.

The receipt matters.

The database transaction and object transfer cannot be one atomic transaction. Make each transition idempotent: a repeated completion request compares the stored checksum and object metadata before accepting the same result, while a conflicting result fails. This is an exactly-once *effect* goal, not a claim that network delivery occurs exactly once.

Never infer completion from a redirect.

## Which boundary decides retention?

For training artifacts, choose a deletion deadline at creation and record the policy version that produced it. If a dataset is needed for reproducibility, retention must be justified explicitly; indefinite storage is not a neutral default. One defensible design separates a short-lived upload reservation from a verified artifact and later from a deletion tombstone. The tombstone keeps an audit trail without retaining the file contents. A legal hold, if applicable, needs its own authorization, reason, and release procedure rather than a silent change to the expiry field.

| Transfer path | Authorization boundary | Retention consequence | Suitable use |
| --- | --- | --- | --- |
| Browser to private object store with signed grants | Server checks the manifest before each grant | Deletion worker must remove bytes and invalidate access paths | Large artifacts when direct transfer is practical |
| Application-proxied transfer | Server checks every request | Easier immediate denial, but application bears byte traffic | Small or specially inspected payloads |
| Public object URL | No per-user authorization at read time | Incompatible with private artifact access | Intentionally public files only |

A signature cannot generally be recalled just by marking a database row deleted. Set grant lifetimes to the shortest workable interval, avoid issuing new grants once deletion starts, and assess whether the storage system supports revocation or key rotation for the threat model. Record the maximum outstanding grant lifetime as part of the deletion service-level definition. Backups, versions, replicas, caches, and exported copies require separate inventory and deletion or expiry rules; deleting the current key is not proof that all copies have disappeared.

GDPR does not prescribe a universal retention period or demand that every byte stay in the EU. It does require storage limitation, appropriate security, and a lawful basis for processing; transfers of personal data outside the EEA need an applicable transfer mechanism and safeguards. A US/EU deployment therefore needs a documented data map and legal review, not a region selector marketed as compliance. Check where object bytes, metadata, logs, backups, and signing operations actually reside.

## How does the critical path work?

The following Go sketch isolates the policy decision from the storage signer. Its interfaces deliberately omit any vendor-specific endpoint. The transaction that reserves a key must persist the tenant and region selected by policy; callers may request an artifact but may not supply a storage key. A real implementation also validates size and content type before reserving, uses an unpredictable key, and bounds the grant lifetime.

```go
type Artifact struct {
    ID, TenantID, Region, Key, State string
    RetainUntil time.Time
}

type Repository interface {
    Get(ctx context.Context, artifactID string) (Artifact, error)
}

type Signer interface {
    Put(ctx context.Context, region, key string, expires time.Time) (string, error)
    Get(ctx context.Context, region, key string, expires time.Time) (string, error)
}

func DownloadGrant(ctx context.Context, repo Repository, signer Signer,
    tenantID, artifactID string, now time.Time) (string, error) {
    a, err := repo.Get(ctx, artifactID)
    if err != nil { return "", err }
    if a.TenantID != tenantID || a.State != "ready" || !now.Before(a.RetainUntil) {
        return "", errors.New("artifact unavailable")
    }
    return signer.Get(ctx, a.Region, a.Key, now.Add(5*time.Minute))
}
```

The five-minute expiry is an illustrative policy value, not a standards requirement. Authenticate the caller before this function, and log the grant decision without logging the signed URL itself: query parameters can carry bearer-style credentials. For uploads, reserve a pending row, sign a write constrained to its assigned key, then verify the stored object's size and digest before changing the row to ready. Concurrent completion calls should converge on one verified result.

## What happens when deletion races a download?

Define the guarantee precisely. Mark the row deleting before the deletion worker touches storage, so new grants fail; let already issued grants expire under the documented maximum lifetime, or enforce a stronger revocation mechanism if immediate cutoff is required. The worker deletes the object and any known versions, confirms the result, and only then records completion. Retries must be safe when the object is already absent. Keep the tombstone and audit event even if a transient storage error delays the physical delete; alert on overdue deletions rather than treating a queued job as evidence of erasure.

Test that sequence with a clock you control: issue a grant, move the row to deleting, attempt another grant, repeat the delete after a simulated timeout, and verify that an outstanding grant's possible lifetime is accounted for. In deployment, separate signing authority by environment and region, restrict object permissions, and rehearse restoration from backups against the same retention rules. Measure pending uploads that never complete, grants refused after deletion, and elapsed time from deadline to verified removal. These measurements illuminate different failures; one aggregate success rate hides them.

The rejected alternative is to treat an object-store lifecycle rule as the sole retention ledger. Lifecycle expiry is useful for bulk cleanup of abandoned uploads or as a backstop, but its schedule does not by itself establish who authorized a deadline, whether a signed reader remains active, or whether a backup copy persists. Use it alongside the manifest and reconciliation process, not as their substitute. If an operator changes policy, preserve the old and new policy versions in the audit record and test the migration against objects whose deadlines would move in either direction. Reconciliation must report discrepancies between database state and object inventory, including orphaned uploads and missing verified objects, without converting a missing object into a fabricated deletion receipt.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://gdpr-info.eu/art-5-gdpr/
- https://gdpr-info.eu/art-32-gdpr/
- https://commission.europa.eu/law/law-topic/data-protection/international-dimension-data-protection_en
