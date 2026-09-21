# Postgres Nightly Usage Job — Turning Timeseries into Auditable Tenant Charges

**Short answer:** A nightly rollup should attribute usage to an immutable tenant identity captured when each event is accepted, never to whichever tenant owns an API key when the job happens to run. That rule permits a production key to rotate without downtime and keeps retries from changing billing: overlap old and new credentials briefly, resolve both to the same tenant, persist the credential identifier for audit, and insert each closed usage bucket under a database-enforced idempotency key. **Authentication can change; historical attribution cannot.**

## How should a nightly usage job turn timeseries into billing rows?

The dangerous schema stores only `api_key` beside a timestamp and defers tenant lookup until the nightly job. Rotation then changes the lookup table between ingestion and settlement. An event accepted under the retiring credential can become unresolvable after deletion, while an event retried under the replacement credential can look like fresh usage. A correct HTTP response at ingestion does not repair that accounting error.

Separate three identities instead: `tenant_id` is the billable principal, `credential_id` identifies the credential version used at admission, and `event_id` identifies the business occurrence. Store all three on the immutable usage event. The secret itself does not belong in that row; OWASP recommends limiting secret exposure, recording secret-management activity, and supporting rotation with a documented lifecycle.

For a zero-downtime change, create a replacement credential, allow both credential versions during a bounded overlap, move producers to the replacement, observe that traffic has left the retiring version, and only then revoke it. Revocation changes future authentication. It must not rewrite already accepted events.

Short overlap, long evidence.

Retries are normal.

## Fix the accounting boundary before writing the job

Define a half-open UTC interval, `[start, end)`, and close only intervals whose source data is past the system's documented lateness allowance. Half-open boundaries make an event at midnight belong to exactly one day, while a watermark prevents an arbitrary scheduler delay from becoming a billing decision. If late events are permitted after close, handle them as explicit adjustments with their own identifiers; silently editing a posted billing row weakens reconciliation and the audit trail.

A compact relational model is enough. The raw event table needs a uniqueness constraint on the producer's stable event identity, such as `(tenant_id, event_id)`. The derived table needs a second uniqueness constraint on the calculation identity, such as `(tenant_id, meter, period_start, period_end, rollup_version)`. These keys answer different questions: did the system accept this business event before, and did it materialize this calculation before?

The version belongs in the rollup key because billing logic changes. A correction can run as a new version, compare totals, and produce an explicit adjustment without overwriting the evidence used by the earlier close. **Exactly-once billing is an outcome assembled from durable deduplication and transactional writes, not a scheduler promise.**

Keep money out of the first aggregation step. Sum integer quantities in the meter's documented base unit, preserve the period and rule version, and let a separately versioned rating stage apply contract terms. This division makes a quantity dispute distinguishable from a pricing dispute.

## A transactional rollup in Go

The query below locks one job run, aggregates accepted events by their stored tenant identity, and inserts deterministic rows. It deliberately does not join through the current credentials table. The database uniqueness constraint is the last line of defense when a worker loses its response and retries.

```go
package rollup

import (
    "context"
    "database/sql"
    "time"
)

type Window struct {
    Start time.Time
    End   time.Time
}

func Close(ctx context.Context, db *sql.DB, w Window, version int) error {
    tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // The lock key is stable for this UTC window; another worker may retry later.
    var locked bool
    err = tx.QueryRowContext(ctx,
        `SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`,
        w.Start.UTC().Format(time.RFC3339),
    ).Scan(&locked)
    if err != nil {
        return err
    }
    if !locked {
        return nil
    }

    _, err = tx.ExecContext(ctx, `
        INSERT INTO billing_usage
            (tenant_id, meter, period_start, period_end,
             rollup_version, quantity, source_event_count)
        SELECT tenant_id, meter, $1, $2, $3,
               SUM(quantity), COUNT(*)
          FROM usage_events
         WHERE occurred_at >= $1
           AND occurred_at <  $2
           AND accepted_at <  $4
         GROUP BY tenant_id, meter
        ON CONFLICT
            (tenant_id, meter, period_start, period_end, rollup_version)
        DO UPDATE SET
            quantity = EXCLUDED.quantity,
            source_event_count = EXCLUDED.source_event_count`,
        w.Start.UTC(), w.End.UTC(), version, w.End.UTC(),
    )
    if err != nil {
        return err
    }

    return tx.Commit()
}
```

The `accepted_at < end` predicate is a policy choice, not a universal law. A system that meters by occurrence time but accepts delayed events needs a later watermark or an adjustment path. What matters is that the cutoff is documented, observable, and reproduced during reconciliation. Using `SERIALIZABLE` also means callers must retry serialization failures with bounded backoff; it does not remove the need for unique keys.

`ON CONFLICT DO UPDATE` makes a replay converge when the eligible source set is unchanged. For a legally or operationally immutable posted ledger, use `DO NOTHING` after close and write differences to an adjustment table instead. The latter costs more operational machinery but gives reviewers a clearer chain of custody. I would choose it once rows cross the posting boundary.

The main limitation is workload shape. Postgres aggregation is a poor fit when the eligible event set cannot be retained in the transactional database, when a window exceeds the database's acceptable scan and lock budget, or when metering requires continuous sub-minute publication rather than a nightly close. The trade-off buys a simple transaction boundary by spending capacity on the primary data store. Choose a stream processor or analytical store instead when that trade-off is unacceptable, but require its output to arrive at a transactional posting boundary with stable tenant identity, deterministic calculation keys, and independently checked totals. Moving computation does not move the accounting obligation.

## Reconciliation is part of the write path

A green job status proves little. Record a run identifier, window, code or rule version, start and finish times, input event count, tenant count, summed input quantity, inserted row count, and adjustment count. Do not put raw API keys or secret values in logs. The credential identifier is sufficient to investigate whether traffic moved during rotation.

Three invariants catch most expensive mistakes:

- The sum of quantities selected from eligible raw events equals the sum represented by rollup rows for the same window and version.
- Replaying the same closed window changes neither row count nor quantity.
- Every accepted event retains one tenant identity even after its credential is revoked.

Test those invariants with an intentionally awkward fixture: tenant `t_204` sends event `evt_9001` with credential `cred_old`; the same event is delivered again after `cred_new` becomes active; an event occurs exactly at `00:00:00Z`; and the retiring credential is revoked before reconciliation. The expected result is one copy of `evt_9001`, stable attribution to `t_204`, and no boundary event counted twice. This is more useful than a large random fixture because each row represents a known failure mode.

Boundaries decide money.

Operational alarms should compare invariants, not merely watch duration. Alert when source and rollup quantities differ, when a supposedly closed window changes, when an unusual share of requests still uses the retiring credential near its deadline, or when serialization retries exhaust their budget. Retain audit data according to the applicable regulatory, contractual, and privacy limits; there is no universal retention period for fintech systems.

## Compare mechanisms by evidence, not convenience

| Mechanism | Prevents concurrent workers | Survives process loss | Proves stable billing attribution |
|---|---:|---:|---:|
| Scheduler singleton | Usually | No | No |
| Postgres advisory transaction lock | Yes, per database and lock key | The lock releases | No |
| Unique rollup constraint | Resolves duplicate inserts | Yes | Only for derived rows |
| Immutable tenant and credential IDs on events | No | Yes | Yes |
| Reconciliation totals and adjustments | Detects divergence | Yes, with retained records | Yes |

No single row in the table supplies the guarantee. The useful design combines admission-time identity, raw-event deduplication, a closed-window policy, a unique materialization key, and reconciliation. A queue with deduplication can reduce duplicate work, but queue delivery semantics do not replace these storage invariants.

## Roll out the change without losing the trail

First add nullable `tenant_id` and `credential_id` columns to new usage events, populate them at authentication time, and measure missing identities. Backfill only when a deterministic historical mapping exists; ambiguous events belong in a review queue rather than under a guessed tenant. Then add raw-event and rollup uniqueness constraints, run the new calculation in shadow mode over several closed windows, and compare counts and quantities without publishing its output.

Next rotate one production credential through the overlap sequence and verify that both credential versions resolve to one immutable tenant while producing distinguishable audit records. Enable the new rollup for that tenant cohort, rehearse a retry after commit with the response deliberately discarded, and confirm that the second execution converges. Finally revoke the old credential and retain its non-secret identifier with the authentication audit record.

The decision rule is compact: publish a billing row only when its source window is closed, its tenant attribution was fixed at admission, its calculation identity is unique, and its totals reconcile. Rotation is then an authentication lifecycle event, not a rewrite of financial history.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://www.postgresql.org/docs/current/transaction-iso.html
- https://www.postgresql.org/docs/current/explicit-locking.html#advisory-locks
- https://www.postgresql.org/docs/current/sql-insert.html
- https://www.rfc-editor.org/rfc/rfc3339
