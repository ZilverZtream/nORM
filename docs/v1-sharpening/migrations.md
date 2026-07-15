# Domain 4 — Schema & migrations

**Scope:** provider-correct DDL generation, up/down data preservation, rename detection,
SQLite table-recreate, and advisory-locked concurrent deploys.

## 1.0 exit criteria

- [ ] The migration data-preservation fuzzer (up/down oracle over random schemas + data) runs
      **dry for a sustained window** with zero new kills.
- [ ] Every `Down` is runnable standalone and restores dropped NOT NULL columns with the pinned
      type-zero backfill contract — no unrunnable-standalone migrations.
- [ ] SQLite table-recreate: exactly one recreate per table per direction; rename folds into the
      recreate; `SchemaDiff.Table` reference inconsistencies are handled.
- [ ] Temporal/provider DDL precision is correct (e.g. MySQL DATETIME(6)/TIME(6), TINYINT
      UNSIGNED, temporal-default rewrite).
- [ ] Concurrent deploys are advisory-locked per provider; safe rename detection documented.

## Current confidence

Strong. The preservation fuzzer found and closed the unrunnable-standalone dropped-NOT-NULL-
restore kill (Down now always recreates; type-zero backfill contract pinned). SQLite one-recreate
invariant and rename-fold are in place.

## Open items

- [ ] Sustain the preservation fuzzer dry window; record schema/seed ranges.
- [ ] Re-verify advisory-lock behaviour on live SQL Server / PostgreSQL / MySQL under concurrent
      deploy.
- [ ] Confirm rename detection vs DROP+ADD boundary is documented and matches `[RenameColumn]`.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~MigrationDataPreservation"`
- `dotnet test tests/ --filter "FullyQualifiedName~MigrationRename|FullyQualifiedName~SchemaSnapshot"`
- Live migration phase of the release gate.

## Risks

`SchemaDiff.Table` refs are inconsistent by design (DroppedColumns = old, rest = new); any diff
consumer must respect that split.
