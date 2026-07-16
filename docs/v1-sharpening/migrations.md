# Domain 4 — Schema & migrations

**Scope:** provider-correct DDL generation, up/down data preservation, rename detection,
SQLite table-recreate, and advisory-locked concurrent deploys.

## 1.0 exit criteria

- [ ] The migration data-preservation fuzzer (up/down oracle over random schemas + data) runs
      **dry for a sustained window** with zero new kills. (NH-0401: dry on the current tree - 220
      tests; the sustained multi-week window is open.)
- [x] Every `Down` is runnable standalone and restores dropped NOT NULL columns with the pinned
      type-zero backfill contract — no unrunnable-standalone migrations (NH-0401).
- [x] SQLite table-recreate: exactly one recreate per table per direction; rename folds into the
      recreate; `SchemaDiff.Table` reference inconsistencies are handled (NH-0401).
- [x] Temporal/provider DDL precision is correct at the SQL-generation level (MySQL
      DATETIME(6)/TIME(6), TINYINT UNSIGNED, temporal-default rewrite) — `MigrationSqlGeneration`
      green (NH-0401); live-DB round-trip re-verified on the live gate.
- [ ] Concurrent deploys are advisory-locked per provider; safe rename detection documented.
      (NH-0401: rename detection + `[RenameColumn]` doc contract green; per-provider live
      advisory-lock deferred to the live provider gate.)

## Current confidence

Strong. The preservation fuzzer found and closed the unrunnable-standalone dropped-NOT-NULL-
restore kill (Down now always recreates; type-zero backfill contract pinned). SQLite one-recreate
invariant and rename-fold are in place.

## Open items

- [~] Sustain the preservation fuzzer dry window; record schema/seed ranges. (NH-0401 recorded a
      220-test dry run; env-directed sweeps now feed `docs/v1-sharpening/fuzzer-dry-log.md` — set
      `NORM_MIGRATION_FUZZ_SWEEP="start:count"`; seeds 902000+ swept dry 2026-07-16. The sustained
      window accumulates in the log.)
- [ ] Re-verify advisory-lock behaviour on live SQL Server / PostgreSQL / MySQL under concurrent
      deploy.
- [x] Rename detection vs DROP+ADD boundary is documented and matches `[RenameColumn]` (NH-0401).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~MigrationDataPreservation"`
- `dotnet test tests/ --filter "FullyQualifiedName~MigrationRename|FullyQualifiedName~SchemaSnapshot"`
- Live migration phase of the release gate.

## Risks

`SchemaDiff.Table` refs are inconsistent by design (DroppedColumns = old, rest = new); any diff
consumer must respect that split.
