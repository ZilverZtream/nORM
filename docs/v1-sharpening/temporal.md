# Domain 6 — Temporal & versioning

**Scope:** nORM-managed history tables/triggers, `AsOf(tag)` reconstruction, sub-second version
precision, and provider-native vs nORM-managed temporal storage.

## 1.0 exit criteria

- [ ] The temporal reconstruction fuzzer runs **dry for a sustained window** with zero new kills.
      (NH-0601: dry on the current tree - 50 tests; the sustained multi-week window is open.)
- [ ] `AsOf` returns the correct version at sub-second boundaries on every provider; version
      timestamps have ms (or better) precision, not second granularity. (NH-0601: SQLite ms
      precision green - `TemporalPrecision`; all-four-provider sub-second deferred to the live gate.)
- [x] `AsOf` re-binds correctly across cached plans (no stale plan returns the wrong version)
      (NH-0601: `TemporalAsOfPlanCache` green).
- [x] Strict provider mobility requires nORM-managed temporal storage; provider-native storage is
      correctly gated as provider-bound (NH-0601: `ProviderNativeTemporal` green).

## Current confidence

Strong. A precision bug was closed: SQLite triggers now use `strftime` `%f` (ms) — they were
`datetime('now')` at second precision, so sub-second versions collapsed and `AsOf` between them
returned the wrong version. `AsOf` cross-plan re-binding is covered.

## Open items

- [x] **KILL 44 (found and FIXED 2026-07-16): `Include` under `AsOf` silently mixed eras.**
      The root query reconstructed history while every eager-loaded relation read the LIVE
      tables (probe: AsOf(t1) truth `[1:10]`, returned `[1:99, 2:20]`). FIX: the query plan
      now carries the AsOf timestamp (safe to bake — `AsOf()` embeds the timestamp as an
      expression constant, so each timestamp gets its own cached plan), and the eager-load
      pipeline reads every include level through the SAME history window the root FROM uses
      (emulated derived table or the provider-native clause), including the nested EXISTS
      levels of multi-hop includes. Many-to-many + AsOf now FAILS LOUD at translation — the
      association table is raw and unversioned, so era membership is unknowable (versioning
      association tables is a future product decision). Pinned by
      `IncludeAsOfConsistencyContractTests` + `ManyToManyTemporalContractTests`;
      `TemporalAsOfPlanCacheTests` still green.

- [~] Sustain the reconstruction fuzzer dry window. (NH-0601 recorded a 50-test dry run;
      env-directed sweeps now feed `docs/v1-sharpening/fuzzer-dry-log.md` via
      `NORM_TEMPORAL_FUZZ_SWEEP="start:count"` — seeds 702000-702079 swept dry 2026-07-16.)
- [x] Verify sub-second precision on all four providers' history triggers/storage. (Closed
      2026-07-16 by the differential evidence already running live: the reconstruction machine
      writes versions ~50-100ms apart and takes SERVER-CLOCK checkpoints between them — a
      second-precision trigger or storage column would collapse adjacent versions and fail the
      checkpoint oracle. `LiveProviderTemporal*` + `TemporalMigrationLiveBehaviourTests` are
      26/26 non-vacuous on live SQL Server/PostgreSQL/MySQL (re-verified today), SQLite is
      covered by the millisecond-trigger fix and `TemporalHistoryReconstructionFuzzTests`, and
      the generators' fractional-precision DDL (MySQL DATETIME(6)/TIME(6), temporal-default(6))
      is pinned at the SQL-text level.)
- [x] Confirm temporal + tenant + soft-delete interactions are covered. (NH-0601: temporal+tenant
      green - `TemporalTriggerTenantScope`, `TenantTemporalProviderSwap`. Soft-delete closed
      2026-07-16 by differential probe, CORRECT with zero code changes: a global filter composes
      with AsOf over the RECONSTRUCTED era state — visible while live with era values, hidden
      after the soft delete, current view hidden, history chain complete (the soft delete is an
      ordinary versioned update). Pinned by `SoftDeleteTemporalInteractionContractTests`.)
- [x] Temporal-aware migrations (NH-0612): migrations on trigger-emulated temporal tables now
      mirror the history schema in lock-step and re-emit the versioning triggers from the
      post-change schema on all four generators - a SQLite recreate previously KILLED versioning
      silently, and ADD COLUMN never reached history on any provider. SQLite closed behaviourally
      (`TemporalMigrationContractTests`); servers closed at the SQL-text level
      (`ServerTemporalMigrationDdlContractTests`) AND behaviourally on the live servers
      (`TemporalMigrationLiveBehaviourTests` against the normtest application database — seed
      versions, generator ADD COLUMN migration, post-migration write reaches history, AsOf spans
      the migration; re-verified non-vacuous 2026-07-16).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~TemporalHistoryReconstructionFuzzTests"`
- `dotnet test tests/ --filter "FullyQualifiedName~TemporalAsOfPlanCache|FullyQualifiedName~TemporalPrecision"`

## Risks

Temporal probes need gaps larger than clock precision, or they measure clock granularity instead
of correctness. `DateTimeOffset.LocalDateTime` uses snapshot semantics (offset baked at build time).
