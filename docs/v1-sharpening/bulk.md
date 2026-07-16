# Domain 3 — Bulk operations

**Scope:** provider-native and fallback bulk insert/update/delete, converter/type fidelity in
staging, and interaction with tenant filters and cache invalidation.

## 1.0 exit criteria

- [ ] The bulk-CUD oracle fuzzer runs **dry for a sustained window** with zero new kills.
      (NH-0301: dry on the current tree - 126 tests; the sustained multi-week window is open.)
- [ ] Native and fallback paths produce identical results on every provider; the chosen path is
      documented and threshold-driven. (NH-0301: batched/fallback parity green on SQLite; native
      paths on live SQL Server / PostgreSQL / MySQL deferred to the live provider gate.)
- [ ] Converter columns stage and round-trip by `Converter.ProviderType` with **no silent
      precision/format loss** (e.g. no DATETIME(0) sub-second truncation via UPDATE…JOIN).
      (Live-verified fix on MySQL + SQL Server; re-run on the live gate.)
- [x] Bulk paths apply `col.Converter` + `ParameterAssign.AssignValue` — never a raw bypass
      (NH-0301).
- [x] Bulk writes honour tenant boundaries and invalidate cache tags for every touched table
      (NH-0301: `BulkTenantIsolation` + `BulkCacheInvalidation` green).

## Current confidence

Strong. MySQL bulk-staging DATETIME(0) rounding was found live and fixed (MySQL + SQL Server
stage by provider type; migration DDL now DATETIME(6)/TIME(6)). SQLite bulk paths were fixed to
apply the converter (raw-Guid-as-TEXT corruption closed).

## Open items

- [~] Sustain the bulk oracle dry window; record seed ranges. (NH-0301 recorded a 126-test dry
      run; env-directed sweeps now feed `docs/v1-sharpening/fuzzer-dry-log.md` — set
      `NORM_BULK_FUZZ_SWEEP="start:count"`; seeds 802000+ swept dry 2026-07-16. The sustained
      window accumulates in the log.)
- [ ] Re-verify converter fidelity for every provider type after any converter/staging change.
- [x] Bulk-update/delete cache invalidation is covered (NH-0301: `BulkCacheInvalidation` green;
      correlated/multi-table cache-tag coverage cross-checked in Domain 8 / NH-0801).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~BulkCudOracleFuzzTests"`
- Live bulk parity via the release gate.
- `docs/bulk-operations.md` contract matches behaviour.

## Risks

`Microsoft.Data.Sqlite` stores raw `Guid` parameters as TEXT, not BLOB — bulk paths must bind
through the converter or corruption is silent.
