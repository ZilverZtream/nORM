# Domain 3 — Bulk operations

**Scope:** provider-native and fallback bulk insert/update/delete, converter/type fidelity in
staging, and interaction with tenant filters and cache invalidation.

## 1.0 exit criteria

- [ ] The bulk-CUD oracle fuzzer runs **dry for a sustained window** with zero new kills.
- [ ] Native and fallback paths produce identical results on every provider; the chosen path is
      documented and threshold-driven.
- [ ] Converter columns stage and round-trip by `Converter.ProviderType` with **no silent
      precision/format loss** (e.g. no DATETIME(0) sub-second truncation via UPDATE…JOIN).
- [ ] Bulk paths apply `col.Converter` + `ParameterAssign.AssignValue` — never a raw bypass.
- [ ] Bulk writes honour tenant boundaries and invalidate cache tags for every touched table.

## Current confidence

Strong. MySQL bulk-staging DATETIME(0) rounding was found live and fixed (MySQL + SQL Server
stage by provider type; migration DDL now DATETIME(6)/TIME(6)). SQLite bulk paths were fixed to
apply the converter (raw-Guid-as-TEXT corruption closed).

## Open items

- [ ] Sustain the bulk oracle dry window; record seed ranges.
- [ ] Re-verify converter fidelity for every provider type after any converter/staging change.
- [ ] Confirm bulk-update/delete cache invalidation covers correlated/multi-table reads
      (cross-check Domain 8).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~BulkCudOracleFuzzTests"`
- Live bulk parity via the release gate.
- `docs/bulk-operations.md` contract matches behaviour.

## Risks

`Microsoft.Data.Sqlite` stores raw `Guid` parameters as TEXT, not BLOB — bulk paths must bind
through the converter or corruption is silent.
