# Domain 6 — Temporal & versioning

**Scope:** nORM-managed history tables/triggers, `AsOf(tag)` reconstruction, sub-second version
precision, and provider-native vs nORM-managed temporal storage.

## 1.0 exit criteria

- [ ] The temporal reconstruction fuzzer runs **dry for a sustained window** with zero new kills.
- [ ] `AsOf` returns the correct version at sub-second boundaries on every provider; version
      timestamps have ms (or better) precision, not second granularity.
- [ ] `AsOf` re-binds correctly across cached plans (no stale plan returns the wrong version).
- [ ] Strict provider mobility requires nORM-managed temporal storage; provider-native storage is
      correctly gated as provider-bound.

## Current confidence

Strong. A precision bug was closed: SQLite triggers now use `strftime` `%f` (ms) — they were
`datetime('now')` at second precision, so sub-second versions collapsed and `AsOf` between them
returned the wrong version. `AsOf` cross-plan re-binding is covered.

## Open items

- [ ] Sustain the reconstruction fuzzer dry window.
- [ ] Verify sub-second precision on all four providers' history triggers/storage.
- [ ] Confirm temporal + tenant + soft-delete interactions are covered.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~TemporalHistoryReconstructionFuzzTests"`
- `dotnet test tests/ --filter "FullyQualifiedName~TemporalAsOfPlanCache|FullyQualifiedName~TemporalPrecision"`

## Risks

Temporal probes need gaps larger than clock precision, or they measure clock granularity instead
of correctness. `DateTimeOffset.LocalDateTime` uses snapshot semantics (offset baked at build time).
