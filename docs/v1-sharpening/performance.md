# Domain 13 — Performance & benchmarks

**Scope:** threshold-gated benchmark governance, competitive hot-path performance vs EF Core /
Dapper / raw ADO.NET, and regression protection.

## 1.0 exit criteria

- [ ] The benchmark governance thresholds pass on a fresh, isolated run for every method in the
      provider matrix; the fastest method per provider remains a nORM path.
- [ ] Benchmarks are re-baselined close to the RC on representative hardware; the baseline is
      committed and the threshold rules take the fastest row per method (guards live-server
      variance).
- [ ] No hot-path regression vs the committed baseline; any regression is explained and accepted
      or fixed.
- [ ] Benchmark methodology (`docs/benchmark-governance.md`) is explicit about baseline rules and
      fairness locks.

## Current confidence

Good. As of the last baseline, all threshold rules pass and nORM is faster than the baselines on
9/10 methods. The insert benchmark is thermally sensitive — rerun before diagnosing a regression.

## Open items

- [ ] Re-baseline near RC on stable hardware; delete `benchmarks/bin` first (SQLITE_FULL risk).
- [ ] Confirm the threshold script still takes the fastest row per method.
- [ ] Add coverage for any hot path changed by DI/Set<T>/future work.

## Verification

- `eng/run-provider-benchmark-slice.ps1` / the release gate benchmark phase + `eng/check-benchmark-thresholds.ps1`.
- `docs/benchmark-governance.md`, `docs/benchmark-baseline-*`.

## Risks

Benchmarks are thermally and environment sensitive; a single noisy run is not a regression. Always
rerun isolated before acting. Performance is a 1.0 quality bar but never gates correctness.
