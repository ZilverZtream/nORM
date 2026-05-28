# RC2 Performance Slice Status

This file records focused provider-sliced benchmark evidence gathered while
hardening the v1 performance claims. It is not a replacement for the full RC
release gate; it is the fast loop used to isolate and verify public benchmark
budgets before spending hours on the full gate.

## Current Evidence

The latest RC2 benchmark loop ran the public provider matrix as provider chunks
instead of one noisy monolithic run:

| Slice | Providers | Public budget coverage | Result |
| --- | --- | --- | --- |
| `BenchmarkDotNet.Artifacts/provider-slices/20260528-015921` | SQLite, SQL Server, PostgreSQL, MySQL | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched/diagnostic bulk rows | Passed threshold gate on commit `a2fd29b221f84644bd93513436ea629c56619338` |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-223024` | SQLite, SQL Server | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched bulk | Passed threshold gate |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-221104` | PostgreSQL | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched bulk | Passed threshold gate |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-222018` | MySQL | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched bulk | Passed threshold gate |
| `BenchmarkDotNet.Artifacts/provider-slices/20260528-001020` | SQL Server | Simple/runtime compiled and complex/runtime compiled after the compiled string-parameter reuse fix | Passed threshold gate |

The earlier monolithic run is retained only as a failed-run diagnostic because
it mixed provider output and failed during the PostgreSQL phase. The RC gate now
uses provider-specific slices for the matrix so each provider has isolated logs
and CSV reports before the merged threshold check runs.

## Former Budget Risks

The original seven performance risks are now inside the executable RC2
threshold budgets:

| Area | Latest nORM | Baseline | Ratio | Status |
| --- | ---: | ---: | ---: | --- |
| PostgreSQL complex compiled | `203.16 us` | `235.12 us` prepared Raw ADO | `0.864x` | Fixed: ahead |
| PostgreSQL complex runtime | `242.31 us` | `346.43 us` optimized Raw ADO | `0.699x` | Fixed: ahead |
| PostgreSQL join runtime | `163.28 us` | `312.15 us` optimized Raw ADO | `0.523x` | Fixed: ahead |
| SQLite join compiled | `49.52 us` | `52.24 us` Dapper | `0.948x` | Fixed: ahead of chosen threshold baseline |
| SQLite join runtime | `60.49 us` | `52.24 us` Dapper | `1.158x` | Fixed: inside `1.5x` runtime threshold |
| SQL Server single insert | `126.99 us` | `158.99 us` Dapper | `0.799x` | Fixed: ahead |
| PostgreSQL batched tx per-row | `7.656 ms` | `6.653 ms` EF SaveChanges in Tx | `1.151x` | Diagnostic path; no public threshold violation |

## Non-Benchmark Validation

- Full RC correctness gate with `-Mode rc -MinLiveProviders 3 -StressIterations 20 -SkipBenchmark`:
  passed on commit `0bdf8ecdeaa09a66d1e08371ced10b3405ed59a3`. The generated
  `artifacts/v1-rc/rc-artifacts.md` manifest records the exact gate commit,
  provider configuration, package hashes, and benchmark-skipped caveat.
- Quick release gate with `-SkipBenchmark`: passed.
- Live provider gate with SQL Server, PostgreSQL, and MySQL configured:
  `1356/1356` direct live-provider tests passed after the SQL Server compiled
  string parameter reuse fix; the RC gate live-provider filter passed
  `1347/1347` in both required passes.
- Direct Release full-suite run with live provider environment configured:
  `9872/9872` tests passed; the RC gate full-suite step also passed
  `9872/9872` in both required passes.
- RC stress gates passed with 20 iterations each: navigation `26/26`,
  transaction `55/55`, and compiled-query `26/26` on every iteration.
- RC focused gates passed: source-gen parity `97/97`, bulk/provider parity
  `424/424`, migration `215/215`, cache bounds `16/16`, and adversarial
  `767/767`.

## Public-Claim Interpretation

- Public query, count, single-insert, and idiomatic bulk budgets pass in the
  current provider-sliced loop.
- `BulkInsert_Batched_*`, `BulkInsert_Naive_*`, and `Tx + per row` remain
  diagnostic paths unless explicitly named in a claim.
- The next public RC performance manifest should be produced by
  `eng/v1-release-gate.ps1 -Mode rc` without `-SkipBenchmark`, now that the
  gate runs the provider matrix as split provider slices.
- A local full markdown table for the current slice is written to
  `.tmp/full-provider-benchmark-table-20260528-015921.md`.
