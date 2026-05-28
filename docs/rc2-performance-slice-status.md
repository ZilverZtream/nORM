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
| `BenchmarkDotNet.Artifacts/provider-slices/20260528-054728` | SQLite, SQL Server, PostgreSQL, MySQL | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched/diagnostic bulk rows | Passed final RC2 threshold gate on commit `4e162f0afb30526cc35b091ebe3c13d9af371271` |
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
| PostgreSQL complex compiled | `202.48 us` | `230.67 us` prepared Raw ADO | `0.878x` | Fixed: ahead |
| PostgreSQL complex runtime | `244.04 us` | `342.42 us` optimized Raw ADO | `0.713x` | Fixed: ahead |
| PostgreSQL join runtime | `165.40 us` | `306.67 us` optimized Raw ADO | `0.539x` | Fixed: ahead |
| SQLite join compiled | `49.09 us` | `53.91 us` Dapper | `0.911x` | Fixed: ahead of chosen threshold baseline |
| SQLite join runtime | `58.75 us` | `53.91 us` Dapper | `1.090x` | Fixed: inside `1.5x` runtime threshold |
| SQL Server single insert | `133.80 us` | `154.29 us` Raw ADO | `0.867x` | Fixed: ahead |
| PostgreSQL batched tx per-row | tracked as diagnostic | not a public threshold baseline | n/a | Diagnostic path; no public threshold violation |

## Non-Benchmark Validation

- Full RC2 gate with `-Mode rc -MinLiveProviders 3 -StressIterations 20`:
  passed on commit `4e162f0afb30526cc35b091ebe3c13d9af371271` with benchmarks
  enabled. The generated `artifacts/v1-rc/rc-artifacts.md` manifest records the
  exact gate commit, provider configuration, package hashes, and benchmark
  evidence.
- Quick release gate with `-SkipBenchmark`: passed.
- Live provider gate with SQL Server, PostgreSQL, and MySQL configured:
  `1356/1356` direct live-provider tests passed after the SQL Server compiled
  string parameter reuse fix; the RC gate live-provider filter passed
  `1347/1347` in both required passes.
- Direct Release full-suite run with live provider environment configured:
  `9873/9873` tests passed in both required RC gate full-suite passes.
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
- The public RC2 performance manifest has been produced by
  `eng/v1-release-gate.ps1 -Mode rc` without `-SkipBenchmark`.
- A local full markdown table for the final slice is written to
  `.tmp/full-provider-benchmark-table-20260528-054728.md`.
