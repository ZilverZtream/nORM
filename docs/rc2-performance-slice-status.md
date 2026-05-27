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
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-223024` | SQLite, SQL Server | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched bulk | Passed threshold gate |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-221104` | PostgreSQL | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched bulk | Passed threshold gate |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-222018` | MySQL | Simple/runtime compiled, join/runtime compiled, complex/runtime compiled, count, single insert, idiomatic bulk, batched bulk | Passed threshold gate |

The earlier monolithic run is retained only as a failed-run diagnostic because
it mixed provider output and failed during the PostgreSQL phase. The RC gate now
uses provider-specific slices for the matrix so each provider has isolated logs
and CSV reports before the merged threshold check runs.

## Former Budget Risks

The original seven performance risks have been reduced to one policy-sensitive
watch item:

| Area | Latest nORM | Baseline | Ratio | Status |
| --- | ---: | ---: | ---: | --- |
| PostgreSQL complex compiled | `134.50 us` | `178.60 us` prepared Raw ADO | `0.75x` | Fixed: ahead |
| PostgreSQL complex runtime | `163.10 us` | `178.60 us` prepared Raw ADO | `0.91x` | Fixed: ahead |
| PostgreSQL join runtime | `149.00 us` | `136.00 us` prepared Raw ADO | `1.10x` | Fixed: inside budget |
| SQLite join compiled | `53.77 us` | `50.99 us` prepared Raw ADO | `1.05x` | Fixed: near parity |
| SQLite join runtime | `73.73 us` | `50.99 us` prepared Raw ADO | `1.45x` | Watch: public threshold passes against the runtime baseline set, but prepared Raw ADO remains faster |
| SQL Server single insert | `129.00 us` | `155.00 us` Raw ADO | `0.83x` | Fixed: ahead |
| PostgreSQL batched tx per-row | `7.645 ms` | `6.845 ms` EF SaveChanges in Tx | `1.12x` | Fixed: inside budget |

## Non-Benchmark Validation

- Full RC correctness gate with `-SkipBenchmark`: passed. The generated
  `artifacts/v1-rc/rc-artifacts.md` manifest records the exact gate commit and
  package hashes.
- Quick release gate with `-SkipBenchmark`: passed.
- Live provider gate with SQL Server, PostgreSQL, and MySQL configured:
  `1207/1207` live-provider tests passed.
- Direct Release full-suite run with live provider environment configured:
  `9729/9729` tests passed.
- Single-pass stress smoke for navigation, transaction, and compiled-query gate
  groups: `107/107` tests passed.
- Single-pass source-gen parity, bulk/provider parity, migration, cache, and
  adversarial gate groups: `1484/1484` tests passed.

## Public-Claim Interpretation

- Public query, count, single-insert, and idiomatic bulk budgets pass in the
  focused provider-sliced loop.
- `BulkInsert_Batched_*`, `BulkInsert_Naive_*`, and `Tx + per row` remain
  diagnostic paths unless explicitly named in a claim.
- The next public RC performance manifest should be produced by
  `eng/v1-release-gate.ps1 -Mode rc` without `-SkipBenchmark`, now that the
  gate runs the provider matrix as split provider slices.
