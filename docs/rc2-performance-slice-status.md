# RC2 Performance Slice Status

This file records the focused provider-sliced benchmark evidence gathered while
hardening the v1 performance claims. It is not a replacement for the full RC
release gate; it is the fast loop used to isolate and verify public benchmark
budgets before spending hours on the full gate.

All slices below were run from isolated git worktrees because local agent
worktrees under `.claude/` make BenchmarkDotNet discover duplicate benchmark
project names when it runs directly from the main checkout.

| Slice | Providers | Public budget coverage | Result |
| --- | --- | --- | --- |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-143000` | SQLite, SQL Server, PostgreSQL, MySQL | Simple runtime and compiled queries | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-141847` | SQLite, PostgreSQL | Complex runtime and compiled queries | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-144133` | SQL Server, MySQL | Complex runtime/compiled queries and runtime/compiled joins | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-141512` | SQLite, PostgreSQL | Runtime and compiled joins | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-143655` | SQLite, SQL Server, PostgreSQL, MySQL | Count | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-142235` | SQLite, SQL Server, PostgreSQL, MySQL | Single insert | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-142633` | SQLite, SQL Server, PostgreSQL, MySQL | Idiomatic `BulkInsertAsync` | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-154725` | SQLite, PostgreSQL | Simple runtime and compiled queries after pooled-plan guard fix | Passed |
| `BenchmarkDotNet.Artifacts/provider-slices/20260527-164414` | MySQL | Complex runtime/compiled queries and runtime/compiled joins after pooled-plan guard fix | Passed |

Notable tight-but-passing ratios:

- PostgreSQL simple compiled: `1.201/1.35` vs prepared optimized Raw ADO.
- SQL Server single insert: `1.078/1.25` vs Raw ADO, with lower allocation.
- MySQL runtime join: `1.379/1.5` vs optimized Raw ADO.
- SQL Server runtime join: `1.12/1.5` vs optimized Raw ADO.
- MySQL complex compiled: `1.005/1.4` vs prepared optimized Raw ADO.
- After the pooled-plan guard fix, MySQL runtime join improved to `1.038/1.5`
  and MySQL complex compiled remained inside budget at `1.027/1.4`.

Current non-benchmark validation for the RC2 candidate:

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

Public-claim interpretation:

- The public query, count, single-insert, and idiomatic bulk budgets pass in the
  focused provider-sliced loop.
- `BulkInsert_Batched_*`, `BulkInsert_Naive_*`, and `Tx + per row` remain
  diagnostic paths unless explicitly named in a claim.
- The final RC package still needs the full release gate without
  `-SkipBenchmark` so the package manifest and raw BenchmarkDotNet artifacts are
  produced from one release commit.
