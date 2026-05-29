# nORM Performance Benchmarks

This project compares nORM against Entity Framework Core, Dapper, and Raw
ADO.NET for the operations that matter for v1.0 release validation.

## Running From A Repository With Agent Worktrees

> **Important.** If this repository contains agent worktrees under
> `.claude/worktrees/`, each one carries its own copy of
> `nORM.Benchmarks.csproj`. BenchmarkDotNet's project discovery then becomes
> ambiguous and the run fails before measuring anything. In that situation run
> benchmarks through `eng/run-benchmark-isolated.ps1`, which transparently runs
> the benchmark in a clean detached `git worktree` under the temp directory and
> copies the reports back. Do **not** delete the user's worktrees to work around
> this. The plain `dotnet run` commands below are correct only in a clean clone
> with no extra `nORM.Benchmarks.csproj` files under the repository root.

```powershell
# Isolated equivalents of the commands in this README:
eng/run-benchmark-isolated.ps1 -- --fast Query_Complex
eng/run-benchmark-isolated.ps1 -- --provider-matrix --provider Sqlite --filter "*ProviderMatrixBenchmarks.Insert_Single*"
eng/run-benchmark-isolated.ps1 -- --filter "*TenantTemporalBenchmarks*"
```

See `docs/benchmark-governance.md` for the full policy.

## Benchmark Suites

### SQLite Full Comparison

```powershell
dotnet run -c Release
```

Runs the original SQLite comparison suite with memory allocation tracking,
statistical analysis, and BenchmarkDotNet result exports.

### Fast nORM-Only Checks

```powershell
dotnet run -c Release -- --fast Query_Complex
```

Runs focused nORM benchmarks for quick regression checks. This is useful while
iterating on query translation, compiled queries, materialization, and bulk
paths.

### Provider Matrix

```powershell
$env:NORM_TEST_SQLSERVER = 'Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False;Connect Timeout=10'
$env:NORM_TEST_POSTGRES = 'Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=<password>'
$env:NORM_TEST_MYSQL = 'Server=127.0.0.1;Port=3306;Database=normtest;User ID=root;Password=<password>'
dotnet run -c Release -- --provider-matrix
```

The provider matrix runs the same schema, seed data, and benchmark scenarios on
SQLite, SQL Server, PostgreSQL, and MySQL. Each provider compares:

- nORM
- Entity Framework Core
- Dapper
- Raw ADO.NET convenience mapping
- Raw ADO.NET optimized mapping
- Raw ADO.NET typed/no-box mapping

Covered scenarios:

- Single insert
- Simple query
- Complex query with filters, ordering, skip, and take
- Compiled/prepared simple and complex query paths. Prepared Raw ADO baselines
  are labeled `PreparedOptimized`; Dapper benchmarks are only labeled Dapper
  when Dapper performs result materialization.
- Join query
- Count query
- 1k/10k row materialization scaling
- Parallel read throughput under pooled live-provider connections
- Naive, batched, prepared, and idiomatic bulk insert variants

Use `--provider` plus BenchmarkDotNet filters for focused provider runs. This
keeps result tables provider-specific and avoids noisy four-provider summaries
while investigating regressions:

```powershell
dotnet run -c Release -- --provider-matrix --provider Postgres --filter "*ProviderMatrixBenchmarks.Query_Complex*"
dotnet run -c Release -- --provider-matrix --provider Postgres --filter "*ProviderMatrixBenchmarks.Query_Join*"
dotnet run -c Release -- --provider-matrix --provider Sqlite --filter "*ProviderMatrixBenchmarks.Query_Join*"
dotnet run -c Release -- --provider-matrix --provider SqlServer --filter "*ProviderMatrixBenchmarks.Insert_Single*"
dotnet run -c Release -- --provider-matrix --provider Postgres --filter "*ProviderMatrixBenchmarks.BulkInsert_Batched*"
```

For repeatable focused evidence, prefer the checked-in slice runner. It runs one
provider/filter pair at a time, preserves each provider's logs and raw reports,
merges the CSV reports into a single evidence folder, and can run the threshold
checker with missing unrelated rules allowed:

```powershell
eng/run-provider-benchmark-slice.ps1 `
  -Providers "Postgres,Sqlite" `
  -Filters "*ProviderMatrixBenchmarks.Query_Join*" `
  -CheckThresholds
```

## Test Data

- 12,000 users in the provider matrix; 1,000 users in the legacy SQLite-only
  comparison
- 2,000 orders
- Provider-specific DDL with equivalent columns and indexes
- Deterministic seed values for stable cross-provider comparisons

## Reading Results

Key columns:

- `Mean`: average execution time; lower is better.
- `Allocated`: managed allocation per operation; lower means less GC pressure.
- `Rank`: BenchmarkDotNet relative rank within the displayed result set.

For v1.0, nORM should remain competitive with Dapper and with the optimized Raw
ADO.NET categories on hot query paths, keep both runtime and compiled/prepared
query paths within their recorded time and allocation baselines, and allocate
substantially less than EF Core on comparable no-tracking query paths.
Provider-specific runtime fast paths may beat compiled/prepared paths; that is
acceptable when the full matrix stays healthy. Public claims must name the exact
category being compared, for example `RawAdo_Convenience`,
`RawAdo_Optimized`, `RawAdo_TypedNoBox`, `RawAdo_PreparedOptimized`, or
`RawAdo_PreparedTypedNoBox`, and must come from generated BenchmarkDotNet
reports rather than hand-copied numbers. SQLite runs apply identical
`journal_mode = WAL`, `synchronous = NORMAL`, and `busy_timeout` settings to
EF Core, nORM, Dapper, and Raw ADO connections so single-insert rows measure
data-layer overhead rather than mismatched durability settings.

BenchmarkDotNet writes reports to `BenchmarkDotNet.Artifacts/results/`.
