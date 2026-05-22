# nORM Performance Benchmarks

This project compares nORM against Entity Framework Core, Dapper, and Raw
ADO.NET for the operations that matter for v1.0 release validation.

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

Covered scenarios:

- Single insert
- Simple query
- Complex query with filters, ordering, skip, and take
- Compiled/prepared simple and complex query paths. Prepared Raw ADO baselines
  are labeled `PreparedOptimized`; Dapper benchmarks are only labeled Dapper
  when Dapper performs result materialization.
- Join query
- Count query
- Naive, batched, prepared, and idiomatic bulk insert variants

Use BenchmarkDotNet filters for focused provider runs:

```powershell
dotnet run -c Release -- --provider-matrix --filter "*ProviderMatrixBenchmarks.Query_Complex*"
dotnet run -c Release -- --provider-matrix --filter "*ProviderMatrixBenchmarks.BulkInsert*"
```

## Test Data

- 1,000 users
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
`RawAdo_Optimized`, or `RawAdo_PreparedOptimized`, and must come from generated
BenchmarkDotNet reports rather than hand-copied numbers.

BenchmarkDotNet writes reports to `BenchmarkDotNet.Artifacts/results/`.
