# nORM v1.0 Readiness Checklist

This checklist defines the release-hardening work that must be complete before a
public v1.0 package is cut.

## Required Gates

- Public API snapshot passes without `NORM_UPDATE_PUBLIC_API`.
- Full test suite passes in `Release`.
- SQL Server, PostgreSQL, and MySQL live provider gates pass against real local
  or CI databases.
- Fast complex-query benchmark keeps runtime and compiled/prepared paths at or
  below the recorded time and allocation baselines. Provider-specific runtime
  fast paths may legitimately beat compiled/prepared paths and should not be
  treated as a failure by itself.
- Full provider benchmark matrix passes for SQLite, SQL Server, PostgreSQL, and
  MySQL, covering nORM, EF Core, Dapper, and Raw ADO.NET for query,
  prepared/compiled, join, count, insert, and bulk insert scenarios.
- `dotnet pack` succeeds for `nORM` and `dotnet-norm`, including symbols.

## Latest RC Evidence

- Command: `.\eng\v1-release-gate.ps1 -Mode rc -MinLiveProviders 3 -StressIterations 20`
- Completed: 2026-06-14 16:37:58 UTC.
- Commit: `890fb097d4e5a89daa5e0f4449e16667a9138bc5`.
- Working tree: clean; mode: `rc`; benchmark skipped: `False`.
- Build: Release build succeeded with 0 warnings.
- Public API snapshot: 2/2 passed.
- Package consumer smoke: 6/6 passed.
- CLI smoke: 89/89 passed.
- Live provider gate: 2,117/2,117 passed; second pass also 2,117/2,117 passed.
- Full suite: 11,931/11,931 passed; second pass also 11,931/11,931 passed.
- Provider/source-generator parity: 97/97 passed.
- Bulk/provider parity: 424/424 passed.
- Migration provider gate: 219/219 passed.
- Cache memory bounds: 16/16 passed.
- Concurrency/adversarial gate: 794/794 passed.
- Fast complex benchmark: `Query_Complex_NoResults` 6.277 us / 3.37 KB,
  `Query_Complex_Compiled` 14.750 us / 2.48 KB, `Query_Complex` 22.592 us /
  7.47 KB.
- Provider matrix: SQLite, SQL Server, PostgreSQL, and MySQL completed with raw
  BenchmarkDotNet reports and split CSVs under
  `BenchmarkDotNet.Artifacts/provider-slices/20260614-145753`.
- Benchmark threshold gate: passed; summary in
  `BenchmarkDotNet.Artifacts/v1-evidence/benchmark-thresholds.md`.
- Packages:
  - `src/bin/Release/nORM.1.0.0-rc.3.nupkg`
    SHA-256 `a6528d263dfdd039a236785b9a675bd56971255e962f1345100893f509eecb9c`
  - `src/bin/Release/nORM.1.0.0-rc.3.snupkg`
    SHA-256 `0841e23d2ef6a669d15cea5734239f2c0913d774f295c62f3222cabe25970d7a`
  - `src/dotnet-norm/bin/Release/dotnet-norm.1.0.0-rc.3.nupkg`
    SHA-256 `57475a40241860c614662e92401be5c83bebc5677b56f01f7765d706a65f94a1`
  - `src/dotnet-norm/bin/Release/dotnet-norm.1.0.0-rc.3.snupkg`
    SHA-256 `d3256f95a2ac4b45e02ede529f1339819673d8abd7c95de257a2fd596a1e230e`

## Recommended RC Command

Configure live providers:

```powershell
$env:NORM_TEST_SQLSERVER = 'Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False;Connect Timeout=10'
$env:NORM_TEST_POSTGRES = 'Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=<password>'
$env:NORM_TEST_MYSQL = 'Server=127.0.0.1;Port=3306;Database=normtest;User ID=root;Password=<password>'
```

Run the release-candidate gate:

```powershell
.\eng\v1-release-gate.ps1 -Mode rc -MinLiveProviders 3 -StressIterations 20
```

## API Freeze

Before tagging v1.0, review:

- `nORM.Core` context/query/transaction APIs.
- `nORM.Configuration` model-building APIs.
- `nORM.Providers` provider extension points.
- `nORM.Migration` runner and SQL generator APIs.
- `nORM.Navigation` lazy/explicit loading APIs.
- `nORM.Enterprise` interceptors, tenant provider, retry policy, and cache
  provider contracts.

Any public surface that is not intended for stable use should be made internal,
hidden behind a narrower abstraction, or documented as advanced/provider-facing.

## Release Notes Inputs

Capture these values from the final RC run:

- Commit SHA.
- Full suite pass count and duration.
- Live provider pass count and configured providers.
- Fast benchmark means and allocations for `Query_Complex` and
  `Query_Complex_Compiled`.
- Provider matrix benchmark summaries for SQLite, SQL Server, PostgreSQL, and
  MySQL.
- Package files produced in `src/bin/Release`.
