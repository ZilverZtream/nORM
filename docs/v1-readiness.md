# nORM v1.0 Readiness Checklist

This checklist defines the release-hardening work that must be complete before a
public v1.0 package is cut.

## Required Gates

- Public API snapshot passes without `NORM_UPDATE_PUBLIC_API`.
- Full test suite passes in `Release`.
- SQL Server and PostgreSQL live provider gates pass against real local or CI
  databases.
- MySQL support is either live-gated or explicitly documented as preview.
- Fast complex-query benchmark keeps compiled query materially ahead of runtime
  query and below the recorded allocation baseline.
- Full provider benchmark matrix passes for SQLite, SQL Server, and PostgreSQL,
  covering nORM, EF Core, Dapper, and Raw ADO.NET for query, prepared/compiled,
  join, count, insert, and bulk insert scenarios.
- `dotnet pack` succeeds for `nORM` and `dotnet-norm`, including symbols.

## Recommended RC Command

Configure live providers:

```powershell
$env:NORM_TEST_SQLSERVER = 'Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False;Connect Timeout=10'
$env:NORM_TEST_POSTGRES = 'Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=<password>'
```

Run the release-candidate gate:

```powershell
.\eng\v1-release-gate.ps1 -Mode rc -MinLiveProviders 2 -StressIterations 20
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
- Provider matrix benchmark summaries for SQLite, SQL Server, and PostgreSQL.
- Package files produced in `src/bin/Release`.
