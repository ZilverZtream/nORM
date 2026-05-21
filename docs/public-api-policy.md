# Public API Policy

nORM is approaching a v1.0 API freeze. Public API changes must be intentional,
reviewed, and reflected in the shipped API baseline.

## Baseline

The public API baseline is stored in:

```text
tests/PublicApi.Shipped.txt
```

`PublicApiSnapshotTests.Public_api_matches_v1_baseline` compares the exported
surface of `nORM.dll` against that file. A normal test run fails when public
types, members, fields, events, constructors, or method signatures change.

## Updating The Baseline

Only update the baseline for a reviewed public API change:

```powershell
$env:NORM_UPDATE_PUBLIC_API = '1'
dotnet test tests\nORM.Tests.csproj -c Release --filter "FullyQualifiedName~PublicApiSnapshotTests"
Remove-Item Env:\NORM_UPDATE_PUBLIC_API
```

The API diff must be reviewed as part of the pull request or release branch
change. Accidental public API exposure should be fixed in code instead of added
to the baseline.

## v1.0 Rules

- Breaking changes require an explicit v1.0 readiness decision before the final
  release branch.
- New public members need XML documentation and tests that exercise the supported
  behavior.
- Experimental surface should be internal or clearly documented before v1.0.
- Provider-specific behavior must be documented when it differs between SQLite,
  SQL Server, PostgreSQL, and MySQL.
