# Release Checklist

Use this checklist for every v1 release candidate and stable release.

## Before RC

- Public API review completed and `tests/PublicApi.Shipped.txt` updated only for
  intentional surface.
- Package consumer tests pass against produced `.nupkg` files.
- Source generator analyzer is present under `analyzers/dotnet/cs/`.
- README claims match docs and benchmark evidence.
- No preview SDK is used; `global.json` is honored locally and in CI.

## Validation

- `dotnet build nORM.sln -c Release --no-restore --nologo`
- `dotnet test tests/nORM.Tests.csproj -c Release --no-build --no-restore`
- `eng/v1-release-gate.ps1 -Mode full`
- Live SQL Server, PostgreSQL, MySQL, and SQLite provider gates.
- BenchmarkDotNet provider matrix with benchmark governance artifacts.
- `artifacts/v1-rc/rc-artifacts.md` reviewed and linked from the release notes
  or release issue.

## Packaging

- Runtime package metadata, README, license, repository URL, Source Link, symbols,
  and analyzer assets validated.
- `dotnet-norm` tool package smoke tested from the produced package.
- Release notes and changelog updated.

## Publishing

- Tag points at the validated commit.
- CI artifacts are retained for the release.
- RC artifact manifest commit matches the tag target.
- Known limitations and provider/version caveats are included in release notes.
- Security/support policy is current.
