# Publishing a preview to NuGet

This runbook covers publishing a **pre-1.0 `0.9.x` preview** of nORM to NuGet so
non-synthetic workloads and external users can exercise it against real databases.
That external validation is the near-term gate in [`RELEASE.md`](../RELEASE.md)
(criterion 3) and must happen **before** the public API is frozen and a
`1.0.0-rc.1` is cut - real-world feedback can still shape the surface while the
version is `0.x`.

> **Who pushes:** the `dotnet nuget push` step is a human action requiring a NuGet
> API key. It is deliberately outside any automated gate. Never commit, paste, or
> echo the API key.

## What ships

| Package | Id | Kind |
| --- | --- | --- |
| Runtime library | `TheNorm` | class library (the API namespace is `nORM`; the id is `TheNorm` because `nORM` is taken) |
| CLI tool | `dotnet-norm` | .NET global tool, command `norm` |

Both carry MIT license metadata, README, XML docs, Source Link, and `.snupkg`
symbols; the runtime package also ships the source generator under
`analyzers/dotnet/cs/`. Provider drivers (`Npgsql`, `MySqlConnector`) are
dependencies of the **tool**, not the runtime package - a runtime consumer pulls
in only `Microsoft.Data.SqlClient` and `Microsoft.Data.Sqlite`.

## 1. Confirm the version

`NormVersion` in [`Directory.Build.props`](../Directory.Build.props) is the single
source of truth for both packages and the changelog. It is currently `0.9.0`.

- `0.9.x` is a pre-1.0 `0.x` release: **the public API is not frozen** and may
  change before `1.0.0`. That is intentional for a validation preview.
- Never reuse a published version. If a preview needs a re-pack, bump the patch
  (`0.9.1`) or use an explicit prerelease label (`0.9.1-preview.1`); update
  `CHANGELOG.md` in the same commit (see [release-checklist](release-checklist.md)).

## 2. Build and validate the packages

Run the release gate, which restores, builds, tests, packs both packages, and
asserts the produced `.nupkg`/`.snupkg` match `NormVersion` exactly:

```powershell
pwsh eng/v1-release-gate.ps1 -Mode full
```

For a fast local check of just the package outputs (no full gate), the
package-consumer suite packs the runtime project and verifies a real consumer can
restore it, the analyzer ships, and the metadata/symbols are correct:

```powershell
dotnet test tests/nORM.Tests.csproj -c Release --filter "Category=PackageConsumer"
```

The packages land at:

```
src/bin/Release/TheNorm.<version>.nupkg            (+ .snupkg)
src/dotnet-norm/bin/Release/dotnet-norm.<version>.nupkg   (+ .snupkg)
```

> Re-pack from current source before pushing: the README is embedded in the
> runtime package, so a stale build can ship outdated docs.

## 3. Push (human action, requires an API key)

```powershell
$key = $env:NUGET_API_KEY   # set out-of-band; do not hard-code
dotnet nuget push src/bin/Release/TheNorm.<version>.nupkg `
    --api-key $key --source https://api.nuget.org/v3/index.json
dotnet nuget push src/dotnet-norm/bin/Release/dotnet-norm.<version>.nupkg `
    --api-key $key --source https://api.nuget.org/v3/index.json
```

`nuget.org` ingests the matching `.snupkg` automatically when it sits next to the
`.nupkg`. Push the runtime package first so the tool's dependency resolves.

## 4. Smoke-test the published packages

```powershell
dotnet tool install -g dotnet-norm --version <version>
norm --help
```

```powershell
dotnet add package TheNorm --version <version>
# build a throwaway consumer; confirm restore + the analyzer runs
```

## 5. Gather validation feedback

The point of the preview is [`RELEASE.md`](../RELEASE.md) criterion 3. Collect:

- Real workloads on real databases across SQLite, SQL Server, PostgreSQL, and MySQL.
- Any **correctness** surprise - silent-wrong results or data loss are release
  blockers and outrank everything else.
- API ergonomics friction, since the surface can still change while `0.x`.

Track findings as issues. When the bar in `RELEASE.md` is met (a sustained
zero-correctness-kill window **and** clean external validation), freeze the public
API (flip the "may still change" language in
[public-api-policy.md](public-api-policy.md)) and cut `1.0.0-rc.1` per
[release-checklist.md](release-checklist.md).

## Rollback

NuGet does not allow hard deletes. `dotnet nuget delete <Id> <version>` only
**unlists** a version - it disappears from search but existing references can still
restore it. So choose each preview version deliberately; prefer publishing a new
patch/prerelease over trying to retract one.
