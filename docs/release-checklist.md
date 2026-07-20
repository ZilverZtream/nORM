# Release Checklist

Use this checklist when cutting a genuine release candidate or stable release.
nORM is currently pre-1.0 (0.x): release candidates are cut only when the bar in
`RELEASE.md` is met, never on a cadence.

## Version Transitions

`Directory.Build.props` carries `NormVersion`, the single source of truth for every package and
the changelog. Every transition below requires updating `NormVersion` in `Directory.Build.props`
**and** appending a matching section to `CHANGELOG.md` (one PR, same commit). The release gate's
`Assert-CurrentPackageOutput` step then refuses any stale `TheNorm.*.nupkg` / `dotnet-norm.*.nupkg`
that does not match `NormVersion` byte-for-byte.

| Phase | `NormVersion` example | When |
| --- | --- | --- |
| Release candidate | `1.0.0-rc.1`, `1.0.0-rc.2`, ... | Hardening; published to NuGet pre-release feed only |
| Stable v1 | `1.0.0` | RC manifest signed off, live providers green, benchmark thresholds green |
| Post-v1 development | `1.1.0-dev.1` | Immediately after tagging `v1.0.0` to avoid accidental reuse |
| Stable v1.x | `1.1.0`, `1.2.0`, ... | New feature work that preserves the v1 public API contract |
| Patch | `1.0.1`, `1.0.2`, ... | Bug fixes against v1.0 without API additions |

Rules:

- Never reuse a `NormVersion` value. A bad RC becomes `1.0.0-rc.2`, not a re-pack of `rc.1`.
- After `git tag v1.0.0`, bump `NormVersion` to the next development value in the same merge so
  `main` never reads back as a published version.
- `CHANGELOG.md` must contain a heading whose version matches `NormVersion` for any commit that
  produces a release artifact. The `Unreleased` section becomes the named release section at tag
  time.
- `ChangelogVersionContractTests` asserts the runtime `NormVersion` is referenced in the
  changelog so docs and binaries cannot drift apart.

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

See [publishing.md](publishing.md) for the step-by-step NuGet push runbook (used
for both `0.9.x` validation previews and the eventual stable release).

- Tag points at the validated commit.
- CI artifacts are retained for the release.
- RC artifact manifest commit matches the tag target.
- Known limitations and provider/version caveats are included in release notes.
- Security/support policy is current.
