# Benchmark Governance

Performance claims for v1 must be backed by reproducible benchmark artifacts
from the release commit. The benchmark harness is allowed to compare several
styles, but public claims must name the style being compared.

## Required Baselines

Read benchmarks that compare nORM to handwritten ADO.NET must distinguish:

- `RawAdo_Convenience`: straightforward ADO.NET with name lookups or conversion
  helpers. Useful as a realistic manual baseline, not as the fastest possible
  manual code.
- `RawAdo_Optimized`: fixed ordinals, typed getters, and the same SQL shape as
  the ORM/Dapper path. Portable conversion helpers are allowed only when the
  provider stores the value in a different physical shape.
- `RawAdo_TypedNoBox`: fixed ordinals and provider typed getters for every
  projected column. This is the handwritten allocation floor; public allocation
  claims must be checked against this row when it exists.
- `RawAdo_PreparedOptimized`: prepared command reuse plus the optimized reader.
- `RawAdo_PreparedTypedNoBox`: prepared command reuse plus the typed/no-box
  reader floor.
- `Dapper`: Dapper SQL execution with Dapper materialization.
- `Dapper_Prepared`: only permitted when Dapper still performs materialization.
  A prepared `DbCommand` that is read manually is a Raw ADO benchmark, not a
  Dapper benchmark.
- `nORM`: normal LINQ translation and materialization.
- `nORM_Compiled`: nORM compiled query path.

## Claim Rules

- Do not publish "beats raw ADO" unless nORM beats `RawAdo_Optimized` or
  `RawAdo_PreparedOptimized` for the same provider, SQL shape, row count, and
  projection.
- If nORM only beats `RawAdo_Convenience`, say so explicitly.
- Dapper comparisons must use typed materialization for typed nORM projections.
- Dynamic Dapper rows and typed nORM projections are not equivalent unless the
  claim is explicitly about dynamic result shapes.
- Row counts and first-row projection parity must be verified when adding or
  changing a benchmark.
- Large-read claims must include the scale rows (`Query_Scale1k_*` and
  `Query_Scale10k_*`) so materializer allocation behavior is visible beyond
  10-row latency checks.
- Throughput claims must include the `Query_ParallelThroughput_*` rows so
  allocation differences are tested under concurrent GC pressure.
- SQLite comparisons must apply the same per-connection durability PRAGMAs to
  EF Core, nORM, Dapper, and Raw ADO connections. `journal_mode = WAL` is
  file-persistent, but `synchronous = NORMAL` is connection-local; a benchmark
  that leaves one connection at SQLite's default `FULL` measures fsync policy,
  not ORM overhead. Until this is equalized, SQLite single-insert rows must not
  be published.
- Bulk-insert claims must use the `BulkInsert_Idiomatic_*` rows unless the
  claim explicitly says it is measuring a low-level diagnostic path. The
  `BulkInsert_Naive_*`, `BulkInsert_Batched_*`, and `Tx + per row` rows exist
  to diagnose fallback behavior and command overhead; they are not the public
  `BulkInsertAsync` performance claim.

## Running Benchmarks From A Repository With Agent Worktrees

This repository may contain agent worktrees under `.claude/worktrees/`. Each one
is a full checkout of the repository and therefore carries its own
`benchmarks/nORM.Benchmarks.csproj`. When more than one `nORM.Benchmarks.csproj`
exists anywhere under the repository root, BenchmarkDotNet's project discovery is
ambiguous and the run fails before any measurement is taken.

Do not delete the user's worktrees to work around this. Instead, run benchmarks
through a path that isolates the run in a clean detached checkout:

- `eng/run-benchmark-isolated.ps1 -- <benchmark args>` — general-purpose wrapper.
  When duplicate benchmark projects are present it creates a detached
  `git worktree` of `HEAD` under the system temp directory (which never contains
  `.claude/worktrees`), runs `dotnet run` there, copies the raw reports back into
  `benchmarks/BenchmarkDotNet.Artifacts`, and removes the temporary worktree.
- `eng/run-provider-benchmark-slice.ps1` — provider-matrix slices; applies the
  same isolation and additionally merges per-slice CSV evidence and can run the
  threshold checker.
- `eng/v1-release-gate.ps1` (`rc`/`full` modes) — applies the same isolation for
  its benchmark step automatically.

Only run the benchmark project directly with `dotnet run` from `benchmarks/` when
you have confirmed there are no duplicate `nORM.Benchmarks.csproj` files under the
repository root (for example in a clean clone with no agent worktrees).

## BenchmarkDotNet Configuration

Provider-matrix public evidence uses at least `launchCount: 3`,
`warmupCount: 3`, and `iterationCount: 20`. Shorter jobs may exist for local
smoke checks, but their results are not public release evidence. Release
summaries should publish ratios with the BenchmarkDotNet error/confidence
columns and avoid presenting overlapping results as definitive wins.

## Release Artifacts

Release candidates must upload:

- raw BenchmarkDotNet reports,
- machine and SDK information,
- provider versions and connection configuration summary with secrets redacted,
- the exact benchmark filter used,
- a short markdown summary that identifies the fastest method per provider and
  flags any nORM losses above the release threshold.

`eng/benchmark-evidence.ps1` generates the required release-evidence manifest
from `BenchmarkDotNet.Artifacts/results`. It writes
`BenchmarkDotNet.Artifacts/v1-evidence/benchmark-evidence.md` and `.json` with
the release commit, SDK/OS, raw report paths, driver package versions, redacted
provider configuration, and fastest method per provider. Release automation runs
this script after benchmark steps in `full` and `rc` modes.

## Executable Thresholds

`eng/benchmark-thresholds.json` is the versioned v1 benchmark budget file.
`eng/check-benchmark-thresholds.ps1` reads the raw BenchmarkDotNet CSV reports
and compares named nORM paths against the fastest matching Dapper, optimized Raw
ADO, or EF baseline for the same provider. The RC release gate runs this check
as a hard pass/fail step after generating benchmark evidence. `full` mode runs
the same checker with missing provider-matrix rules allowed so local evidence
smoke runs do not pretend to validate the full public benchmark claim.

## Tenant And Temporal Overhead

Tenant and temporal changes must run the focused overhead benchmark before
release sign-off:

```powershell
eng/run-benchmark-isolated.ps1 -- --filter "*TenantTemporalBenchmarks*"
```

`TenantTemporalBenchmarks` records:

- generated tenant query overhead by comparing a manual tenant predicate against
  the injected tenant predicate over the same rowset and result;
- generated insert, update, and delete overhead split by operation with and
  without temporal triggers.

These rows are not a substitute for the public provider matrix, but they are
required evidence when changing tenant boundary code, temporal bootstrap,
temporal triggers, or temporal history materialization. Release notes may cite
tenant/temporal performance only when the raw BenchmarkDotNet report for this
suite is attached to the release artifacts.

`FastNormBenchmarks` is a smoke suite only. The release evidence generator
rejects `FastNormBenchmarks` reports in `rc` and `full` modes so fast local
checks cannot accidentally become public benchmark evidence.
