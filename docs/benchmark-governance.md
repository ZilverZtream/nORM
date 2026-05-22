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
  the ORM/Dapper path.
- `RawAdo_PreparedOptimized`: prepared command reuse plus the optimized reader.
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

## BenchmarkDotNet Configuration

Provider-matrix public evidence uses at least `warmupCount: 3` and
`iterationCount: 10`. Shorter jobs may exist for local smoke checks, but their
results are not public release evidence.

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
