# Release Gates

This repository has two levels of verification:

- The ordinary test suite, which runs without external databases and is suitable for every change.
- The live provider gate, which also runs provider parity and provider-swap tests against any configured SQL Server, PostgreSQL, or MySQL databases.

Release and CI builds use the supported SDK pinned in `global.json`. The v1
gate currently expects .NET SDK `8.0.417` with `allowPrerelease` disabled; CI
prints `dotnet --info` and fails if the resolved SDK differs from the pinned
version or resolves to a preview build.

## Local Live Provider Gate

Use `eng\live-provider-gate.cmd` from the repository root.

```cmd
eng\live-provider-gate.cmd quick
eng\live-provider-gate.cmd live
eng\live-provider-gate.cmd full
```

Modes:

- `build`: restore and build the solution.
- `quick`: build and run the provider-swap smoke gate.
- `live`: build and run live provider parity, provider behavior, migration, bulk, and provider-swap tests.
- `full`: restore, build, run the full test suite, then run the live gate.

The script uses the same environment variables as the tests:

```cmd
set NORM_TEST_SQLSERVER=Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False
set NORM_TEST_POSTGRES=Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=postgres
set NORM_TEST_MYSQL=Server=127.0.0.1;Port=3306;Database=normtest;User ID=root;Password=normtest;AllowPublicKeyRetrieval=True
```

`*_CS` aliases are also supported by the test infrastructure, for example `NORM_TEST_POSTGRES_CS`.

The gate sets `NORM_REQUIRE_LIVE_PARITY=any` when at least one live provider is configured and defaults `NORM_MIN_LIVE_PROVIDERS` to the number of configured providers. Set either variable before running the script when a stricter local policy is needed.

For a release candidate, run `full` with every supported live provider configured. For everyday regression work, run `quick` or `live` with the providers available on the machine.

## v1.0 Release Candidate Gate

Use `eng\v1-release-gate.ps1` for v1.0 release-candidate validation. It extends
the live-provider gate with the public API snapshot, package creation, repeated
navigation/transaction/compiled-query stress loops, provider/source-generator
parity, migration parity, concurrency/adversarial coverage, the fast complex
query benchmark, and the full SQLite/SQL Server/PostgreSQL/MySQL provider
benchmark matrix.

```powershell
.\eng\v1-release-gate.ps1 -Mode rc -MinLiveProviders 3 -StressIterations 20
```

Modes:

- `quick`: restore, build, public API snapshot, and package validation.
- `live`: quick gate plus live provider tests.
- `full`: live gate plus full test suite and benchmark.
- `rc`: full gate plus the repeated v1.0 stress/parity loops and provider
  matrix benchmarks.

The RC provider matrix requires `NORM_TEST_SQLSERVER` (or
`NORM_TEST_SQLSERVER_CS`), `NORM_TEST_POSTGRES` (or `NORM_TEST_POSTGRES_CS`),
and `NORM_TEST_MYSQL` (or `NORM_TEST_MYSQL_CS`). Use
`-SkipProviderMatrixBenchmark` only when validating a non-performance change
that cannot access all three external servers locally.

The public API baseline is documented in `docs\public-api-policy.md`. The v1.0
release checklist is documented in `docs\v1-readiness.md`.

## CI Release Gates

CI uses the same release-gate script instead of a separate hand-written test
path:

- `.github/workflows/ci.yml` runs the quick package/API/CLI gate on every push
  and pull request.
- The live MySQL, PostgreSQL, and SQL Server jobs run `v1-release-gate.ps1
  -Mode live -MinLiveProviders 1 -SkipBenchmark` against their service
  database.
- `.github/workflows/v1-rc.yml` runs the full `rc` gate on a weekly schedule and
  on manual dispatch with SQL Server, PostgreSQL, and MySQL service databases.

The CI gates upload TRX test results, generated packages, and BenchmarkDotNet
artifacts so the release evidence is attached to the workflow run.
