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

## Scaffolding Gate

Use this focused local gate after scaffolding, CLI scaffold, or schema metadata
changes:

```powershell
dotnet build nORM.sln -c Release --nologo
dotnet test tests/nORM.Tests.csproj -c Release --no-build --filter "Scaffolding|SchemaSignatureTests|DynamicTypeQueryTests|RelationshipConfigurationTests|UpdateNoMutableColumnsTests|PublicApiSnapshotTests|PublicApiClassificationTests|Scaffold_fail_on_warnings_returns_nonzero_after_writing_report|Scaffold_with_warnings_returns_zero_and_prints_warning_paths|Scaffold_sqlite_output_builds_as_consumer_project|Scaffold_help_describes_bounded_contract_and_warning_reports"
```

Live provider scaffolding parity is included in `eng\live-provider-gate.cmd live`
and the RC gate through `LiveProviderScaffoldingParityTests`.

## RC3 Tenant/Temporal Sample Gate

RC3 adds a product-proof sample gate around `samples/nORM.Sample.Store`. The
sample is an ASP.NET Core web app with a browser frontend, tenant login,
authenticated APIs, bulk operations, representative LINQ, and nORM-managed
temporal history. The same focused gate also covers native tenant session/RLS
DDL tests, explicit RLS apply/drop tests, and explicit SQL Server
provider-native temporal mode/bootstrap tests.

Use the fast local path while iterating:

```powershell
dotnet build nORM.sln -c Release --nologo
dotnet test tests/nORM.Tests.csproj -c Release --no-build --filter "Tenant|Temporal|Sample|ProviderSwap|ProviderMobilityStrict"
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- verify-providers
```

Use the strict provider mobility gate for release evidence:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --report ../../artifacts/provider-swap/sample-store.json
```

Unlike `verify-providers`, `certify-provider-swap` fails when a requested
provider is missing or fails. The JSON report is the provider-swap artifact for
the sample app. The certification scenario runs with
`DbContextOptions.UseStrictProviderMobility()` so provider-bound escape hatches
cannot pass as provider-mobile evidence. The report records `ScanStatus` as
`NotRequested`, `Pass`, or `Fail` so an empty findings list is not ambiguous.

For existing applications, add `--scan-path <app-source>` to include
provider-bound usage findings in the JSON artifact. Strict certification fails
when the scan finds raw SQL APIs, stored procedure APIs, direct connection
access, raw transaction/command handles, command interceptors, direct provider
access, dynamic table queries, custom `[SqlFunction]` SQL fragments,
`[CompileTimeQuery]` raw SQL, provider-native tenant/temporal configuration, or
silent client-eval opt-ins, because those must be rewritten, emulated, or
reviewed before the application can be called provider-mobile. Explicit
`ClientEvaluationPolicy.Warn` projection tails are warning-level inventory:
they are only admissible after server filters, ordering and paging have run.
Provider package and connection bootstrapping is warning-level inventory when
it stays in the composition root.

Runtime strict failures and static certification findings must stay aligned
through `ProviderMobilityTranslator`; the support classes, severity, reason,
and suggested-fix contract are documented in
[`provider-mobility-translation-layer.md`](provider-mobility-translation-layer.md).

For application-level portability evidence outside the sample app, run the
reusable CLI certification command and include schema inspection:

```powershell
norm portability certify --scan-path src/MyApp --assembly bin/Release/net8.0/MyApp.dll --report artifacts/provider-mobility.json --html artifacts/provider-mobility.html
```

`--assembly` builds the schema snapshot from the design-time nORM context.
`--schema-snapshot Migrations/schema.snapshot.json` can be used when only the
saved migration snapshot is available. The schema check rejects unsupported CLR
column types, provider-specific default SQL, invalid FK metadata, and
non-integral identity columns before the app can be called provider-mobile.
Use `--providers` to narrow the provider target capability profile recorded in
the report; otherwise `all-four` records SQLite, SQL Server, PostgreSQL, and
MySQL target decisions. Add target connection options such as
`--sqlite-connection` or `--postgres-connection` when the release artifact must
prove actual server versions instead of descriptor-only capability floors. A
descriptor-only report must leave `ActualServerVersion` empty; it must not reuse
the minimum supported version as fake live evidence.
Live target certification also executes a minimal provider JSON expression on
each supplied target so the report proves required JSON functionality is present,
not only that the server version looks high enough.
The provider target section must include both coarse capabilities and concrete
translation-strategy rows: paging, identifier escaping, parameter binding,
boolean predicates, null semantics, LIKE escaping, string concatenation,
DateTime/decimal/TimeSpan normalization, temporal clock source, generated-key
retrieval, bitwise XOR, case-sensitive string comparison, regex translation,
temporal construction/arithmetic, row-tuple comparison, ordered string
aggregation, and SQL statement length limits. Warning-level
target rows do not fail the gate, but they must appear in the recommendation
section so reviewers can distinguish native behavior from nORM-owned emulation.

Tenant or temporal implementation changes also require the focused overhead
benchmark:

```powershell
dotnet run --project benchmarks/nORM.Benchmarks.csproj -c Release -- --filter "*TenantTemporalBenchmarks*"
```

Current local RC3 overhead evidence from isolated BenchmarkDotNet run
`TenantTemporalBenchmarks` at commit `48c721e5`: generated tenant count query
measured 64.29 us versus 64.53 us for the same manual tenant predicate
(1.03x, 99.9% CI overlap) while allocating 2.66 KB versus 7.07 KB.
Temporal insert measured 37.80 us versus 36.79 us without temporal triggers.
Temporal update measured 2.16 ms versus 2.06 ms without temporal triggers, and
temporal delete measured 2.12 ms versus 1.81 ms without temporal triggers. The
write measurements have wide intervals and must be cited with the raw
BenchmarkDotNet report, not as a precise point claim.

For RC evidence, run the live provider gate with SQL Server, PostgreSQL, and
MySQL configured. `TenantTemporalProviderSwapTests` executes the same sample
scenario on SQLite and every configured live provider.

Current RC3 local evidence with SQL Server, PostgreSQL, and MySQL configured:
`dotnet test tests\nORM.Tests.csproj -c Release --no-build --filter
"Tenant|Temporal|Sample|ProviderSwap|ProviderMobilityStrict"` passed 594/594,
`eng\live-provider-gate.cmd live` passed 1425/1425, and
`dotnet run --project samples/nORM.Sample.Store -c Release --no-build --
verify-providers` passed SQLite, SQL Server, PostgreSQL, and MySQL.
The sample certification report records error/warning totals and recommended
fix rows in addition to per-provider checks. Provider FAIL results, and SKIP
results under strict certification, contribute error-level report evidence.
SQLite sample verification runs against an isolated in-memory database so
parallel certification probes cannot collide on a shared local file.

## v1.0 Release Candidate Gate

Use `eng\v1-release-gate.ps1` for v1.0 release-candidate validation. It extends
the live-provider gate with the public API snapshot, package creation, repeated
navigation/transaction/compiled-query stress loops, provider/source-generator
parity, migration parity, concurrency/adversarial coverage, the fast complex
query benchmark, and the full SQLite/SQL Server/PostgreSQL/MySQL provider
benchmark matrix. The provider matrix runs as provider-specific slices so each
database has its own log and raw BenchmarkDotNet reports before the threshold
gate evaluates the merged result set.

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

**`-SkipBenchmark` caveat**: When `-SkipBenchmark` is passed, BenchmarkDotNet
is not run and the resulting RC artifact manifest contains no performance
evidence. A manifest produced with `-SkipBenchmark` validates correctness and
provider parity only — it does not constitute benchmark evidence and must not be
used to support public performance claims. Run without `-SkipBenchmark` on the
release commit before making any throughput or latency claims.

The RC provider matrix writes its split runs under
`BenchmarkDotNet.Artifacts/provider-slices/<timestamp>/` and then points the
benchmark evidence and threshold steps at that run's merged `results/`
directory. This keeps SQLite, SQL Server, PostgreSQL, and MySQL failures
isolated while preserving one release-grade threshold report.

Every successful gate writes an artifact index with
`eng/rc-artifact-manifest.ps1` under `artifacts/v1-rc/`. The manifest records
the validated commit, mode, SDK, configured providers, TRX files, package files,
BenchmarkDotNet outputs, and benchmark evidence/threshold summaries. A release
tag should point at the commit in this manifest, not merely at a locally passing
working tree.

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

The CI gates upload TRX test results, generated packages, BenchmarkDotNet
artifacts, and the RC artifact manifest so the release evidence is attached to
the workflow run.
