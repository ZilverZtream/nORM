# Test Suite Ownership

The v1 test suite is expected to be warning-free and searchable. Generated test
outputs must stay out of the repository, and new regression coverage should be
named after the subsystem or behavior it protects.

## Test Categories

Categories are declared in `tests/TestCategories.cs` as string constants and
applied via xUnit's `[Trait("Category", TestCategory.X)]` attribute at the class
level. The table below shows which CI gate runs each category.

| Category | Constant | Description | Quick gate | Live gate | Full/RC gate |
| --- | --- | --- | --- | --- | --- |
| Fast | `TestCategory.Fast` | Pure in-process unit tests; no external I/O | Yes | Yes | Yes |
| LiveProvider | `TestCategory.LiveProvider` | Require real DB connection (NORM_TEST_* env vars) | **Excluded** | Yes | Yes |
| Stress | `TestCategory.Stress` | Long concurrency/fault-injection loops | No | No | Yes |
| ProviderParity | `TestCategory.ProviderParity` | Provider SQL, behavior, migration, bulk, and binding parity suites selected by the v1 live/RC gate while preserving their Fast/Stress category | Yes | Yes | Yes |
| NavigationStress | `TestCategory.NavigationStress` | Navigation batching, cancellation, and provider-cap stress set selected by the RC loop | Yes | Yes | Yes |
| TransactionStress | `TestCategory.TransactionStress` | Transaction lifecycle, race, and fault-injection set selected by the RC loop | Yes | Yes | Yes |
| CompiledQueryStress | `TestCategory.CompiledQueryStress` | Compiled-query binding, fast-path, and SQL-shape set selected by the RC loop | Yes | Yes | Yes |
| ProviderSourceGenParity | `TestCategory.ProviderSourceGenParity` | Provider/source-generator parity set selected by the RC loop | Yes | Yes | Yes |
| BulkProviderParity | `TestCategory.BulkProviderParity` | Bulk-operation provider parity set selected by the RC loop | Yes | Yes | Yes |
| MigrationParity | `TestCategory.MigrationParity` | Migration provider, cancellation, and replay set selected by the RC loop | Yes | Yes | Yes |
| CacheMemory | `TestCategory.CacheMemory` | Cache memory-bound, eviction, and cache-lock set selected by the RC loop | Yes | Yes | Yes |
| AdversarialConcurrency | `TestCategory.AdversarialConcurrency` | Concurrency, adversarial, fault-injection, and stress set selected by the RC loop | Yes | Yes | Yes |
| PackageConsumer | `TestCategory.PackageConsumer` | Inspect/consume built .nupkg artifacts; require prior `dotnet pack` | Yes | Yes | Yes |

### Filtering notes

- The quick gate runs `dotnet test --filter "Category!=LiveProvider"` to skip all
  live-database tests when no provider connection strings are available.
- Live and full gates run without a category filter, so all tests execute.
- Live-provider tests that still enter a provider-specific body without a
  configured server use the shared `Skip.If` helper to early-return. Release
  strictness is enforced by `eng/v1-release-gate.ps1` provider-minimum checks
  before live/RC tests start; runtime skip exceptions are not part of the local
  no-provider contract.
- The v1 release gate runs package-consumer smoke tests with
  `Category=PackageConsumer` and starts live-provider selection with
  `Category=LiveProvider`, then appends `Category=ProviderParity` for
  provider SQL, behavior, migration, bulk, and binding parity suites that must
  stay in live/RC evidence without being marked live-provider-only.
- The RC loop routes navigation, transaction, compiled-query, provider/source-gen,
  bulk, migration, and cache-memory buckets through the dedicated categories
  above. The broad concurrency/adversarial gate routes through
  `Category=AdversarialConcurrency`. These category filters were matched
  against the previous name-based filters with identical discovered test sets.
- To run only fast tests locally: `dotnet test tests/ --filter "Category=Fast"`
- To run only live tests: `dotnet test tests/ --filter "Category=LiveProvider"`

### Annotation coverage

Every public class that declares xUnit `[Fact]` or `[Theory]` tests must carry
an explicit `[Trait("Category", ...)]` annotation. `TestCategoryHygieneTests`
enforces the allowed v1 categories in-process so new test classes cannot fall
out of the category-driven gates by accident.
It also rejects runtime skip exceptions in `src/` and `tests/`; live-provider
tests should use the shared `Skip.If` early-return helper and rely on the
release gate for required-provider failures.

Helper files, xUnit collection definitions, and shared test infrastructure do
not declare test methods and are exempt from category traits.

## Generated Outputs

Local and CI test outputs belong under ignored artifact paths:

- `tests/TestResults/`
- `*.trx`
- `*.coverage`
- `coverage/`
- `coverage-report/`

Release-gate TRX files are uploaded as CI artifacts, not committed source.

## Encoding Gate

`eng/scripts/check-encoding.ps1` runs in the v1 release gate and rejects
replacement characters plus common double-encoded mojibake markers. Keep marker
examples encoded as character codes or in the script's explicit excluded docs so
the scan stays actionable.

## Legacy Coverage Files

The `CoverageBoost*.cs` files are legacy regression suites that predate the v1
ownership cleanup. They remain in place because they cover many targeted
translation, materialization, navigation, provider, and exception edge cases.
Do not add new catch-all `CoverageBoost` files. When touching these tests,
prefer moving coherent groups into domain-named files such as
`QueryTranslationRegressionTests`, `MaterializerRegressionTests`,
`NavigationRegressionTests`, or `ProviderRegressionTests`.
Do not recreate `CoverageBoostScaffoldingRuntimeGroupsTests.cs`; its mixed
scaffolding/runtime/navigation/migration groups have been split into
class-named files with the original test class names.

## Scaffolding Source Size

Production scaffolding files stay below 250 lines so reverse-engineering logic
remains split by provider, model phase, or generated-code responsibility.
`RepositoryHygieneTests` enforces this boundary for `src/nORM/Scaffolding`.
When a scaffolding change needs more room, split the responsibility into a new
focused helper instead of growing an existing file.

## CLI Scaffold Source Size

CLI scaffold command files stay below 200 lines so option binding, request
resolution, execution, output handling, and project/configuration helpers remain
separate responsibilities. `RepositoryHygieneTests` enforces this boundary for
`src/dotnet-norm/Program.Scaffolding*.cs`.

## CLI Design-Time Source Size

CLI design-time loading files stay below 200 lines so assembly loading,
dependency probing, runtimeconfig probing, context creation, and project
resolution remain separate responsibilities. `RepositoryHygieneTests` enforces
this boundary for `src/dotnet-norm/Program.DesignTime*.cs`.

## Provider Mobility Certification Source Size

Provider mobility certification files stay below 200 lines so report models,
orchestration, provider-target decisions, schema-snapshot inspection, findings,
recommendations, and report writers remain separate responsibilities.
`RepositoryHygieneTests` enforces this boundary for
`src/dotnet-norm/ProviderMobilityCertification*.cs`.

## Provider Mobility Source Scanner Size

Provider mobility source scanner files stay below 200 lines so source traversal,
rule inventories, source-file scanning, project-file XML scanning, and finding
classification remain separate responsibilities. `RepositoryHygieneTests`
enforces this boundary for `src/dotnet-norm/ProviderMobilitySourceScanner*.cs`.

## Provider Mobility Schema Inspector Size

Provider mobility schema inspector files stay below 200 lines so table
traversal, column checks, default-value classification, foreign-key validation,
provider-generation smoke checks, and CLR type resolution remain separate
responsibilities. `RepositoryHygieneTests` enforces this boundary for
`src/dotnet-norm/ProviderMobilitySchemaInspector*.cs`.

## Provider Mobility Translation Size

Provider mobility translation files stay below 250 lines so public mobility
contract models, finding classification, runtime strict checks, provider
capability/version decisions, implementation strategy groups, and SQL probes
remain separate responsibilities. `RepositoryHygieneTests` enforces this
boundary for `src/nORM/Configuration/ProviderMobility*.cs`.

## Scaffolding Contract Test Sources

Scaffolding contract source-reader helpers stay in
`ScaffoldingContractDocTestSources.cs` so `ScaffoldingContractDocTests.cs`
stays focused on the consumer-facing contract assertions.

## Scaffolding Live-Provider Matrix

Scaffold live-provider tests should cover SQLite, SQL Server, PostgreSQL, and
MySQL whenever the database feature exists on all four providers. Partial
provider coverage must be listed in `ScaffoldLiveProviderParityInventoryTests`
with the exact provider set and a capability reason, so missing providers are
explicit decisions rather than accidental gaps. Provider-specific live-provider
`[Fact]` tests are inventoried there as well, so provider-only scaffold coverage
cannot bypass the all-four matrix by avoiding provider `InlineData`.

## CLI Integration Test Size

CLI integration tests stay below 1500 lines per file so command coverage remains
split by command area instead of growing one catch-all test object.
`RepositoryHygieneTests` enforces this boundary for `tests/CliIntegration*.cs`.
When adding CLI scaffold, migration, database, or portability coverage, place it
in the matching partial test file or create a new command-area file.

## Query Translator Source Size

The central `QueryTranslator.cs` file stays below 1500 lines so translator state
and dispatch do not absorb plan generation, post-materialization, or other large
helper responsibilities. Every `QueryTranslator*.cs` partial stays below 1500 lines.
Split each partial by operator family before it grows into a catch-all translator
file. `RepositoryHygieneTests` enforces both boundaries.

## Query Provider Source Size

Every `NormQueryProvider*.cs` partial stays below 1500 lines so aggregate
rewrites, async execution, simple-query fast paths, CUD, and streaming/plan
helpers remain separate execution responsibilities. `RepositoryHygieneTests`
enforces this boundary.

## Select Clause Visitor Source Size

Every `SelectClauseVisitor*.cs` partial stays below 1500 lines so projection
method calls, navigation aggregate subqueries, formatting, operators, and helper
utilities remain separate responsibilities. `RepositoryHygieneTests` enforces
this boundary.

## Expression SQL Visitor Source Size

Every `ExpressionToSqlVisitor*.cs` partial stays below 1500 lines so binary/null
semantics, member/constant folding, control-flow dispatch, method-call
translation, navigation subqueries, formatting, and support helpers remain
separate responsibilities. `RepositoryHygieneTests` enforces this boundary.

## SQLite Provider Source Size

Every `SqliteProvider*.cs` partial stays below 1500 lines so SQLite SQL
translation, schema/temporal behavior, and bulk DML remain separate provider
responsibilities. `RepositoryHygieneTests` enforces this boundary.

## Concrete Provider Source Size

Every `SqlServerProvider*.cs`, `PostgresProvider*.cs`, and `MySqlProvider*.cs` file stays below 1500 lines so concrete provider dialect defaults, scalar SQL,
regex and method translation, temporal/tenant/runtime behavior, and bulk DML remain separate provider responsibilities. `RepositoryHygieneTests` enforces
this boundary.

## Database Provider Source Size

Every `DatabaseProvider*.cs` partial stays below 1500 lines so base provider
capability and dialect defaults, temporal/tenant hooks, scalar SQL expressions,
runtime/connection validation, fallback bulk operations, and DML SQL builders
remain separate responsibilities. `RepositoryHygieneTests` enforces this
boundary.

## DbContext Source Size

Every `DbContext*.cs` partial stays below 1500 lines so construction,
connection/command infrastructure, mapping/query roots, transactions,
tenant/temporal APIs, disposal, raw SQL, prepared statements, change tracking,
and write operations remain separate responsibilities. `RepositoryHygieneTests`
enforces this boundary.

## Schema Snapshot Source Size

Every `SchemaSnapshot*.cs` file stays below 1500 lines so snapshot DTOs, model
scanning, destructive-change diagnostics, and diffing remain separate migration
responsibilities. `RepositoryHygieneTests` enforces this boundary.

## Entity Type Builder Source Size

Every `EntityTypeBuilder*.cs` file stays below 1500 lines so configuration
storage, core entity-level API, property builders, reference builders, and
collection/many-to-many builders remain separate model-configuration
responsibilities. `RepositoryHygieneTests` enforces this boundary.

## Materializer Factory Source Size

Every `MaterializerFactory*.cs` file stays below 1500 lines so cache state,
compiled-materializer guards, IL precompile emitters, public factory APIs,
schema-aware mapping, core materialization, constructor/projection helpers,
projection-column extraction, reader emitters, conversions, and support types
remain separate query-pipeline responsibilities. `RepositoryHygieneTests`
enforces this boundary.
