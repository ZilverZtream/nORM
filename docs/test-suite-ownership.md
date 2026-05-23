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
| PackageConsumer | `TestCategory.PackageConsumer` | Inspect/consume built .nupkg artifacts; require prior `dotnet pack` | Yes | Yes | Yes |

### Filtering notes

- The quick gate runs `dotnet test --filter "Category!=LiveProvider"` to skip all
  live-database tests when no provider connection strings are available.
- Live and full gates run without a category filter, so all tests execute.
- To run only fast tests locally: `dotnet test tests/ --filter "Category=Fast"`
- To run only live tests: `dotnet test tests/ --filter "Category=LiveProvider"`

### Annotation coverage

Annotated so far:
- `LiveProviderIntegrationTests` — `LiveProvider`
- `LiveCrossProviderTests` — `LiveProvider`
- `LiveProviderSavepointMigrationTests` — `LiveProvider`
- `PackageConsumerIntegrationTests` — `PackageConsumer`

All other test classes run in every gate (they are implicitly Fast unless
annotated otherwise).

## Generated Outputs

Local and CI test outputs belong under ignored artifact paths:

- `tests/TestResults/`
- `*.trx`
- `*.coverage`
- `coverage/`
- `coverage-report/`

Release-gate TRX files are uploaded as CI artifacts, not committed source.

## Legacy Coverage Files

The `CoverageBoost*.cs` files are legacy regression suites that predate the v1
ownership cleanup. They remain in place because they cover many targeted
translation, materialization, navigation, provider, and exception edge cases.
Do not add new catch-all `CoverageBoost` files. When touching these tests,
prefer moving coherent groups into domain-named files such as
`QueryTranslationRegressionTests`, `MaterializerRegressionTests`,
`NavigationRegressionTests`, or `ProviderRegressionTests`.
