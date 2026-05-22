# Test Suite Ownership

The v1 test suite is expected to be warning-free and searchable. Generated test
outputs must stay out of the repository, and new regression coverage should be
named after the subsystem or behavior it protects.

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
