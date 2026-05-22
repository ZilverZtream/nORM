# Contributing

Contributions should be small enough to review and must include the evidence
needed to prove the change.

## Development Setup

1. Install the .NET SDK pinned by `global.json`.
2. Run `dotnet restore`.
3. Run `dotnet build nORM.sln -c Release --no-restore --nologo`.
4. Run focused tests for the area changed.

## Pull Request Bar

- Include tests or a clear explanation when tests are not applicable.
- Update docs for public API, provider, CLI, migration, or behavior changes.
- Update `tests/PublicApi.Shipped.txt` only when the public API change is
  intentional.
- For performance changes, include BenchmarkDotNet output and follow
  `docs/benchmark-governance.md`.
- Do not commit generated packages, TRX files, coverage reports, or local
  BenchmarkDotNet artifacts.

## Coding Guidelines

- Prefer existing patterns and provider abstractions.
- Keep provider-specific behavior explicit.
- Do not broaden public API surface unless it is documented and tested.
- Unsupported LINQ/provider behavior must fail deterministically with the
  documented exception taxonomy.
