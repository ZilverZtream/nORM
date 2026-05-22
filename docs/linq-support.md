# LINQ Support Matrix

nORM v1 does not claim unlimited LINQ translation. The supported contract is:
server-side translation for the query shapes below, explicit client-evaluation
rules for projections, and deterministic exceptions for shapes outside the
matrix.

Status values:

- Supported: intended v1 behavior for SQL Server, SQLite, PostgreSQL, and MySQL.
- Constrained: supported only under the restrictions listed.
- Preview: usable but not part of the v1 compatibility promise.
- Unsupported: must fail deterministically rather than silently changing
  semantics.

| Feature | Status | Notes |
| --- | --- | --- |
| `Where` predicates | Supported | Member comparisons, boolean logic, null checks, `Contains`, `StartsWith`, `EndsWith`, string/date/math translations covered by provider tests. |
| `Select` entity and scalar projections | Supported | Fully translatable projections run server-side. |
| `Select` with custom client logic | Constrained | Controlled by `DbContextOptions.ClientEvaluationPolicy`: `Warn` logs and allows the projection tail after server materialization, `Throw` rejects it, and `Allow` permits it silently. |
| `OrderBy`, `ThenBy` | Supported | Provider-specific identifier escaping and expression translation apply. |
| `Skip`, `Take` | Supported | Provider-specific paging syntax is used. MySQL skip-only queries emit the maximum unsigned BIGINT limit. |
| `Distinct` | Supported | Applies to the projected SQL shape. |
| `Count`, `LongCount`, `Any`, `All` | Supported | Includes predicate overloads where translated by the expression visitor. |
| `Sum`, `Average`, `Min`, `Max` | Supported | Aggregate projection and grouped aggregate paths are covered by translator tests. |
| `GroupBy` | Constrained | Supported for SQL grouping and aggregate projections. Arbitrary client `IGrouping` composition is not a v1 guarantee. |
| Inner joins | Supported | `Join` translates to provider SQL and is included in benchmark parity gates. |
| Group joins | Constrained | Supported for simple key joins and bounded by `DbContextOptions.MaxGroupJoinSize`. Async streaming with group joins is not supported. |
| `SelectMany` | Constrained | Supported for tested navigation/query shapes. Complex correlated collection expansion is not a full v1 guarantee. |
| Set operations: `Union`, `Intersect`, `Except` | Supported | Translator tests cover SQL generation and execution coverage tests cover result behavior. |
| `Include`, `ThenInclude` | Constrained | Supported for mapped navigation paths. Composite-key dependent includes and unsupported relationship shapes throw `NotSupportedException`. |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | Supported | These are nORM-specific query modifiers with dedicated tests. |
| Raw SQL composition | Constrained | `FromSqlRawAsync<T>` executes caller-provided SQL. Caller owns SQL shape and parameter safety until the safer raw SQL API gate is completed. |
| `AsAsyncEnumerable` | Constrained | Streams ordinary queries. Include and group-join streaming paths are rejected because they require coordinated materialization. |
| `ExecuteUpdateAsync` | Constrained | Simple filtered entity queries only. Grouped, ordered, joined, and aggregated queries are rejected. Set expressions currently allow constants and captured values through the existing bulk CUD path. |
| `ExecuteDeleteAsync` | Constrained | Simple filtered entity queries only. Grouped, ordered, joined, and aggregated queries are rejected. |
| Arbitrary CLR methods in predicates | Unsupported | Unsupported methods/members throw from expression translation instead of being evaluated client-side in filters. |
| Arbitrary client evaluation before server filtering/paging | Unsupported | Server filters and paging must happen before any allowed client projection tail. |

## Provider Notes

The matrix is provider-neutral unless a provider-specific note says otherwise.
The same query shape should either work on every supported provider or fail with
a documented provider limitation. Provider-specific syntax differences are
handled by `DatabaseProvider` implementations and cross-provider tests.

## Documentation Rules

- README wording must point here instead of claiming "full LINQ" or "complete
  LINQ" without qualification.
- New LINQ translations need a test that proves SQL shape or result behavior.
- New unsupported LINQ shapes need a test that proves the exception is stable
  and does not fall back to unsafe client-side filtering.
- `docs/linq-support-coverage.md` maps each non-unsupported row in this matrix
  to executable test files and is enforced by documentation contract tests.
