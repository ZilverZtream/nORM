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

## Query operators

| Feature | Status | Notes |
| --- | --- | --- |
| `Where` predicates | Supported | Member comparisons, boolean logic, null checks, `Contains`/`StartsWith`/`EndsWith`, conditional (ternary) expressions, arithmetic (`+`/`-`/`*`/`/`/`%`), and string/date/math translations covered by provider tests. |
| `Select` entity and scalar projections | Supported | Fully translatable projections run server-side. |
| `Select` into anonymous types | Supported | Positional constructor matching against the projected columns. |
| `Select` into DTO with positional record | Supported | Constructor parameter names matched against projected member names. |
| `Select` into DTO with parameterized class constructor | Supported | Same constructor-match rule as records. |
| `Select` into DTO with init-only / writable property setters (`new T { A = x.A }`) | Supported | `MemberInit` projections bind each assignment to the DTO property; navigation-collection assignments are skipped. |
| `Select` with custom client logic | Constrained | Controlled by `DbContextOptions.ClientEvaluationPolicy`. The v1 default is `Throw`; `Warn` logs and allows the projection tail after server materialization, and `Allow` permits it silently. `string.Format`/interpolated strings/enum `.ToString()` fall into this path. |
| `OrderBy`, `ThenBy` | Supported | Including the `Descending` variants. Provider-specific identifier escaping and expression translation apply. |
| `Reverse` | Supported | Flips the active ORDER BY direction at the SQL layer. |
| `Skip`, `Take` | Supported | Provider-specific paging. `Skip(n).Take(m)` is the canonical pagination shape. Constraint: `Take(n).Skip(m)` currently treats the two operations commutatively; the wrap-in-subquery form is post-v1. |
| `Distinct` | Supported | Applies to the projected SQL shape. |
| `Count`, `LongCount`, `Any`, `All` | Supported | Includes predicate overloads and short-circuiting AND/OR. |
| Navigation aggregates: `parent.Children.Any(...)`, `.All(...)`, `.Count()`, `.LongCount()` | Supported | Emit correlated `EXISTS` / `NOT EXISTS` / scalar `(SELECT COUNT(*) ...)` subqueries against the dependent table. |
| `Sum`, `Average`, `Min`, `Max` | Supported | Both top-level aggregate queries and grouped aggregates, including computed selectors like `g.Sum(x => x.Price * x.Qty)` and conditional selectors like `g.Sum(x => x.Active ? x.Score : 0)`. |
| `GroupBy` | Constrained | Single-column keys only; composite anonymous-type keys throw `NormQueryException` at translation time. Within that constraint: `g.Key`, individual aggregates, multi-aggregate anonymous-type projections, and `Where(g => aggregate ...)` (HAVING) all execute server-side. Arbitrary client `IGrouping` composition is not a v1 guarantee. |
| Inner joins | Supported | `Join` translates to provider SQL and is included in benchmark parity gates. |
| Group joins | Constrained | Supported for simple key joins and bounded by `DbContextOptions.MaxGroupJoinSize`. Async streaming with group joins is not supported. The `GroupJoin` + `SelectMany` + `DefaultIfEmpty` (query-syntax left join) composition is not a full v1 contract for arbitrary projection bodies; prefer navigation + `DefaultIfEmpty()`. |
| `SelectMany` | Constrained | Supported shapes: cross-product (`from a in A from b in B select new { a.X, b.Y }` → `CROSS JOIN`), inner-join through a navigation (with optional `Where` filter), and `+ DefaultIfEmpty()` over a navigation (→ `LEFT JOIN`, parent rows without matching children appear once with null child fields). Complex correlated collection expansion is not a full v1 guarantee. |
| Set operations: `Union`, `Intersect`, `Except` | Supported | Each side must project the same row shape. `Concat` lowers to `UNION ALL` under the same shape rule. Operators applied AFTER the set operation (later `Where` / `OrderBy` / `Skip` / `Take`) are post-v1; compose ordering and paging within each side, then combine. |
| `Include`, `ThenInclude` | Constrained | Supported for mapped navigation paths, including multi-level chains. Use with `AsSplitQuery()` for nested-collection paths. Composite-key dependent includes and unsupported relationship shapes throw `NormUnsupportedFeatureException`. |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | Supported | These are nORM-specific query modifiers with dedicated tests. |
| Raw SQL composition | Constrained | `FromSqlRawAsync<T>` and `FromSqlInterpolatedAsync<T>` execute read-only `SELECT`/CTE statements through the provider-aware raw query gate. Raw SQL does not add tenant predicates automatically. |
| `AsAsyncEnumerable` | Constrained | Streams ordinary queries. Include and group-join streaming paths are rejected because they require coordinated materialization. |
| `ExecuteUpdateAsync` | Constrained | Simple filtered entity queries only. Grouped, ordered, joined, distinct, paged, and aggregated queries are rejected. Assignment values allow literal constants and precomputed captured locals only; method calls, inline computed values, column-based updates, and server expressions are post-v1. |
| `ExecuteDeleteAsync` | Constrained | Simple filtered entity queries only. Grouped, ordered, joined, distinct, paged, and aggregated queries are rejected. |
| Arbitrary CLR methods in predicates | Unsupported | Unsupported methods/members throw from expression translation instead of being evaluated client-side in filters. |
| Arbitrary client evaluation before server filtering/paging | Unsupported | Server filters and paging must happen before any allowed client projection tail. |
| `TakeWhile` / `SkipWhile` | Unsupported | No SQL equivalent; throw deterministically rather than buffering the entire table client-side. |
| `OfType<T>` / `Cast<T>` | Unsupported | Post-v1; would require TPH discriminator integration on `OfType` and a runtime conversion on `Cast`. |
| `SequenceEqual` | Unsupported | Not a query contract; materialize both sides explicitly with `ToListAsync` and compare in CLR. |

## Terminal operators

| Feature | Status | Notes |
| --- | --- | --- |
| `ToListAsync`, `ToArrayAsync`, `ToDictionaryAsync` (2 overloads), `ToHashSetAsync` | Supported | Reference-type generic constraint matches the underlying `IQueryable<T> where T : class` rule. |
| `FirstAsync`, `FirstOrDefaultAsync` | Supported | Apply `Take(1)` server-side. |
| `LastAsync`, `LastOrDefaultAsync` | Supported | Flip the ORDER BY direction and `Take(1)` server-side. |
| `SingleAsync`, `SingleOrDefaultAsync` | Supported | Read up to 2 rows to detect duplicate-row violations. |
| `ElementAt`, `ElementAtOrDefault` | Supported | Translate to `OFFSET n LIMIT 1`. |
| `CountAsync`, `LongCountAsync`, `AnyAsync` | Supported | Translate to `COUNT(*)` / `EXISTS` server-side. |
| `AsAsyncEnumerable` | Constrained | See main query-operator matrix above. |

## Value translations

| Feature | Status | Notes |
| --- | --- | --- |
| `string` methods: `ToUpper`, `ToLower`, `Length`, `Trim`/`TrimStart`/`TrimEnd`, `Substring`, `Replace`, `IndexOf`, `Contains`, `StartsWith`, `EndsWith` | Supported | Provider-specific syntax. |
| `string` statics: `IsNullOrEmpty`, `IsNullOrWhiteSpace`, `Concat`, `Compare`, `CompareTo` | Supported | `Compare` / `CompareTo` emit a `CASE` returning `-1` / `0` / `1`. |
| `string.Format` / interpolated strings | Constrained | Run via the client-side projection split under `ClientEvaluationPolicy.Allow` or `Warn`. |
| `Convert.ToInt32` / `ToInt64` / `ToString` / `ToBoolean` / `ToDouble` / `ToDecimal` / etc. | Supported | Emit a provider `CAST`. |
| `Math.Abs`, `Ceiling`, `Floor`, `Round`, `Sqrt`, `Pow`, `Exp`, `Log`, `Log10`, `Sign`, `Min`, `Max`, `Truncate` | Supported | Per-provider mapping (`LEAST`/`GREATEST` on PG/MySQL, `CASE` for SQL Server `Min`/`Max`, etc.). |
| `DateTime` members: `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `DayOfYear`, `DayOfWeek`, `Date` | Supported | Provider-specific extraction; `DayOfWeek` normalized to `System.DayOfWeek` (`0`=Sun..`6`=Sat) on every provider. |
| `DateTime.AddDays` / `AddMonths` / `AddYears` / `AddHours` / `AddMinutes` / `AddSeconds` | Supported | Provider-specific interval arithmetic. |
| `DateTime.UtcNow` / `DateTime.Now` / `DateTime.Today` in predicates | Supported | Evaluated as a bound parameter at execution time (static-member constant path). |
| `DateTimeOffset` members | Supported | Same member surface as `DateTime`; materializer parses SQLite TEXT into `DateTimeOffset`. |
| `DateOnly.Year` / `Month` / `Day` / `DayOfYear` | Supported | Per provider. |
| `TimeOnly.Hour` / `Minute` / `Second` | Supported | Per provider. |
| `TimeSpan` member access (`TotalSeconds`, `Days`, etc.) | Unsupported | Storage-format aware translation is post-v1; route through client-side projection. |
| `Nullable<T>.HasValue`, `Value`, `GetValueOrDefault()` / `GetValueOrDefault(fallback)` | Supported | Emit `IS NOT NULL`, the operand itself, and `COALESCE(..., fallback)` respectively. |
| Conditional expressions (`cond ? a : b`) in `Where` and `Select` | Supported | Emit `CASE WHEN ... END`. Nested conditionals supported. |
| Arithmetic operators (`+`, `-`, `*`, `/`, `%`) in `Where`, `Select`, and aggregate selectors | Supported | Column types drive numeric semantics. |
| Enum equality and `(int)enumCol` projection | Supported | Enum values are emitted as their underlying integer; cast collapses to the operand. |
| Enum `.ToString()` in projection | Constrained | Routed via the client-side projection split; the SQL fetches the underlying integer column. |
| Local-collection `Contains` (`ids.Contains(x.Id)`) | Supported | Null-aware: emits `(col IN (...) OR col IS NULL)` when the list contains nulls. |
| `Guid.Empty` and other static-field constants in predicates | Supported | Evaluated at execution time via the static-member constant path. |
| `Guid.NewGuid()` in queries | Unsupported | Generate the value in CLR before composing the query. |
| `NormFunctions.Like(value, pattern)` | Supported | Emits `(value LIKE pattern)` verbatim — no automatic LIKE-pattern escaping unlike `Contains`/`StartsWith`/`EndsWith`. |

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
