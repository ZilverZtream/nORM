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
| `Select` with custom client logic | Constrained | Controlled by `DbContextOptions.ClientEvaluationPolicy`. The v1 default is `Throw`; `Warn` logs and allows the projection tail after server materialization, and `Allow` permits it silently. User-defined helper methods fall into this path. |
| `OrderBy`, `ThenBy` | Supported | Including the `Descending` variants. Provider-specific identifier escaping and expression translation apply. |
| `Reverse` | Supported | Flips the active ORDER BY direction at the SQL layer. |
| `Skip`, `Take` | Supported | Provider-specific paging. `Skip(m).Take(n)` is the canonical pagination shape. `Take(n).Skip(m)` works with both literal counts (rewritten algebraically to `Skip(m).Take(n - m)`) and runtime parameters (emits `LIMIT (@take - @skip) OFFSET @skip` so the runtime values still produce the [skip, take) window). |
| `Distinct` | Supported | Applies to the projected SQL shape. |
| `Count`, `LongCount`, `Any`, `All` | Supported | Includes predicate overloads and short-circuiting AND/OR. |
| Navigation aggregates: `parent.Children.Any(...)`, `.All(...)`, `.Count()`, `.LongCount()` | Supported | Emit correlated `EXISTS` / `NOT EXISTS` / scalar `(SELECT COUNT(*) ...)` subqueries against the dependent table. Two-hop chains (`parent.Children.SelectMany(c => c.GrandChildren).Count()`) emit a nested `IN` subquery. |
| `Sum`, `Average`, `Min`, `Max` | Supported | Both top-level aggregate queries and grouped aggregates, including computed selectors like `g.Sum(x => x.Price * x.Qty)` and conditional selectors like `g.Sum(x => x.Active ? x.Score : 0)`. |
| `GroupBy` | Supported | Single-column and composite anonymous-type keys (`p => new { p.A, p.B }`) supported. Aggregate projections (`g.Key`, `g.Count()`, `g.Sum(x => x.Amount)`, etc.) run server-side with SQL GROUP BY. Raw streaming (`GroupBy(key).ToListAsync()` → `List<IGrouping<K, V>>`) performs client-side grouping in memory after fetching all rows. |
| Inner joins | Supported | `Join` translates to provider SQL and is included in benchmark parity gates. |
| Group joins | Supported | Simple key joins, bounded by `DbContextOptions.MaxGroupJoinSize`. Supports `AsAsyncEnumerable` streaming (groups are yielded one at a time). The `GroupJoin` + `SelectMany` + `DefaultIfEmpty` (query-syntax left join) composition is fully supported, including projections that contain idiomatic null-guards (`c == null ? null : c.Prop` / `c != null ? c.Prop : null`) — the translator strips the redundant guard since LEFT JOIN already NULLs unmatched rows — and non-null fallback variants (`c == null ? "None" : c.Prop` / `c != null ? c.Prop : "None"`) which are lowered to `COALESCE(c.Prop, 'None')`. |
| `SelectMany` | Supported | Cross-product (`from a in A from b in B select new { a.X, b.Y }` → `CROSS JOIN`), navigation inner-join (with optional `Where` filter on the navigation), navigation `+ DefaultIfEmpty()` (→ `LEFT JOIN`), query-syntax left join (`GroupJoin + SelectMany + DefaultIfEmpty` → `LEFT JOIN`), and correlated expansion (`p => ctx.Query<Child>().Where(c => c.FK == p.PK)` → `INNER JOIN ... ON [predicate]`, including compound predicates). Result selectors (transparent identifier projections) work for all join forms. |
| Set operations: `Union`, `Intersect`, `Except` | Supported | Each side must project the same row shape. `Concat` lowers to `UNION ALL` under the same shape rule. `OrderBy` / `OrderByDescending` / `Skip` / `Take` AFTER the set op work (the trailing clauses apply to the unified result). `Where` AFTER the set op also works — the translator wraps the set-op SQL as `SELECT * FROM (<inner>) AS T0` so the predicate filters the unified rows. |
| `Include`, `ThenInclude` | Supported | Supported for mapped navigation paths including composite-key dependents, multi-level chains, and tenant-filtered contexts. Use with `AsSplitQuery()` to trigger eager loading. Unmapped navigation properties throw `NormUnsupportedFeatureException`. |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | Supported | These are nORM-specific query modifiers with dedicated tests. |
| Raw SQL composition | Constrained | `FromSqlRawAsync<T>` and `FromSqlInterpolatedAsync<T>` execute read-only `SELECT`/CTE statements through the provider-aware raw query gate. Raw SQL does not add tenant predicates automatically. |
| `AsAsyncEnumerable` | Supported | Streams ordinary queries and GroupJoin queries (groups yielded one at a time). Include queries are rejected because eager-load paths require a coordinated multi-round-trip fetch incompatible with row-by-row streaming. |
| `ExecuteUpdateAsync` | Supported | Filtered and join-sourced entity queries, including composite-primary-key entities. Grouped, ordered, distinct, paged, and aggregated queries are rejected. Join sources: single-PK → `WHERE pk IN (SELECT T0.pk ...)`, composite-PK → row-tuple `WHERE (pk1, pk2) IN (SELECT T0.pk1, T0.pk2 ...)` (SQLite/Postgres/MySQL) or JOIN-based form (SQL Server). Two assignment forms: (1) literal / captured-local value — `SetProperty(x => x.Foo, 42)`; (2) server-side computed expression — `SetProperty(x => x.Counter, x => x.Counter + 1)` supporting member access, constants, arithmetic (`+`/`-`/`*`/`/`/`%`), and Convert. |
| `ExecuteDeleteAsync` | Supported | Filtered and join-sourced entity queries, including composite-primary-key entities and correlated `EXISTS`/`NOT EXISTS` via `Where(p => !ctx.Query<Other>().Any(o => o.K == p.K))`. Grouped, ordered, distinct, paged, and aggregated queries are rejected. Join sources: single-PK → `WHERE pk IN (SELECT T0.pk ...)`, composite-PK → row-tuple `WHERE (pk1, pk2) IN (SELECT T0.pk1, T0.pk2 ...)` (SQLite/Postgres/MySQL) or JOIN-based form (SQL Server). |
| Arbitrary CLR methods in predicates | Unsupported | Unsupported methods/members throw from expression translation instead of being evaluated client-side in filters. |
| Arbitrary client evaluation before server filtering/paging | Unsupported | Server filters and paging must happen before any allowed client projection tail. |
| `TakeWhile` / `SkipWhile` | Unsupported | No SQL equivalent; throw deterministically rather than buffering the entire table client-side. |
| `OfType<T>` / `Cast<T>` | Constrained | Identity pass-through when `T` equals the source element type, or (for reference types) is a base class assignable from it. `OfType<TDerived>()` is also supported for TPH hierarchies: when `TDerived` carries `[DiscriminatorValue]` and its base type carries `[DiscriminatorColumn]`, the translator injects the discriminator `WHERE` predicate automatically — composing with further `Where(...)` calls works as expected. Downcasting to a type with no discriminator metadata still throws `NormUnsupportedFeatureException`. |
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
| `AsAsyncEnumerable` | Supported | See main query-operator matrix above. |

## Value translations

| Feature | Status | Notes |
| --- | --- | --- |
| `string` methods: `ToUpper`, `ToLower`, `Length`, `Trim`/`TrimStart`/`TrimEnd`, `Substring`, `Replace`, `IndexOf`, `Contains`, `StartsWith`, `EndsWith` | Supported | Provider-specific syntax. |
| `string` statics: `IsNullOrEmpty`, `IsNullOrWhiteSpace`, `Concat`, `Compare`, `CompareTo` | Supported | `Compare` / `CompareTo` emit a `CASE` returning `-1` / `0` / `1`. |
| `string.Format` / interpolated strings | Supported | Constant templates with positional placeholders lower to provider concatenation. Fixed numeric specs (`F<N>`), common date/time tokens (`yyyy`, `yy`, `MM`, `dd`, `HH`, `mm`, `ss`), and alignment are translated through provider hooks. Locale-aware/custom .NET formatting outside that subset remains client-projection work. |
| `Convert.ToInt32` / `ToInt64` / `ToString` / `ToBoolean` / `ToDouble` / `ToDecimal` / etc. | Supported | Emit a provider `CAST`. |
| `Math.Abs`, `Ceiling`, `Floor`, `Round`, `Sqrt`, `Pow`, `Exp`, `Log`, `Log10`, `Sign`, `Min`, `Max`, `Truncate` | Supported | Per-provider mapping (`LEAST`/`GREATEST` on PG/MySQL, `CASE` for SQL Server `Min`/`Max`, etc.). |
| `DateTime` members: `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `DayOfYear`, `DayOfWeek`, `Date` | Supported | Provider-specific extraction; `DayOfWeek` normalized to `System.DayOfWeek` (`0`=Sun..`6`=Sat) on every provider. |
| `DateTime.AddDays` / `AddMonths` / `AddYears` / `AddHours` / `AddMinutes` / `AddSeconds` | Supported | Provider-specific interval arithmetic. |
| `DateTime.UtcNow` / `DateTime.Now` / `DateTime.Today` in predicates | Supported | Evaluated as a bound parameter at execution time (static-member constant path). |
| `DateTimeOffset` members | Supported | Same member surface as `DateTime`; materializer parses SQLite TEXT into `DateTimeOffset`. |
| `DateOnly.Year` / `Month` / `Day` / `DayOfYear` | Supported | Per provider. |
| `TimeOnly.Hour` / `Minute` / `Second` | Supported | Per provider. |
| `TimeSpan` member access on a stored TimeSpan column (`r.Duration.TotalSeconds`, `.Days`, etc.) | Supported | `TotalSeconds`, `TotalMinutes`, `TotalHours`, `TotalDays`, `TotalMilliseconds`, `Days`, `Hours`, `Minutes`, `Seconds` in `Where` and `Select`. Provider-specific seconds extraction via `GetTimeSpanColumnSecondsSql` (SQLite TEXT 'c'-format parse, SQL Server `DATEDIFF_BIG(MICROSECOND)`, PostgreSQL `EXTRACT(EPOCH)`, MySQL `TIME_TO_SEC`). |
| `DateTime`/`DateTimeOffset` subtraction TimeSpan members (`(r.End - r.Start).TotalHours`, `.TotalDays`, `.TotalSeconds`, `.TotalMinutes`, `.TotalMilliseconds`, `.Days`, `.Hours`, `.Minutes`, `.Seconds`) | Supported | Provider lowers via `julianday()*86400` on SQLite, `DATEDIFF_BIG(MICROSECOND) / 1000000.0` on SQL Server, `EXTRACT(EPOCH FROM ...)` on PostgreSQL, and `TIMESTAMPDIFF(MICROSECOND) / 1000000.0` on MySQL. Total* members emit fractional values; the integer-component members (`Days`/`Hours`/`Minutes`/`Seconds`) match System.TimeSpan semantics (truncate toward zero with modular wrap). |
| `Nullable<T>.HasValue`, `Value`, `GetValueOrDefault()` / `GetValueOrDefault(fallback)` | Supported | Emit `IS NOT NULL`, the operand itself, and `COALESCE(..., fallback)` respectively. |
| `Convert.ChangeType(col, typeof(T))` with `Type` constant | Supported | Pattern-matched at build time; emits `CAST(col AS X)` for int/short/byte/sbyte/long/string/double/float/decimal/bool. Runtime-variable target type unsupported (no expression-tree dispatch). |
| `DateTimeOffset.LocalDateTime` accessor (projection / WHERE / OrderBy) | Constrained | Snapshot semantics: `TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow)` captured at query-build time. Does NOT use per-instant historical TZ rules, so a single query crossing a DST boundary may differ from .NET's `LocalDateTime` by one hour on the wrong-side rows. SQL has no portable way to invoke .NET's TZ rules per row without provider-specific TZ database integration. |
| `DateTimeOffset` col `==` / `!=` DateTime literal | Constrained | Lowered to UTC-instant equality via per-provider epoch-millisecond hooks. Millisecond-resolution: full .NET 100ns tick equality is not portable across SQLite TEXT/date functions and native provider precisions. `.Kind == Utc` literals get offset 0; Unspecified / Local literals use the snapshot local offset. |
| `DateTimeOffset` - `DateTimeOffset` → `TimeSpan` (cross-column) | Supported | UTC-epoch-millisecond difference. Independent of either column's stored offset. Materialiser reads as REAL seconds via `TimeSpan.FromSeconds`; SQLite assertions allow 1ms tolerance. |
| `DateTimeOffset` col `+` / `-` `TimeSpan` col → `DateTimeOffset` | Supported | SQLite preserves the original offset suffix via `substr(col, 20)` extraction. Other providers use native `DATETIMEOFFSET` / `TIMESTAMPTZ` arithmetic which keeps the offset natively. |
| LINQ `Aggregate` sum-fold (1-arg + seed forms) | Supported | `(acc, x) => acc + x` lowers to `Sum(selector)` server-side. 2-arg overload adds the seed in CLR after the SQL sum. 1-arg form throws on empty source per .NET semantics. |
| LINQ `Aggregate` min/max-fold (Math.Max/Min + Conditional shapes) | Supported | Recognised shapes: `Math.Max(acc, x)`, `Math.Min(acc, x)`, `(acc, x) => x > acc ? x : acc` (and mirrored / `>=` / `<` / `<=` variants). Lowers to `Max(selector)` / `Min(selector)` server-side. 3-arg seed acts as a ceiling/floor. |
| LINQ `Aggregate` string-concat fold (simple + seed-aware separator) | Supported | `(acc, x) => acc + sep + x` and `(acc, x) => acc + (acc == "" ? "" : sep) + x` both lower to `GroupBy(_ => 1).Select(g => string.Join(sep, g.Select(...)))` so the provider's `STRING_AGG` / `GROUP_CONCAT` runs on the server. Empty source returns seed (3-arg) or throws (1-arg). |
| `Enum.TryParse<T>(stringCol, out T)` as WHERE predicate | Supported | Lowers to a case-sensitive provider comparison against enum names; the 3-arg `ignoreCase` overload lowers through `LOWER(col) IN (...)` when `ignoreCase` is a constant or captured bool. The out variable is a syntax requirement (expression trees disallow `out _`) but is unused at the SQL level. The richer "TryParse + use out-var in same predicate" shape is not a parseability check; use `col == nameof(X)` instead. |
| Conditional expressions (`cond ? a : b`) in `Where` and `Select` | Supported | Emit `CASE WHEN ... END`. Nested conditionals supported. |
| Arithmetic operators (`+`, `-`, `*`, `/`, `%`) in `Where`, `Select`, and aggregate selectors | Supported | Column types drive numeric semantics. |
| Enum equality and `(int)enumCol` projection | Supported | Enum values are emitted as their underlying integer; cast collapses to the operand. |
| Enum `.ToString()` in projection | Supported | Emits a provider-neutral `CASE` expression mapping defined enum values to names; undefined values fall back to the numeric text representation, matching .NET `Enum.ToString()`. |
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
