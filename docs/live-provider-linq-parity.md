# Live Provider LINQ Parity

This report tracks live (real-database) parity for the frozen v1 LINQ matrix
across SQL Server, PostgreSQL, MySQL, and SQLite. It complements
`linq-support.md` (the contract) and `linq-support-coverage.md` (the test
anchors) by recording **observed live-provider behaviour**, not just translator
shape.

## Scope

Per the freeze: nORM v1 does not add new LINQ surface area beyond what
`linq-support.md` documents. This report drives the release gate: a row is
v1-green only when it passes live on every configured provider.

## Result legend

- ✅ — passes runtime + (where applicable) compiled-query parity on the live
  provider, with row values, ordering, and null semantics matching .NET / the
  documented provider note.
- ⚠️ — passes with a documented caveat (e.g. snapshot DST for
  DateTimeOffset.LocalDateTime, millisecond-resolution for DTO instant comparison).
- ❌ — fails on the live provider; see the linked test or commit.
- 🚧 — coverage gap; the SQLite probe passes but no live-parity test exists yet.
- — — not applicable to this provider (e.g. provider-specific dialect).

The runtime/compiled distinction matters because nORM has separate paths
(`NormQueryProvider.ExecuteSync` / `ExpressionCompiler.CompileQuery`). A row
passes only when both forms agree.

## Priority matrix

The report is sorted by ship-priority within each section. Where ✅ rows
already exist, the linked file is the live-parity test that backs the claim.

### Base operators

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Where` predicates | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderShapeParityTests`, `ProviderParityMandatoryTests` |
| `Select` (entity / scalar / DTO / anonymous / `new T { … }`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderIntegrationTests`, `LiveProviderShapeParityTests` |
| `OrderBy` / `OrderByDescending` / `ThenBy` / `ThenByDescending` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `ProviderParityQueryPagingTests`, `LiveProviderShapeParityTests` |
| `Skip` / `Take` (pagination) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SqlServer uses `OFFSET … FETCH NEXT … ROWS`; Postgres/MySQL use `LIMIT/OFFSET`. | `ProviderParityQueryPagingTests`, `LiveProviderSkipTakeParityTests` |
| `Reverse` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Flips active ORDER BY. | `LinqReverseAndLastTests` (shape), `LiveProviderShapeParityTests` |
| `Distinct` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderShapeParityTests`, `LinqGuidAndDistinctTests` |
| `DefaultIfEmpty` standalone | ✅ | ✅ | ✅ | ✅ | ✅ | — | Post-materialization contract: non-empty sources unchanged; empty sources return one null/default element. Left-join DefaultIfEmpty is covered separately. | `LinqDefaultIfEmptyTests`, `LiveProviderDefaultIfEmptyParityTests` |

### Terminal operators

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `First` / `FirstOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderTerminalOpParityTests`, `TerminalOperatorParityTests` |
| `Single` / `SingleOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderTerminalOpParityTests`, `TerminalOperatorParityTests` |
| `Last` / `LastOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Requires explicit OrderBy; flips ORDER BY direction to pick final row. | `LiveProviderTerminalOpParityTests`, `TerminalOperatorParityTests`, `LinqReverseAndLastTests` |
| `ElementAt` / `ElementAtOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Uses `OFFSET N ROWS FETCH NEXT 1 ROWS ONLY` (SqlServer) or `LIMIT 1 OFFSET N` (others). | `LiveProviderTerminalOpParityTests`, `TerminalOperatorParityTests` |
| `Any` / `All` (predicate + parameterless) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderTerminalOpParityTests`, `TerminalOperatorParityTests` |
| `Contains` (column-in-collection + collection-in-row) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Null collection element split into `IS NULL` branch. | `TerminalOperatorParityTests`, `LiveProviderContainsParityTests` |
| `Count` / `LongCount` (parameterless + predicate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderTerminalOpParityTests`, `TerminalOperatorParityTests` |
| `MinBy` / `MaxBy` | ✅ | ✅ | ✅ | ✅ | ✅ | — | ORDER BY key ascending/descending plus one-row terminal read; empty source throws. | `LinqMinByMaxByTests`, `LiveProviderMinByMaxByParityTests` |

### Aggregates

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Sum` / `Average` / `Min` / `Max` (scalar + selector) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Decimal coerced to REAL on SQLite — small float drift. | `LinqGroupAggregateComputedSelectorTests`, `LiveProviderShapeParityTests` |
| `Sum`/`Min`/`Max`/`Avg` over decimal column | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SQLite: REAL coercion. | `LiveProviderShapeParityTests` |
| `Sum`/`Min`/`Max`/`Avg` over nullable columns | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Throws "no elements" when result is null & TResult is non-nullable value (commit 57). | `LinqOperatorCardinalityTests`, `LiveProviderNullableAggregateParityTests` |
| LINQ `Aggregate` sum-fold (1-arg + seed) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Lowers to synthesised `Sum(selector)` via Select-peel rewrite. | `LinqAggregateOperatorTests`, `LiveProviderRecentScvParityTests` |
| LINQ `Aggregate` min/max-fold (Math.Max/Min + Conditional) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Lowers to `Max(selector)` / `Min(selector)`; seed acts as ceiling/floor. | `LinqAggregateMinMaxFoldTests`, `LiveProviderRecentScvParityTests` |
| LINQ `Aggregate` string-concat fold (simple + seed-aware separator) | ✅ | ✅ | ✅ | ✅ | ✅ | — | SQL Server/Postgres/MySQL use native ordered aggregate (WITHIN GROUP / inline ORDER BY). SQLite uses outer ORDER BY to guide index scan order for GROUP_CONCAT. | `LinqAggregateStringConcatTests`, `LiveProviderRecentScvParityTests` |
| LINQ `Aggregate` conditional-fold (`acc + (cond ? weight : 0)`) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Lowers to `SUM(CASE WHEN cond THEN weight ELSE 0 END)` via the existing Add-pattern rewrite. | `LinqAggregateConditionalFoldTests`, `LiveProviderRecentScvParityTests` |

### GroupBy / joins / set ops

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `GroupBy` single key | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Raw `GroupBy(key).ToListAsync()` returns `IGrouping<K,V>` via client-side grouping after provider fetch; aggregate projections remain server-side SQL GROUP BY. | `LiveProviderGroupByParityTests`, `LinqGroupByProjectionTests` |
| `GroupBy` composite anon key | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderGroupByParityTests`, `LinqCompositeGroupByTests`, `LinqGroupMultiAggregateTests` |
| `GroupBy` HAVING (`Where(g => agg)`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderGroupByParityTests`, `LinqHavingTests` |
| `Join` inner | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderJoinSelectManyParityTests`, `CompiledJoinDiagnosticTest` |
| `GroupJoin` simple key | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Bounded by `MaxGroupJoinSize`. `CommandBehavior.SequentialAccess` replaced with `Default` to avoid backward-seek on Npgsql when innerKeyIndex > first inner col. | `LiveProviderJoinSelectManyParityTests`, `GroupJoinTests`, `GroupJoinCompiledMaterializerTests` |
| `SelectMany` cross / nav-join / nav + DefaultIfEmpty / query-syntax left join / correlated expansion | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Bare-MemberAccess selector covered by afef68a. Correlated query expansion lowers to INNER JOIN with the correlated predicate in ON. | `LiveProviderJoinSelectManyParityTests`, `SelectManyTests`, `LinqLeftJoinTests`, `LinqCrossJoinTests`, `LinqCorrelatedSelectManyTests` |
| `ExecuteUpdateAsync` / `ExecuteDeleteAsync` filtered and join-sourced, including composite PK | ✅ | ✅ | ✅ | ✅ | ✅ | — | Composite-PK join-source CUD uses row-tuple IN on SQLite/Postgres/MySQL and a JOIN target form on SQL Server. Grouped/ordered/distinct/paged/aggregated CUD remains rejected. | `LiveProviderCompositePkBulkCudParityTests`, `ExecuteDeleteUpdateJoinSourceTests`, `LinqCompositePkExecuteCudTests` |
| `Union` / `Intersect` / `Except` / `Concat` (incl. ordered/paged tail) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Trailing `Where` wraps as derived table. `Intersect`/`Except` require MySQL 8.0+; nORM's `MySqlProvider` gate blocks 5.x. | `LiveProviderSetOpParityTests`, `QueryTranslatorCrossProviderTests`, `LinqSetOpCompositionTests` |
| `OfType<T>` / `Cast<T>` identity plus TPH derived filter | ✅ | ✅ | ✅ | ✅ | ✅ | — | Constrained to identity/base casts and `[DiscriminatorColumn]` + `[DiscriminatorValue]` TPH derived filters. Unsupported downcasts throw deterministically. | `LiveProviderOfTypeParityTests`, `LinqCastOfTypeTests`, `LinqUnsupportedShapeContractTests` |

### Post-Take/Skip family (silent-wrongness pins)

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Where` after `Take/Skip` (derived-table wrap) | ✅ | ✅ | ✅ | ✅ | ✅ | — | All 13 op groups follow same wrap pattern. | `LinqAfterTakeSkip*`, `LiveProviderPostTakeSkipParityTests` |
| `OrderBy` / `Distinct` / `Reverse` / Set ops / `Count` / `First`/`Single`/`Last`/`ElementAt` / `SelectMany` / `Join`/`GroupJoin` / `GroupBy` / direct aggregate after Take/Skip | ✅ | ✅ | ✅ | ✅ | ✅ | — | Same wrap mechanism. | `LinqAfterTakeSkip*`, `LinqGroupByAfterWindow*`, `LiveProviderPostTakeSkipParityTests` |

### Type-specific surfaces

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `string` methods (Contains, StartsWith, EndsWith, Substring, Trim*, Concat, Compare, ToUpper/Lower, char.ToUpperInvariant, `string.Format`, interpolated projections) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | `string.Format` support is the documented constant-template subset from `linq-support.md`. | `LiveProviderShapeParityTests`, `TypeConversionParityTests`, `LiveProviderCharStringPredicateParityTests` |
| `char.IsDigit` / `char.IsLetter` / `char.IsWhiteSpace` on column character | ✅ | ✅ | ✅ | ✅ | ✅ | — | ASCII-range BETWEEN comparisons; Unicode characters outside ASCII are not matched. | `LiveProviderCharStringPredicateParityTests` |
| `string.IsNullOrEmpty` / `string.IsNullOrWhiteSpace` on nullable column | ✅ | ✅ | ✅ | ✅ | ✅ | — | `IsNullOrEmpty` uses `DATALENGTH(col) = 0` on SQL Server (trailing-space equality rule makes `'   ' = ''` TRUE on SQL Server). All others use `col = ''`. `IsNullOrWhiteSpace` uses `LTRIM(RTRIM(col)) = ''` on all providers. | `LiveProviderCharStringPredicateParityTests`, `LinqWhereStringIsNullOrEmptyTests` |
| `string.Length` comparison (`col.Length > N`) | ✅ | ✅ | ✅ | ✅ | ✅ | — | SQLite/Postgres: `LENGTH`; SQL Server: `LEN`; MySQL: `CHAR_LENGTH`. | `LiveProviderCharStringPredicateParityTests` |
| `DateTime` arithmetic, parts, AddDays/Months/Years/Hours/Minutes/Seconds | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `ProviderParityDepthTests` |
| `DateOnly` / `TimeOnly` arithmetic + parts | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `ProviderParityDepthTests` |
| `TimeSpan` column ops (Total*, components, Negate/Duration/unary-negate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqTimeSpanInstanceNegateAndAbsTests` (SQLite); cross-provider via `ProviderParityDepthTests` |
| `TimeSpan` col `<`/`>`/`<=`/`>=`/`==` col (cross-column ordering) | ✅ | ✅ | ✅ | ✅ | ✅ | — | SQLite TEXT lex-ordering is wrong for multi-day; `NormalizeTimeSpanForCompare` converts both sides to fractional seconds. SQL Server/Postgres/MySQL use native types. | `LinqTimeSpanColumnComparisonTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTime` - `DateTime` → TimeSpan | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SQLite uses julianday REAL arithmetic; assertions allow 1ms tolerance. SQL Server/MySQL use microsecond diff hooks. | `ProviderParityDepthTests`, `LiveProviderDateTimeSubtractionPrecisionTests` |
| `DateTimeOffset.UtcDateTime` / `.LocalDateTime` / `.ToOffset` | ✅ (Local: ⚠️ DST snapshot) | ✅ (Local: ⚠️ DST snapshot) | ✅ (Local: ⚠️ DST snapshot) | ✅ (Local: ⚠️ DST snapshot) | ✅ | — | LocalDateTime uses snapshot offset, not per-instant historical TZ. | `LinqDateTimeOffsetLocalDateTimeTests`, `LinqDateTimeOffsetLocalDateTimeInWhereTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTimeOffset` col `==` / `!=` DateTime literal (UTC-instant equality) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Millisecond-resolution; full .NET 100ns tick equality is not portable. MySQL hook uses `TIMESTAMPDIFF(MICROSECOND,'1970-01-01',col) / 1000` to avoid session-TZ dependence of `UNIX_TIMESTAMP`. | `LinqDateTimeOffsetEqualsDateTimeLiteralTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTimeOffset` col >/</>=/<= DateTime literal | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Existing `NormalizeDateTimeForCompare` covers it. | `ProviderParityDepthTests` |
| `DateTimeOffset` - `DateTimeOffset` → TimeSpan | ✅ | ✅ | ✅ | ✅ | ✅ | — | Epoch-millisecond diff; SQLite assertions allow 1ms tolerance. | `LinqDateTimeOffsetColumnSubtractionTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTimeOffset` col +/- `TimeSpan` col | ✅ | ✅ | ✅ | ✅ | ✅ | — | SQLite preserves original offset via substr-suffix hook. | `LinqDateTimeOffsetPlusTimeSpanColumnTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `decimal` comparisons, ordering, aggregates, distinct, set ops | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SQLite REAL coercion documented. | `TypeConversionParityTests`, `LiveProviderShapeParityTests` |
| `Guid` comparisons, equality, IN | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqGuidAndDistinctTests`, `TypeConversionParityTests` |
| `enum` comparisons, ToString, Parse, IsDefined, HasFlag | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqEnumAndConditionalTests`, `LinqEnumToStringTests`, `LiveProviderEnumParityTests` |
| `Enum.TryParse<T>(col, out _)` parseability check | ✅ | ✅ | ✅ | ✅ | ✅ | — | 2-arg lowers to a provider-forced case-sensitive enum-name comparison; 3-arg constant/captured `ignoreCase` lowers to `LOWER(col) IN (...)`. | `LinqEnumTryParseOutParamTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `Convert.ChangeType(col, typeof(T))` | ✅ | ✅ | ✅ | ✅ | ✅ | — | Type-constant second arg pattern-matched; runtime-variable target type unsupported. | `LinqConvertChangeTypeOnColumnTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `bool?` / nullable predicates (three-valued logic) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqPagingAndNullableBoolTests`, `LiveProviderNullableBoolParityTests` |
| local collection `Contains` (incl. nulls) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Null-bearing collection expands to `(col IN (…) OR col IS NULL)`. | `LiveProviderContainsParityTests` |

### Compiled-query parity

| Feature | SQLite | SqlServer | Postgres | MySQL | Tests |
| --- | --- | --- | --- | --- | --- |
| Compiled-query rows marked ✅ in the matrix above | ✅ | ✅ | ✅ | ✅ | `LiveProviderCompiledQueryParityTests`, `CompileTimeQueryParameterParityTests`, `LinqCompiledQueryExpandedParityTests`, `CompiledQuerySqlShapeParityTests` |

Rows marked `—` in the Compiled column are runtime-only v1 contracts, not
implicit compiled-query claims. Coverage rows are not substitutes for live
provider gates; provider-neutral tests must be paired with SQL Server, SQLite,
PostgreSQL, and MySQL evidence before a row is promoted to v1-green.

## Parity gap tracking

No open 🚧 rows remain in the matrix above. Every row is either ✅ (passes on all
four live providers) or ⚠️ (passes with a documented deterministic caveat).

Rows with `—` in the Compiled column are intentionally excluded from the v1
compiled-query contract. If one of those shapes is promoted later, add a focused
compiled-query live-provider test and flip the row in the same commit.

When a future addition creates a new 🚧, add it here with a root-cause note and
flip it to ✅/⚠️ in the same commit once the live tests pass.

## Triage policy

When a live-parity test fails, classify into one of:

1. **Translator bug** — fix in `Query/` and add a regression test.
2. **Provider SQL bug** — fix the provider's override (`Providers/*Provider.cs`).
3. **Materialisation / conversion bug** — fix in `MaterializerFactory` /
   `ParameterManager`.
4. **Provider semantic difference** — document in this report's "Caveats"
   column with the deterministic behaviour we ship. Don't broaden SCV to mask it.
5. **Accidentally-included unsupported shape** — narrow `linq-support.md` and
   delete the row here.

## Release gate

The v1 gate (`eng/v1-release-gate.ps1`) auto-sets `NORM_REQUIRE_LIVE_PARITY=any`
when any `NORM_TEST_*` env var is configured. Gate-green means: every ✅ row
above passes on every configured live provider AND every 🚧 has been resolved
(either flipped to ✅/⚠️ or removed from the supported matrix).

Provider-shape-only coverage does NOT count as live parity for the gate.
