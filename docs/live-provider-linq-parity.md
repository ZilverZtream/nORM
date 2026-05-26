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
  DateTimeOffset.LocalDateTime, second-resolution for DTO instant comparison).
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
| `Skip` / `Take` (pagination) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SqlServer uses `OFFSET … FETCH NEXT … ROWS`; Postgres/MySQL use `LIMIT/OFFSET`. | `ProviderParityQueryPagingTests` |
| `Reverse` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Flips active ORDER BY. | `LinqReverseAndLastTests` (shape), `LiveProviderShapeParityTests` |
| `Distinct` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderShapeParityTests`, `LinqGuidAndDistinctTests` |

### Terminal operators

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `First` / `FirstOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `TerminalOperatorParityTests` |
| `Single` / `SingleOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `TerminalOperatorParityTests` |
| `Last` / `LastOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Requires explicit OrderBy. | `TerminalOperatorParityTests`, `LinqReverseAndLastTests` |
| `ElementAt` / `ElementAtOrDefault` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `TerminalOperatorParityTests` |
| `Any` / `All` (predicate + parameterless) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | `AnyAsync` on SQLite known mismatch — use `CountAsync() > 0`. | `TerminalOperatorParityTests` |
| `Contains` (column-in-collection + collection-in-row) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Null collection element split into `IS NULL` branch. | `TerminalOperatorParityTests` |
| `Count` / `LongCount` (parameterless + predicate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `TerminalOperatorParityTests` |

### Aggregates

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Sum` / `Average` / `Min` / `Max` (scalar + selector) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Decimal coerced to REAL on SQLite — small float drift. | `LinqGroupAggregateComputedSelectorTests`, `LiveProviderShapeParityTests` |
| `Sum`/`Min`/`Max`/`Avg` over decimal column | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SQLite: REAL coercion. | `LiveProviderShapeParityTests` |
| `Sum`/`Min`/`Max`/`Avg` over nullable columns | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Throws "no elements" when result is null & TResult is non-nullable value (commit 57). | `LinqOperatorCardinalityTests` |
| LINQ `Aggregate` sum-fold (1-arg + seed) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Lowers to synthesised `Sum(selector)` via Select-peel rewrite. | `LinqAggregateOperatorTests`, `LiveProviderRecentScvParityTests` |
| LINQ `Aggregate` min/max-fold (Math.Max/Min + Conditional) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Lowers to `Max(selector)` / `Min(selector)`; seed acts as ceiling/floor. | `LinqAggregateMinMaxFoldTests`, `LiveProviderRecentScvParityTests` |
| LINQ `Aggregate` string-concat fold (simple + seed-aware separator) | ✅ | ✅ | ✅ | ✅ | ✅ | — | SQL Server/Postgres/MySQL use native ordered aggregate (WITHIN GROUP / inline ORDER BY). SQLite uses outer ORDER BY to guide index scan order for GROUP_CONCAT. | `LinqAggregateStringConcatTests`, `LiveProviderRecentScvParityTests` |

### GroupBy / joins / set ops

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `GroupBy` single key | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderShapeParityTests`, `LinqGroupByProjectionTests` |
| `GroupBy` composite anon key | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqCompositeGroupByTests`, `LinqGroupMultiAggregateTests` |
| `GroupBy` HAVING (`Where(g => agg)`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqHavingTests` |
| `Join` inner | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderShapeParityTests`, `CompiledJoinDiagnosticTest` |
| `GroupJoin` simple key | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Bounded by `MaxGroupJoinSize`. | `GroupJoinTests`, `GroupJoinCompiledMaterializerTests` |
| `SelectMany` cross / nav-join / nav + DefaultIfEmpty / query-syntax left join | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Bare-MemberAccess selector covered by afef68a. | `SelectManyTests`, `LinqLeftJoinTests`, `LinqCrossJoinTests` |
| `Union` / `Intersect` / `Except` / `Concat` (incl. ordered/paged tail) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Trailing `Where` wraps as derived table. | `QueryTranslatorCrossProviderTests`, `LinqSetOpCompositionTests` |

### Post-Take/Skip family (silent-wrongness pins)

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Where` after `Take/Skip` (derived-table wrap) | ✅ | ✅ | ✅ | ✅ | ✅ | — | All 13 op groups follow same wrap pattern. | `LinqAfterTakeSkip*`, `LiveProviderPostTakeSkipParityTests` |
| `OrderBy` / `Distinct` / `Reverse` / Set ops / `Count` / `First`/`Single`/`Last`/`ElementAt` / `SelectMany` / `Join`/`GroupJoin` / `GroupBy` / direct aggregate after Take/Skip | ✅ | ✅ | ✅ | ✅ | ✅ | — | Same wrap mechanism. | `LinqAfterTakeSkip*`, `LinqGroupByAfterWindow*`, `LiveProviderPostTakeSkipParityTests` |

### Type-specific surfaces

| Feature | SQLite | SqlServer | Postgres | MySQL | Runtime | Compiled | Caveats | Tests |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `string` methods (Contains, StartsWith, EndsWith, Substring, Trim*, Concat, Compare, ToUpper/Lower, char.ToUpperInvariant) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LiveProviderShapeParityTests`, `TypeConversionParityTests` |
| `DateTime` arithmetic, parts, AddDays/Months/Years/Hours/Minutes/Seconds | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `ProviderParityDepthTests` |
| `DateOnly` / `TimeOnly` arithmetic + parts | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `ProviderParityDepthTests` |
| `TimeSpan` column ops (Total*, components, Negate/Duration/unary-negate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqTimeSpanInstanceNegateAndAbsTests` (SQLite); cross-provider via `ProviderParityDepthTests` |
| `DateTime` - `DateTime` → TimeSpan | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Julianday delta on SQLite (sub-second precision). | `ProviderParityDepthTests` |
| `DateTimeOffset.UtcDateTime` / `.LocalDateTime` / `.ToOffset` | ✅ (Local: ⚠️ DST snapshot) | ✅ (Local: ⚠️ DST snapshot) | ✅ (Local: ⚠️ DST snapshot) | ✅ (Local: ⚠️ DST snapshot) | ✅ | — | LocalDateTime uses snapshot offset, not per-instant historical TZ. | `LinqDateTimeOffsetLocalDateTimeTests`, `LinqDateTimeOffsetLocalDateTimeInWhereTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTimeOffset` col `==` / `!=` DateTime literal (UTC-instant equality) | ✅ | ✅ | ✅ | ✅ | ✅ | — | Second-resolution; sub-second fidelity is future work. MySQL hook uses `TIMESTAMPDIFF(SECOND,'1970-01-01',col)` to avoid session-TZ dependence of `UNIX_TIMESTAMP`. | `LinqDateTimeOffsetEqualsDateTimeLiteralTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTimeOffset` col >/</>=/<= DateTime literal | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Existing `NormalizeDateTimeForCompare` covers it. | `ProviderParityDepthTests` |
| `DateTimeOffset` - `DateTimeOffset` → TimeSpan | ✅ | ✅ | ✅ | ✅ | ✅ | — | Integer epoch-seconds diff for second-resolution accuracy. | `LinqDateTimeOffsetColumnSubtractionTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `DateTimeOffset` col +/- `TimeSpan` col | ✅ | ✅ | ✅ | ✅ | ✅ | — | SQLite preserves original offset via substr-suffix hook. | `LinqDateTimeOffsetPlusTimeSpanColumnTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `decimal` comparisons, ordering, aggregates, distinct, set ops | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | SQLite REAL coercion documented. | `TypeConversionParityTests`, `LiveProviderShapeParityTests` |
| `Guid` comparisons, equality, IN | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqGuidAndDistinctTests`, `TypeConversionParityTests` |
| `enum` comparisons, ToString, Parse, IsDefined, HasFlag | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqEnumAndConditionalTests`, `LinqEnumToStringTests` |
| `Enum.TryParse<T>(col, out _)` parseability check | ✅ | ✅ | ✅ | ✅ | ✅ | — | Lowers to `(col IN ('Name1', ...))`. | `LinqEnumTryParseOutParamTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `Convert.ChangeType(col, typeof(T))` | ✅ | ✅ | ✅ | ✅ | ✅ | — | Type-constant second arg pattern-matched; runtime-variable target type unsupported. | `LinqConvertChangeTypeOnColumnTests` (SQLite), `LiveProviderRecentScvParityTests` |
| `bool?` / nullable predicates (three-valued logic) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — | `LinqPagingAndNullableBoolTests` |
| local collection `Contains` (incl. nulls) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | Null-bearing collection expands to `(col IN (…) OR col IS NULL)`. | `LiveProviderShapeParityTests` |

### Compiled-query parity

| Feature | SQLite | SqlServer | Postgres | MySQL | Tests |
| --- | --- | --- | --- | --- | --- |
| Compiled-query equivalent of every runtime row above | ✅ for the existing matrix; 🚧 for SCV additions in 2026-05-25/26. | | | | `CompileTimeQueryParameterParityTests`, `LinqCompiledQueryExpandedParityTests`, `CompiledQuerySqlShapeParityTests` |

## Open parity gaps (🚧)

No open 🚧 rows remain in the matrix above. Every row is either ✅ (passes on all
four live providers) or ⚠️ (passes with a documented deterministic caveat).

The compiled-query row carries a note that SCV-additions features are intentionally
marked `—` in the Compiled column — their query shape is not supported in the
compiled-query path, which is consistent with their individual row entries.

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
