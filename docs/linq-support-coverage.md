# LINQ Support Coverage

This file anchors `docs/linq-support.md` to executable coverage. Every
Supported, Constrained, Provider-bound compatibility, or Preview matrix row must have at least one test file
listed here. Unsupported rows may be documented without a coverage row only when
the matrix notes an explicit deterministic failure contract.

Coverage rows are test anchors, not a substitute for provider parity evidence.
Rows that are part of the v1 cross-provider contract must also appear in
`docs/live-provider-linq-parity.md` with SQLite, SQL Server, PostgreSQL, and
MySQL evidence before they are treated as release-green.

## Query operators

| Feature | Coverage |
| --- | --- |
| `Where` predicates | `tests/SqlTranslationTests.cs`, `tests/CrossProviderBehaviorTests.cs`, `tests/AdversarialMultiShapeStressTests.cs`, `tests/LinqConversionAndCompareTests.cs`, `tests/LinqEnumAndConditionalTests.cs` |
| `Select` entity and scalar projections | `tests/MaterializerAndExecutorDeepCoverageTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Select` into anonymous types | `tests/LinqEnumAndConditionalTests.cs`, `tests/LinqGroupByProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` into DTO with positional record | `tests/LinqDtoProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` into DTO with parameterized class constructor | `tests/LinqDtoProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` into DTO with init-only / writable property setters (`new T { A = x.A }`) | `tests/LinqDtoProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs`, `tests/ProviderMobilityStrictCommonSurfaceTests.cs`, `tests/LiveProviderJoinSelectManyParityTests.cs` |
| `Select` with custom client logic | `tests/ClientEvaluationPolicyTests.cs`, `tests/QueryExecutorExtendedCoverageTests.cs`, `tests/LinqClientProjectionTests.cs`, `tests/LiveProviderClientEvaluationParityTests.cs` |
| `OrderBy`, `ThenBy` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Reverse` | `tests/LinqReverseAndLastTests.cs` |
| `Skip`, `Take` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqPagingAndNullableBoolTests.cs`, `tests/LiveProviderSkipTakeParityTests.cs`, `tests/LinqWhereAfterTakeTests.cs`, `tests/LinqOrderByAfterTakeTests.cs` (operators after windows, any spine shape), `tests/LiveProviderWindowedChainParityTests.cs` (windowed-chain wraps on all providers) |
| `TakeLast`, `SkipLast` | `tests/LinqSkipLastTakeLastTranslatabilityTests.cs`, `tests/LinqCompiledTailPagingAndKeyedSetOpsTests.cs`, `tests/TailPagingAfterWindowTests.cs` (tail paging after a `Take`/`Skip` window, incl. filtered windows) |
| `TakeWhile` / `SkipWhile` | `tests/LinqTakeSkipWhileProviderMobileTests.cs`, `tests/LiveProviderTakeSkipWhileParityTests.cs`, `tests/LinqUnsupportedShapeContractTests.cs`, `tests/TakeSkipWhileAfterWindowTests.cs` (while operators after a `Take`/`Skip` window) |
| `SequenceEqual` | `tests/LinqSequenceEqualProviderMobileTests.cs`, `tests/LiveProviderSequenceEqualParityTests.cs`, `tests/LinqUnsupportedShapeContractTests.cs` |
| `Distinct` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs`, `tests/LinqGuidAndDistinctTests.cs` |
| `DistinctBy` | `tests/DistinctByProviderShapeTests.cs`, `tests/LinqDistinctByImplementationTests.cs`, `tests/LinqDistinctByDecimalColumnTests.cs`, `tests/LiveProviderDistinctByParityTests.cs` |
| `DefaultIfEmpty` standalone | `tests/LinqDefaultIfEmptyTests.cs`, `tests/LiveProviderDefaultIfEmptyParityTests.cs` |
| `Chunk` | `tests/LinqSequenceTailOperatorTests.cs` |
| `Append`, `Prepend` | `tests/LinqSequenceTailOperatorTests.cs` |
| `Zip` | `tests/LinqSequenceTailOperatorTests.cs` |
| Operators after `Append`/`Prepend`/`Chunk`/`Zip`/`DefaultIfEmpty(value)` | `tests/LinqSequenceTailOperatorTests.cs` |
| `Count`, `LongCount`, `Any`, `All` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryExecutorCoverageTests.cs` |
| Navigation aggregates: `parent.Children.Any(...)`, `.All(...)`, `.Count()`, `.LongCount()` | `tests/LinqNavigationAggregateTests.cs`, `tests/LinqCompiledQueryExpandedParityTests.cs`, `tests/LinqMultiHopNavAggregateInProjectionTests.cs`, `tests/LiveProviderNavigationAggregateParityTests.cs` |
| Owned / many-to-many collection aggregates: `owner.Items.Count()`, `owner.Tags.Sum(t => t.Weight)` | `tests/NavigationAggregateOwnedCollectionTests.cs`, `tests/NavigationAggregateManyToManyTests.cs`, `tests/NavigationAggregateWhereSideTests.cs`, `tests/NavigationAggregateOrderingTests.cs`, `tests/NavigationAggregateCombinedKindsTests.cs`, `tests/NavigationAggregateAsOfContractTests.cs` |
| Explicit-queryable correlated subqueries: `ctx.Query<X>()...` inside predicates and projections | `tests/CorrelatedScalarAggregateSubqueryTests.cs`, `tests/LinqSubqueryInProjectionTests.cs`, `tests/LinqParityFuzzTests.cs` (grouped/correlated pass), `tests/LiveProviderSubqueryFuzzParityTests.cs` |
| `Sum`, `Average`, `Min`, `Max` | `tests/AggregateOperatorTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqGroupAggregateComputedSelectorTests.cs`, `tests/LinqGroupMultiAggregateTests.cs` |
| `GroupBy` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs`, `tests/LinqGroupByProjectionTests.cs`, `tests/LinqGroupMultiAggregateTests.cs`, `tests/LinqHavingTests.cs`, `tests/LinqCompositeGroupByTests.cs`, `tests/ComplexLinqOperatorTests.cs` (streaming IGrouping), `tests/LiveProviderGroupByParityTests.cs` |
| Inner joins | `tests/CompiledJoinDiagnosticTest.cs`, `tests/CompiledQuerySqlShapeParityTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqDistinctBeforeJoinTests.cs` (scalar DISTINCT outer), `tests/ProviderMobilityStrictCommonSurfaceTests.cs`, `tests/LiveProviderJoinSelectManyParityTests.cs` |
| Group joins | `tests/GroupJoinTests.cs`, `tests/GroupJoinOrderByTests.cs`, `tests/GroupJoinCompiledMaterializerTests.cs`, `tests/LinqDistinctBeforeGroupJoinTests.cs` (scalar DISTINCT outer), `tests/LinqLeftJoinConditionalNullCheckTests.cs` (null-guard projections), `tests/ProviderMobilityStrictCommonSurfaceTests.cs`, `tests/LiveProviderJoinSelectManyParityTests.cs`, `tests/LiveProviderAsyncEnumerableParityTests.cs` (streaming) |
| `SelectMany` | `tests/SelectManyTests.cs`, `tests/AdversarialTenantNavigationShapeTests.cs`, `tests/LinqCrossJoinTests.cs`, `tests/LinqLeftJoinTests.cs`, `tests/LinqCorrelatedSelectManyTests.cs` (correlated expansion), `tests/LiveProviderJoinSelectManyParityTests.cs` |
| Set operations: `Union`, `Intersect`, `Except`, `Concat` | `tests/QueryTranslatorCrossProviderTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryTranslatorRecursionTests.cs`, `tests/LinqSetOperationProjectionTests.cs`, `tests/LinqSetOpCompositionTests.cs`, `tests/LinqSetOpWithTakeSkipPagingTests.cs`, `tests/LinqConcatAnonymousProjectionTests.cs`, `tests/LinqSetOperationLiveProviderTests.cs`, `tests/LiveProviderSetOpParityTests.cs`, `tests/LinqCompiledQueryExpandedParityTests.cs` |
| Keyed set operators: `ExceptBy`, `IntersectBy`, `UnionBy` | `tests/DistinctByProviderShapeTests.cs`, `tests/LinqExceptByIntersectByUnionByImplementationTests.cs`, `tests/LinqKeyedSetOpsTranslatabilityTests.cs`, `tests/LiveProviderDistinctByParityTests.cs` |
| `Include`, `ThenInclude` | `tests/IncludeProcessorCoverageTests.cs`, `tests/CompositeKeyIncludeTests.cs`, `tests/LinqIncludeCompositePkErrorTests.cs` (composite-PK dependent), `tests/QueryExecutorExtendedCoverageTests.cs`, `tests/LinqMultiLevelIncludeTests.cs`, `tests/LiveProviderIncludeParityTests.cs` |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | `tests/QueryTranslatorCoverageTests.cs`, `tests/ConstructorBoundEntityTrackingTests.cs`, `tests/MiscCoverageTests.cs` |
| Window functions: `WithRowNumber`, `WithRank`, `WithDenseRank`, `WithLag`, `WithLead` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryTranslatorCrossProviderTests.cs`, `tests/WindowFunctionParameterTests.cs`, `tests/LiveProviderJsonWindowParityTests.cs` |
| `OfType<T>` / `Cast<T>` | `tests/LinqCastOfTypeTests.cs` (identity pass-through; TPH derived-type filtering via `TphOfTypeTests`), `tests/LiveProviderOfTypeParityTests.cs` (live TPH derived-type filtering), `tests/LinqUnsupportedShapeContractTests.cs` (unsupported shapes) |
| Raw SQL composition | Provider-bound compatibility API rejected by strict provider mobility; covered by `tests/RawSqlNameBasedMaterializationTests.cs`, `tests/SourceGenMaterializerCorrectnesTests.cs`, `tests/TransactionIsolationTests.cs`, `tests/LiveProviderRawSqlParityTests.cs`, `tests/ProviderMobilityStrictModeTests.cs` |
| Dynamic table query `DbContext.Query(string)` | Provider-bound compatibility API rejected by strict provider mobility; covered by `tests/DynamicTypeQueryTests.cs`, `tests/DynamicTypeCacheKeyTests.cs`, `tests/DynamicQueryRootConcurrencyTests.cs`, `tests/ProviderMobilityStrictModeTests.cs` |
| Custom `[SqlFunction]` methods | Provider-bound compatibility API rejected by strict provider mobility unless nORM-owned; covered by `tests/ExpressionToSqlVisitorTests.cs`, `tests/LinqNormFunctionsLikeTests.cs`, `tests/ProviderMobilityStrictModeTests.cs` |
| `AsAsyncEnumerable` | `tests/AsyncEnumerableTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/AsyncCancellationAuditTests.cs`, `tests/GroupJoinAsyncEnumerableTests.cs` (GroupJoin streaming), `tests/LiveProviderAsyncEnumerableParityTests.cs` |
| `ExecuteUpdateAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs`, `tests/ExecuteDeleteUpdateJoinSourceTests.cs` (join source), `tests/LinqCompositePkExecuteCudTests.cs` (composite-PK), `tests/LiveProviderCompositePkBulkCudParityTests.cs`, `tests/OrderedPagedBulkCudTests.cs` (ordered/paged/windowed shapes), `tests/OrderedPagedBulkCudLiveTests.cs`, `tests/BulkCudOracleFuzzTests.cs` (oracle fuzz incl. windowed ops) |
| `ExecuteDeleteAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs`, `tests/ExecuteDeleteUpdateJoinSourceTests.cs` (join source), `tests/LinqCompositePkExecuteCudTests.cs` (composite-PK), `tests/LiveProviderCompositePkBulkCudParityTests.cs`, `tests/OrderedPagedBulkCudTests.cs` (ordered/paged/windowed shapes), `tests/OrderedPagedBulkCudLiveTests.cs`, `tests/BulkCudOracleFuzzTests.cs` (oracle fuzz incl. windowed ops) |

## Terminal operators

| Feature | Coverage |
| --- | --- |
| `ToListAsync`, `ToArrayAsync`, `ToDictionaryAsync` (2 overloads), `ToHashSetAsync` | `tests/TerminalOperatorParityTests.cs`, `tests/PublicApi.Shipped.txt` |
| `FirstAsync`, `FirstOrDefaultAsync` | `tests/TerminalOperatorParityTests.cs` |
| `LastAsync`, `LastOrDefaultAsync` | `tests/LinqReverseAndLastTests.cs` |
| `SingleAsync`, `SingleOrDefaultAsync` | `tests/TerminalOperatorParityTests.cs` |
| `ElementAt`, `ElementAtOrDefault` | `tests/TerminalOperatorParityTests.cs` |
| `CountAsync`, `LongCountAsync`, `AnyAsync` | `tests/TerminalOperatorParityTests.cs`, `tests/LinqOperatorCardinalityTests.cs` |
| `MinByAsync`, `MaxByAsync` | `tests/LinqMinByMaxByTests.cs`, `tests/LiveProviderMinByMaxByParityTests.cs` |

## Value translations

| Feature | Coverage |
| --- | --- |
| `string` methods: `ToUpper`, `ToLower`, `Length`, `Trim`/`TrimStart`/`TrimEnd`, `Substring`, `Replace`, `IndexOf`, `Contains`, `StartsWith`, `EndsWith` | `tests/LinqStringFunctionTranslationTests.cs`, `tests/LinqProjectionStringIndexOfModeTests.cs`, `tests/LinqProjectionStringReplaceModeTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs`, `tests/LiveProviderStringFunctionParityTests.cs` |
| `string` statics: `IsNullOrEmpty`, `IsNullOrWhiteSpace`, `Concat`, `Compare`, `CompareOrdinal`, `CompareTo` | `tests/LinqStringFunctionTranslationTests.cs`, `tests/LinqConversionAndCompareTests.cs`, `tests/LiveProviderStringFunctionParityTests.cs` |
| `string.Format` / interpolated strings | `tests/LinqStringFormatTranslationTests.cs`, `tests/LinqProjectionStringFormatTests.cs`, `tests/LinqProjectionStringFormatColumnsTests.cs`, `tests/LinqClientProjectionTests.cs`, `tests/StringFormatSpecProviderShapeTests.cs`, `tests/StringFormatAlignmentProviderShapeTests.cs`, `tests/LiveProviderStringFormatParityTests.cs` |
| `Regex.IsMatch` | `tests/RegexIsMatchProviderShapeTests.cs`, `tests/RegexOptionsCaseInsensitiveProviderShapeTests.cs`, `tests/LiveProviderRegexIsMatchParityTests.cs`, `tests/ProviderMobilityTranslationLayerTests.cs` |
| `Regex.Replace` | `tests/RegexReplaceProviderShapeTests.cs`, `tests/RegexOptionsCaseInsensitiveProviderShapeTests.cs`, `tests/LiveProviderRegexIsMatchParityTests.cs`, `tests/ProviderMobilityTranslationLayerTests.cs` |
| `Guid.NewGuid()` in queries | `tests/GuidNewGuidProviderShapeTests.cs`, `tests/LiveProviderStaticValuePredicateParityTests.cs` |
| `Convert.ToInt32` / `ToInt64` / `ToString` / `ToBoolean` / `ToDouble` / `ToDecimal` / etc. | `tests/LinqConversionAndCompareTests.cs`, `tests/LiveProviderConvertFunctionParityTests.cs` |
| `Math.Abs`, `Ceiling`, `Floor`, `Round`, `Sqrt`, `Pow`, `Exp`, `Log`, `Log10`, `Sign`, `Min`, `Max`, `Truncate` | `tests/LinqMathFunctionTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `DateTime` members: `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `DayOfYear`, `DayOfWeek`, `Date` | `tests/LinqDateTimeMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `DateTime.AddDays` / `AddMonths` / `AddYears` / `AddHours` / `AddMinutes` / `AddSeconds` | `tests/LinqDateTimeMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `DateTime.UtcNow` / `DateTime.Now` / `DateTime.Today` in predicates | `tests/LinqDateTimeNowTests.cs`, `tests/LiveProviderStaticValuePredicateParityTests.cs` |
| `DateTimeOffset` members | `tests/LinqDateTimeOffsetMemberTests.cs`, `tests/LiveProviderDateTimeOffsetMemberParityTests.cs` |
| `TimeSpan` member access on a stored TimeSpan column (`r.Duration.TotalSeconds`, `.Days`, etc.) | `tests/LinqProjectionTimeSpanTotalsTests.cs`, `tests/LiveProviderTimeSpanMemberParityTests.cs` |
| `DateTime`/`DateTimeOffset` subtraction TimeSpan members (`(r.End - r.Start).TotalHours`, `.TotalDays`, `.TotalSeconds`, `.TotalMinutes`, `.TotalMilliseconds`, `.Days`, `.Hours`, `.Minutes`, `.Seconds`) | `tests/LinqDateTimeArithmeticTests.cs`, `tests/LiveProviderDateTimeSubtractionPrecisionTests.cs` |
| `DateOnly.Year` / `Month` / `Day` / `DayOfYear` | `tests/LinqDateTimeMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `TimeOnly.Hour` / `Minute` / `Second` | `tests/LinqTimeOnlyMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `Nullable<T>.HasValue`, `Value`, `GetValueOrDefault()` / `GetValueOrDefault(fallback)` | `tests/LinqNullableMemberAccessTests.cs`, `tests/LinqPagingAndNullableBoolTests.cs`, `tests/LiveProviderNullableBoolParityTests.cs` |
| `Convert.ChangeType(col, typeof(T))` or captured `Type` value | `tests/LinqConvertChangeTypeOnColumnTests.cs`, `tests/LinqConversionAndCompareTests.cs` |
| `DateTimeOffset.LocalDateTime` accessor (projection / WHERE / OrderBy) | `tests/LinqDateTimeOffsetLocalDateTimeTests.cs`, `tests/LinqDateTimeOffsetLocalDateTimeInWhereTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `DateTimeOffset` col `==` / `!=` DateTime literal | `tests/LinqDateTimeOffsetEqualsDateTimeLiteralTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `DateTimeOffset` - `DateTimeOffset` → `TimeSpan` (cross-column) | `tests/LinqDateTimeOffsetColumnSubtractionTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `DateTimeOffset` col `+` / `-` `TimeSpan` col → `DateTimeOffset` | `tests/LinqDateTimeOffsetPlusTimeSpanColumnTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| LINQ `Aggregate` sum-fold (1-arg + seed forms) | `tests/LinqAggregateOperatorTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| LINQ `Aggregate` min/max-fold (Math.Max/Min + Conditional shapes) | `tests/LinqAggregateMinMaxFoldTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| LINQ `Aggregate` string-concat fold (simple + seed-aware separator) | `tests/LinqAggregateStringConcatTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `Enum.TryParse<T>(stringCol, out T)` as WHERE predicate | `tests/LinqEnumTryParseOutParamTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| Conditional expressions (`cond ? a : b`) in `Where` and `Select` | `tests/LinqEnumAndConditionalTests.cs`, `tests/LiveProviderConditionalArithmeticParityTests.cs` |
| Arithmetic operators (`+`, `-`, `*`, `/`, `%`) in `Where`, `Select`, and aggregate selectors | `tests/LinqEnumAndConditionalTests.cs`, `tests/LinqGroupAggregateComputedSelectorTests.cs`, `tests/LiveProviderConditionalArithmeticParityTests.cs` |
| Enum equality and `(int)enumCol` projection | `tests/LinqEnumAndConditionalTests.cs`, `tests/LiveProviderEnumParityTests.cs` |
| Enum `.ToString()` in projection | `tests/LinqEnumToStringTests.cs`, `tests/LiveProviderEnumParityTests.cs` |
| Local-collection `Contains` (`ids.Contains(x.Id)`) | `tests/LinqMatrixContractTests.cs`, `tests/SqlTranslationTests.cs`, `tests/LiveProviderContainsParityTests.cs` |
| `Guid.Empty` and other static-field constants in predicates | `tests/LinqGuidAndDistinctTests.cs`, `tests/LiveProviderStaticValuePredicateParityTests.cs` |
| `Json.Value<T>(jsonColumn, constantPath)` | `tests/JsonPathValidationTests.cs`, `tests/ExpressionToSqlVisitorTests.cs`, `tests/LiveProviderJsonWindowParityTests.cs` |
| `NormFunctions.Like(value, pattern)` / `NormFunctions.ILike(value, pattern)` | `tests/LinqNormFunctionsLikeTests.cs`, `tests/LinqWhereNormIlikeTests.cs`, `tests/LiveProviderNormFunctionsLikeParityTests.cs` |

This file maps each documented LINQ shape to its contract tests. Provider-neutral
rows that are marked `Supported` are expected to have live parity evidence in
`docs/live-provider-linq-parity.md` before v1 release; SQLite-only or
translator-shape tests are listed here only as additional focused coverage, not
as the whole release proof.
