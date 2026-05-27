# LINQ Support Coverage

This file anchors `docs/linq-support.md` to executable coverage. Every
Supported, Constrained, or Preview matrix row must have at least one test file
listed here. Unsupported rows may be documented without a coverage row only when
the matrix notes an explicit deterministic failure contract.

## Query operators

| Feature | Coverage |
| --- | --- |
| `Where` predicates | `tests/SqlTranslationTests.cs`, `tests/CrossProviderBehaviorTests.cs`, `tests/AdversarialMultiShapeStressTests.cs`, `tests/LinqConversionAndCompareTests.cs`, `tests/LinqEnumAndConditionalTests.cs` |
| `Select` entity and scalar projections | `tests/MaterializerAndExecutorDeepCoverageTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Select` into anonymous types | `tests/LinqEnumAndConditionalTests.cs`, `tests/LinqGroupByProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` into DTO with positional record | `tests/LinqDtoProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` into DTO with parameterized class constructor | `tests/LinqDtoProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` into DTO with init-only / writable property setters (`new T { A = x.A }`) | `tests/LinqDtoProjectionTests.cs`, `tests/LiveProviderDtoProjectionParityTests.cs` |
| `Select` with custom client logic | `tests/ClientEvaluationPolicyTests.cs`, `tests/QueryExecutorExtendedCoverageTests.cs`, `tests/LinqClientProjectionTests.cs`, `tests/LiveProviderClientEvaluationParityTests.cs` |
| `OrderBy`, `ThenBy` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Reverse` | `tests/LinqReverseAndLastTests.cs` |
| `Skip`, `Take` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqPagingAndNullableBoolTests.cs`, `tests/LiveProviderSkipTakeParityTests.cs` |
| `Distinct` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs`, `tests/LinqGuidAndDistinctTests.cs` |
| `DefaultIfEmpty` standalone | `tests/LinqDefaultIfEmptyTests.cs`, `tests/LiveProviderDefaultIfEmptyParityTests.cs` |
| `Count`, `LongCount`, `Any`, `All` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryExecutorCoverageTests.cs` |
| Navigation aggregates: `parent.Children.Any(...)`, `.All(...)`, `.Count()`, `.LongCount()` | `tests/LinqNavigationAggregateTests.cs`, `tests/LinqCompiledQueryExpandedParityTests.cs`, `tests/LinqMultiHopNavAggregateInProjectionTests.cs`, `tests/LiveProviderNavigationAggregateParityTests.cs` |
| `Sum`, `Average`, `Min`, `Max` | `tests/AggregateOperatorTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqGroupAggregateComputedSelectorTests.cs`, `tests/LinqGroupMultiAggregateTests.cs` |
| `GroupBy` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs`, `tests/LinqGroupByProjectionTests.cs`, `tests/LinqGroupMultiAggregateTests.cs`, `tests/LinqHavingTests.cs`, `tests/LinqCompositeGroupByTests.cs`, `tests/ComplexLinqOperatorTests.cs` (streaming IGrouping), `tests/LiveProviderGroupByParityTests.cs` |
| Inner joins | `tests/CompiledJoinDiagnosticTest.cs`, `tests/CompiledQuerySqlShapeParityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| Group joins | `tests/GroupJoinTests.cs`, `tests/GroupJoinOrderByTests.cs`, `tests/GroupJoinCompiledMaterializerTests.cs`, `tests/LinqLeftJoinConditionalNullCheckTests.cs` (null-guard projections), `tests/LiveProviderAsyncEnumerableParityTests.cs` (streaming) |
| `SelectMany` | `tests/SelectManyTests.cs`, `tests/AdversarialTenantNavigationShapeTests.cs`, `tests/LinqCrossJoinTests.cs`, `tests/LinqLeftJoinTests.cs`, `tests/LinqCorrelatedSelectManyTests.cs` (correlated expansion), `tests/LiveProviderJoinSelectManyParityTests.cs` |
| Set operations: `Union`, `Intersect`, `Except` | `tests/QueryTranslatorCrossProviderTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryTranslatorRecursionTests.cs`, `tests/LinqSetOperationProjectionTests.cs`, `tests/LinqSetOpCompositionTests.cs` |
| `Include`, `ThenInclude` | `tests/IncludeProcessorCoverageTests.cs`, `tests/CompositeKeyIncludeTests.cs`, `tests/LinqIncludeCompositePkErrorTests.cs` (composite-PK dependent), `tests/QueryExecutorExtendedCoverageTests.cs`, `tests/LinqMultiLevelIncludeTests.cs`, `tests/LiveProviderIncludeParityTests.cs` |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | `tests/QueryTranslatorCoverageTests.cs`, `tests/ConstructorBoundEntityTrackingTests.cs`, `tests/MiscCoverageTests.cs` |
| Window functions: `WithRowNumber`, `WithRank`, `WithDenseRank`, `WithLag`, `WithLead` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryTranslatorCrossProviderTests.cs`, `tests/WindowFunctionParameterTests.cs`, `tests/LiveProviderJsonWindowParityTests.cs` |
| `OfType<T>` / `Cast<T>` | `tests/LinqCastOfTypeTests.cs` (identity pass-through; TPH derived-type filtering via `TphOfTypeTests`), `tests/LiveProviderOfTypeParityTests.cs` (live TPH derived-type filtering), `tests/LinqUnsupportedShapeContractTests.cs` (unsupported shapes) |
| Raw SQL composition | `tests/RawSqlNameBasedMaterializationTests.cs`, `tests/SourceGenMaterializerCorrectnesTests.cs`, `tests/TransactionIsolationTests.cs`, `tests/LiveProviderRawSqlParityTests.cs` |
| `AsAsyncEnumerable` | `tests/AsyncEnumerableTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/AsyncCancellationAuditTests.cs`, `tests/GroupJoinAsyncEnumerableTests.cs` (GroupJoin streaming), `tests/LiveProviderAsyncEnumerableParityTests.cs` |
| `ExecuteUpdateAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs`, `tests/ExecuteDeleteUpdateJoinSourceTests.cs` (join source), `tests/LinqCompositePkExecuteCudTests.cs` (composite-PK), `tests/LiveProviderCompositePkBulkCudParityTests.cs` |
| `ExecuteDeleteAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs`, `tests/ExecuteDeleteUpdateJoinSourceTests.cs` (join source), `tests/LinqCompositePkExecuteCudTests.cs` (composite-PK), `tests/LiveProviderCompositePkBulkCudParityTests.cs` |

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
| `string` methods: `ToUpper`, `ToLower`, `Length`, `Trim`/`TrimStart`/`TrimEnd`, `Substring`, `Replace`, `IndexOf`, `Contains`, `StartsWith`, `EndsWith` | `tests/LinqStringFunctionTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `string` statics: `IsNullOrEmpty`, `IsNullOrWhiteSpace`, `Concat`, `Compare`, `CompareTo` | `tests/LinqStringFunctionTranslationTests.cs`, `tests/LinqConversionAndCompareTests.cs` |
| `string.Format` / interpolated strings | `tests/LinqStringFormatTranslationTests.cs`, `tests/LinqProjectionStringFormatTests.cs`, `tests/LinqProjectionStringFormatColumnsTests.cs`, `tests/LinqClientProjectionTests.cs`, `tests/StringFormatSpecProviderShapeTests.cs`, `tests/StringFormatAlignmentProviderShapeTests.cs` |
| `Convert.ToInt32` / `ToInt64` / `ToString` / `ToBoolean` / `ToDouble` / `ToDecimal` / etc. | `tests/LinqConversionAndCompareTests.cs` |
| `Math.Abs`, `Ceiling`, `Floor`, `Round`, `Sqrt`, `Pow`, `Exp`, `Log`, `Log10`, `Sign`, `Min`, `Max`, `Truncate` | `tests/LinqMathFunctionTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `DateTime` members: `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `DayOfYear`, `DayOfWeek`, `Date` | `tests/LinqDateTimeMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `DateTime.AddDays` / `AddMonths` / `AddYears` / `AddHours` / `AddMinutes` / `AddSeconds` | `tests/LinqDateTimeMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `DateTime.UtcNow` / `DateTime.Now` / `DateTime.Today` in predicates | `tests/LinqDateTimeNowTests.cs` |
| `DateTimeOffset` members | `tests/LinqDateTimeOffsetMemberTests.cs` |
| `TimeSpan` member access on a stored TimeSpan column (`r.Duration.TotalSeconds`, `.Days`, etc.) | `tests/LinqProjectionTimeSpanTotalsTests.cs` |
| `DateTime`/`DateTimeOffset` subtraction TimeSpan members (`(r.End - r.Start).TotalHours`, `.TotalDays`, `.TotalSeconds`, `.TotalMinutes`, `.TotalMilliseconds`, `.Days`, `.Hours`, `.Minutes`, `.Seconds`) | `tests/LinqDateTimeArithmeticTests.cs`, `tests/LiveProviderDateTimeSubtractionPrecisionTests.cs` |
| `DateOnly.Year` / `Month` / `Day` / `DayOfYear` | `tests/LinqDateTimeMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `TimeOnly.Hour` / `Minute` / `Second` | `tests/LinqTimeOnlyMemberTranslationTests.cs`, `tests/LiveProviderValueFunctionParityTests.cs` |
| `Nullable<T>.HasValue`, `Value`, `GetValueOrDefault()` / `GetValueOrDefault(fallback)` | `tests/LinqNullableMemberAccessTests.cs`, `tests/LinqPagingAndNullableBoolTests.cs`, `tests/LiveProviderNullableBoolParityTests.cs` |
| `Convert.ChangeType(col, typeof(T))` with `Type` constant | `tests/LinqConvertChangeTypeOnColumnTests.cs` |
| `DateTimeOffset.LocalDateTime` accessor (projection / WHERE / OrderBy) | `tests/LinqDateTimeOffsetLocalDateTimeTests.cs`, `tests/LinqDateTimeOffsetLocalDateTimeInWhereTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `DateTimeOffset` col `==` / `!=` DateTime literal | `tests/LinqDateTimeOffsetEqualsDateTimeLiteralTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `DateTimeOffset` - `DateTimeOffset` → `TimeSpan` (cross-column) | `tests/LinqDateTimeOffsetColumnSubtractionTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `DateTimeOffset` col `+` / `-` `TimeSpan` col → `DateTimeOffset` | `tests/LinqDateTimeOffsetPlusTimeSpanColumnTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| LINQ `Aggregate` sum-fold (1-arg + seed forms) | `tests/LinqAggregateOperatorTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| LINQ `Aggregate` min/max-fold (Math.Max/Min + Conditional shapes) | `tests/LinqAggregateMinMaxFoldTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| LINQ `Aggregate` string-concat fold (simple + seed-aware separator) | `tests/LinqAggregateStringConcatTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| `Enum.TryParse<T>(stringCol, out T)` as WHERE predicate | `tests/LinqEnumTryParseOutParamTests.cs`, `tests/LiveProviderRecentScvParityTests.cs` |
| Conditional expressions (`cond ? a : b`) in `Where` and `Select` | `tests/LinqEnumAndConditionalTests.cs` |
| Arithmetic operators (`+`, `-`, `*`, `/`, `%`) in `Where`, `Select`, and aggregate selectors | `tests/LinqEnumAndConditionalTests.cs`, `tests/LinqGroupAggregateComputedSelectorTests.cs` |
| Enum equality and `(int)enumCol` projection | `tests/LinqEnumAndConditionalTests.cs`, `tests/LiveProviderEnumParityTests.cs` |
| Enum `.ToString()` in projection | `tests/LinqEnumToStringTests.cs`, `tests/LiveProviderEnumParityTests.cs` |
| Local-collection `Contains` (`ids.Contains(x.Id)`) | `tests/LinqMatrixContractTests.cs`, `tests/SqlTranslationTests.cs`, `tests/LiveProviderContainsParityTests.cs` |
| `Guid.Empty` and other static-field constants in predicates | `tests/LinqGuidAndDistinctTests.cs` |
| `Json.Value<T>(jsonColumn, constantPath)` | `tests/JsonPathValidationTests.cs`, `tests/ExpressionToSqlVisitorTests.cs`, `tests/LiveProviderJsonWindowParityTests.cs` |
| `NormFunctions.Like(value, pattern)` / `NormFunctions.ILike(value, pattern)` | `tests/LinqNormFunctionsLikeTests.cs`, `tests/LinqWhereNormIlikeTests.cs`, `tests/LiveProviderNormFunctionsLikeParityTests.cs` |

This file maps each documented LINQ shape to its contract tests. Provider-neutral
rows that are marked `Supported` are expected to have live parity evidence in
`docs/live-provider-linq-parity.md` before v1 release; SQLite-only or
translator-shape tests are listed here only as additional focused coverage, not
as the whole release proof.
