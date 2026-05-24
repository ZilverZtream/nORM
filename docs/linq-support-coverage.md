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
| `Select` into anonymous types | `tests/LinqEnumAndConditionalTests.cs`, `tests/LinqGroupByProjectionTests.cs` |
| `Select` into DTO with positional record | `tests/LinqDtoProjectionTests.cs` |
| `Select` into DTO with parameterized class constructor | `tests/LinqDtoProjectionTests.cs` |
| `Select` into DTO with init-only / writable property setters (`new T { A = x.A }`) | `tests/LinqDtoProjectionTests.cs` |
| `Select` with custom client logic | `tests/ClientEvaluationPolicyTests.cs`, `tests/QueryExecutorExtendedCoverageTests.cs`, `tests/LinqClientProjectionTests.cs`, `tests/LinqEnumToStringTests.cs` |
| `OrderBy`, `ThenBy` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Reverse` | `tests/LinqReverseAndLastTests.cs` |
| `Skip`, `Take` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqPagingAndNullableBoolTests.cs` |
| `Distinct` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs`, `tests/LinqGuidAndDistinctTests.cs` |
| `Count`, `LongCount`, `Any`, `All` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryExecutorCoverageTests.cs` |
| Navigation aggregates: `parent.Children.Any(...)`, `.All(...)`, `.Count()`, `.LongCount()` | `tests/LinqNavigationAggregateTests.cs`, `tests/LinqCompiledQueryExpandedParityTests.cs` |
| `Sum`, `Average`, `Min`, `Max` | `tests/AggregateOperatorTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/LinqGroupAggregateComputedSelectorTests.cs`, `tests/LinqGroupMultiAggregateTests.cs` |
| `GroupBy` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs`, `tests/LinqGroupByProjectionTests.cs`, `tests/LinqGroupMultiAggregateTests.cs`, `tests/LinqHavingTests.cs`, `tests/LinqCompositeGroupByTests.cs` |
| Inner joins | `tests/CompiledJoinDiagnosticTest.cs`, `tests/CompiledQuerySqlShapeParityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| Group joins | `tests/GroupJoinTests.cs`, `tests/GroupJoinOrderByTests.cs`, `tests/GroupJoinCompiledMaterializerTests.cs` |
| `SelectMany` | `tests/SelectManyTests.cs`, `tests/AdversarialTenantNavigationShapeTests.cs`, `tests/LinqCrossJoinTests.cs`, `tests/LinqLeftJoinTests.cs` |
| Set operations: `Union`, `Intersect`, `Except` | `tests/QueryTranslatorCrossProviderTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryTranslatorRecursionTests.cs`, `tests/LinqSetOperationProjectionTests.cs`, `tests/LinqSetOpCompositionTests.cs` |
| `Include`, `ThenInclude` | `tests/IncludeProcessorCoverageTests.cs`, `tests/CompositeKeyIncludeTests.cs`, `tests/QueryExecutorExtendedCoverageTests.cs`, `tests/LinqMultiLevelIncludeTests.cs` |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | `tests/QueryTranslatorCoverageTests.cs`, `tests/ConstructorBoundEntityTrackingTests.cs`, `tests/MiscCoverageTests.cs` |
| Raw SQL composition | `tests/RawSqlNameBasedMaterializationTests.cs`, `tests/SourceGenMaterializerCorrectnesTests.cs`, `tests/TransactionIsolationTests.cs` |
| `AsAsyncEnumerable` | `tests/AsyncEnumerableTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/AsyncCancellationAuditTests.cs` |
| `ExecuteUpdateAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs` |
| `ExecuteDeleteAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs` |

## Terminal operators

| Feature | Coverage |
| --- | --- |
| `ToListAsync`, `ToArrayAsync`, `ToDictionaryAsync` (2 overloads), `ToHashSetAsync` | `tests/TerminalOperatorParityTests.cs`, `tests/PublicApi.Shipped.txt` |
| `FirstAsync`, `FirstOrDefaultAsync` | `tests/TerminalOperatorParityTests.cs` |
| `LastAsync`, `LastOrDefaultAsync` | `tests/LinqReverseAndLastTests.cs` |
| `SingleAsync`, `SingleOrDefaultAsync` | `tests/TerminalOperatorParityTests.cs` |
| `ElementAt`, `ElementAtOrDefault` | `tests/TerminalOperatorParityTests.cs` |
| `CountAsync`, `LongCountAsync`, `AnyAsync` | `tests/TerminalOperatorParityTests.cs`, `tests/LinqOperatorCardinalityTests.cs` |

## Value translations

| Feature | Coverage |
| --- | --- |
| `string` methods: `ToUpper`, `ToLower`, `Length`, `Trim`/`TrimStart`/`TrimEnd`, `Substring`, `Replace`, `IndexOf`, `Contains`, `StartsWith`, `EndsWith` | `tests/LinqStringFunctionTranslationTests.cs` |
| `string` statics: `IsNullOrEmpty`, `IsNullOrWhiteSpace`, `Concat`, `Compare`, `CompareTo` | `tests/LinqStringFunctionTranslationTests.cs`, `tests/LinqConversionAndCompareTests.cs` |
| `string.Format` / interpolated strings | `tests/LinqClientProjectionTests.cs` |
| `Convert.ToInt32` / `ToInt64` / `ToString` / `ToBoolean` / `ToDouble` / `ToDecimal` / etc. | `tests/LinqConversionAndCompareTests.cs` |
| `Math.Abs`, `Ceiling`, `Floor`, `Round`, `Sqrt`, `Pow`, `Exp`, `Log`, `Log10`, `Sign`, `Min`, `Max`, `Truncate` | `tests/LinqMathFunctionTranslationTests.cs` |
| `DateTime` members: `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `DayOfYear`, `DayOfWeek`, `Date` | `tests/LinqDateTimeMemberTranslationTests.cs` |
| `DateTime.AddDays` / `AddMonths` / `AddYears` / `AddHours` / `AddMinutes` / `AddSeconds` | `tests/LinqDateTimeMemberTranslationTests.cs` |
| `DateTime.UtcNow` / `DateTime.Now` / `DateTime.Today` in predicates | `tests/LinqDateTimeNowTests.cs` |
| `DateTimeOffset` members | `tests/LinqDateTimeOffsetMemberTests.cs` |
| `DateTime`/`DateTimeOffset` subtraction TimeSpan members (`(r.End - r.Start).TotalHours`, `.TotalDays`, `.TotalSeconds`, `.TotalMinutes`, `.TotalMilliseconds`, `.Days`, `.Hours`, `.Minutes`, `.Seconds`) | `tests/LinqDateTimeArithmeticTests.cs` |
| `DateOnly.Year` / `Month` / `Day` / `DayOfYear` | `tests/LinqDateTimeMemberTranslationTests.cs` |
| `TimeOnly.Hour` / `Minute` / `Second` | `tests/LinqTimeOnlyMemberTranslationTests.cs` |
| `Nullable<T>.HasValue`, `Value`, `GetValueOrDefault()` / `GetValueOrDefault(fallback)` | `tests/LinqNullableMemberAccessTests.cs`, `tests/LinqPagingAndNullableBoolTests.cs` |
| Conditional expressions (`cond ? a : b`) in `Where` and `Select` | `tests/LinqEnumAndConditionalTests.cs` |
| Arithmetic operators (`+`, `-`, `*`, `/`, `%`) in `Where`, `Select`, and aggregate selectors | `tests/LinqEnumAndConditionalTests.cs`, `tests/LinqGroupAggregateComputedSelectorTests.cs` |
| Enum equality and `(int)enumCol` projection | `tests/LinqEnumAndConditionalTests.cs` |
| Enum `.ToString()` in projection | `tests/LinqEnumToStringTests.cs` |
| Local-collection `Contains` (`ids.Contains(x.Id)`) | `tests/LinqMatrixContractTests.cs`, `tests/SqlTranslationTests.cs` |
| `Guid.Empty` and other static-field constants in predicates | `tests/LinqGuidAndDistinctTests.cs` |
| `NormFunctions.Like(value, pattern)` | `tests/LinqNormFunctionsLikeTests.cs` |

Coverage rows are not a substitute for live provider gates. Provider-neutral
matrix rows still require SQL Server, SQLite, PostgreSQL, and MySQL evidence
before v1 release.
