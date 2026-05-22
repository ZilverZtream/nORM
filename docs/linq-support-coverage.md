# LINQ Support Coverage

This file anchors `docs/linq-support.md` to executable coverage. Every
Supported, Constrained, or Preview matrix row must have at least one test file
listed here. Unsupported rows may be documented without a coverage row only when
the matrix notes an explicit deterministic failure contract.

| Feature | Coverage |
| --- | --- |
| `Where` predicates | `tests/SqlTranslationTests.cs`, `tests/CrossProviderBehaviorTests.cs`, `tests/AdversarialMultiShapeStressTests.cs` |
| `Select` entity and scalar projections | `tests/MaterializerAndExecutorDeepCoverageTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Select` with custom client logic | `tests/ClientEvaluationPolicyTests.cs`, `tests/QueryExecutorExtendedCoverageTests.cs` |
| `OrderBy`, `ThenBy` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Skip`, `Take` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `Distinct` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs` |
| `Count`, `LongCount`, `Any`, `All` | `tests/LinqOperatorCardinalityTests.cs`, `tests/QueryExecutorCoverageTests.cs` |
| `Sum`, `Average`, `Min`, `Max` | `tests/AggregateOperatorTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| `GroupBy` | `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryComplexityTests.cs` |
| Inner joins | `tests/CompiledJoinDiagnosticTest.cs`, `tests/CompiledQuerySqlShapeParityTests.cs`, `tests/QueryTranslatorCoverageTests.cs` |
| Group joins | `tests/GroupJoinTests.cs`, `tests/GroupJoinOrderByTests.cs`, `tests/GroupJoinCompiledMaterializerTests.cs` |
| `SelectMany` | `tests/SelectManyTests.cs`, `tests/AdversarialTenantNavigationShapeTests.cs` |
| Set operations: `Union`, `Intersect`, `Except` | `tests/QueryTranslatorCrossProviderTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/QueryTranslatorRecursionTests.cs` |
| `Include`, `ThenInclude` | `tests/IncludeProcessorCoverageTests.cs`, `tests/CompositeKeyIncludeTests.cs`, `tests/QueryExecutorExtendedCoverageTests.cs` |
| `AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf` | `tests/QueryTranslatorCoverageTests.cs`, `tests/ConstructorBoundEntityTrackingTests.cs`, `tests/MiscCoverageTests.cs` |
| Raw SQL composition | `tests/RawSqlNameBasedMaterializationTests.cs`, `tests/SourceGenMaterializerCorrectnesTests.cs`, `tests/TransactionIsolationTests.cs` |
| `AsAsyncEnumerable` | `tests/AsyncEnumerableTests.cs`, `tests/QueryTranslatorCoverageTests.cs`, `tests/AsyncCancellationAuditTests.cs` |
| `ExecuteUpdateAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs` |
| `ExecuteDeleteAsync` | `tests/BatchCudTests.cs`, `tests/NormQueryProviderCoverageTests.cs` |

Coverage rows are not a substitute for live provider gates. Provider-neutral
matrix rows still require SQL Server, SQLite, PostgreSQL, and MySQL evidence
before v1 release.
