namespace nORM.Core
{
    /// <summary>
    /// Stable machine-readable codes for <see cref="NormUnsupportedFeatureException.ReasonCode"/>. Each code
    /// identifies a SPECIFIC capability limitation independently of the human-readable message, so the
    /// fuzzing support-contract can tell an intended unsupported shape apart from a silent capability
    /// regression (a shape that used to translate but now throws). Codes are append-only and never renamed
    /// once used: the reason-code contract test asserts the classified throw-sites reference this catalog,
    /// and fuzz-run manifests key on these codes.
    /// <para>
    /// Internal by design — the catalog is consumed test-side via <c>InternalsVisibleTo</c> and is NOT part
    /// of the public API surface. Whether to expose reason codes publicly is a separate, later decision.
    /// </para>
    /// </summary>
    internal static class NormUnsupportedReason
    {
        // ── String method translation (ExpressionToSqlVisitor.MethodCallTranslators.String) ──

        /// <summary>A string comparison-mode argument (bool/StringComparison) was not a resolvable constant or captured value.</summary>
        public const string StringComparisonModeNotConstant = "string-comparison-mode-not-constant";

        /// <summary>A string method overload with the given argument count has no translation.</summary>
        public const string StringMethodOverload = "string-method-overload";

        /// <summary>The active provider does not support this string function form.</summary>
        public const string StringFunctionProviderUnsupported = "string-function-provider-unsupported";

        /// <summary>Case-insensitive <c>string.Replace</c> is not provider-mobile (native REPLACE semantics differ across providers).</summary>
        public const string StringReplaceIgnoreCaseNotMobile = "string-replace-ignorecase-not-mobile";

        /// <summary>The string method has no SQL translation on any provider.</summary>
        public const string StringMethodUntranslatable = "string-method-untranslatable";

        // ── Method-call translation (ExpressionToSqlVisitor.MethodCallTranslators) ──

        /// <summary>The active provider does not implement the DateOnly/TimeOnly translation hook this call needs.</summary>
        public const string DateTimeFunctionProviderHookMissing = "datetime-function-provider-hook-missing";

        /// <summary>A ToString(format) call uses format tokens with no SQL translation (e.g. locale-aware MMM/dddd).</summary>
        public const string ToStringFormatUnsupported = "tostring-format-unsupported";

        /// <summary>Enum.TryParse's ignoreCase argument was not a resolvable constant or captured value.</summary>
        public const string EnumTryParseIgnoreCaseNotConstant = "enum-tryparse-ignorecase-not-constant";

        /// <summary>A custom SQL function is rejected under the strict provider-mobility contract.</summary>
        public const string CustomSqlFunctionStrictMobility = "custom-sql-function-strict-mobility";

        // ── Navigation / correlated-subquery aggregates (ExpressionToSqlVisitor.NavigationSubqueries) ──

        /// <summary>Aggregating a navigation collection under AsOf is not supported (the aggregate reads the live table, not the historical era).</summary>
        public const string CollectionAggregateUnderAsOf = "collection-aggregate-under-asof";

        /// <summary>Aggregating an owned/many-to-many collection whose element has a composite key is not supported.</summary>
        public const string CollectionAggregateCompositeKey = "collection-aggregate-composite-key";

        /// <summary>All(...) over a navigation collection requires an explicit predicate.</summary>
        public const string CollectionAllRequiresPredicate = "collection-all-requires-predicate";

        /// <summary>An aggregate over a navigation collection requires a selector (e.g. x =&gt; x.Value).</summary>
        public const string CollectionAggregateRequiresSelector = "collection-aggregate-requires-selector";

        /// <summary>An aggregate over a correlated subquery requires a single scalar projection.</summary>
        public const string CorrelatedAggregateRequiresScalarProjection = "correlated-aggregate-requires-scalar-projection";

        /// <summary>A non-constant DefaultIfEmpty fallback over a correlated subquery is not supported.</summary>
        public const string CorrelatedDefaultIfEmptyFallbackNotConstant = "correlated-defaultifempty-fallback-not-constant";

        /// <summary>A correlated-subquery element/position operation (Last/ElementAt) requires an OrderBy.</summary>
        public const string CorrelatedRequiresOrdering = "correlated-requires-ordering";

        /// <summary>The correlated subquery's source shape (a windowing/reshaping operator) has no sound scalar-aggregate translation.</summary>
        public const string CorrelatedSourceShapeUnsupported = "correlated-source-shape-unsupported";

        // ── General expression translation (ExpressionToSqlVisitor.Binary/ControlFlow/Enumerable/Members) ──

        /// <summary>A binary operator (e.g. Power) has no portable SQL equivalent.</summary>
        public const string BinaryOperatorUnsupported = "binary-operator-unsupported";

        /// <summary>A method call has no SQL translation in the nORM query translator.</summary>
        public const string MethodUntranslatable = "method-untranslatable";

        /// <summary>A LINQ query operator (Queryable method) has no SQL translation in this context.</summary>
        public const string QueryableMethodUntranslatable = "queryable-method-untranslatable";

        /// <summary>A member access has no SQL translation in this context.</summary>
        public const string MemberUntranslatable = "member-untranslatable";

        // ── Projection navigation aggregates (SelectClauseVisitor.*) ──

        /// <summary>All(...) over a navigation collection inside a projection has no SQL translation.</summary>
        public const string CollectionAllInProjectionUnsupported = "collection-all-in-projection-unsupported";

        /// <summary>An aggregate selector references a member that is not a mapped column on the related/owned entity.</summary>
        public const string AggregateSelectorNotMappedColumn = "aggregate-selector-not-mapped-column";

        /// <summary>A navigation-aggregate selector (member/reference-chain/computed) could not be translated to SQL.</summary>
        public const string NavAggregateSelectorUntranslatable = "nav-aggregate-selector-untranslatable";

        /// <summary>SUM/AVG over a value-converter column has no correct result (ConvertFromProvider does not distribute over the aggregate).</summary>
        public const string NavAggregateValueConverterColumn = "nav-aggregate-value-converter-column";

        // ── Projection navigation First/Last, paging & ordering (SelectClauseVisitor.NavigationFirst/Helpers/MethodCalls) ──

        /// <summary>A navigation First/Last selector or order key could not be translated to SQL.</summary>
        public const string NavFirstSelectorUntranslatable = "nav-first-selector-untranslatable";

        /// <summary>First/Last over a navigation collection under AsOf is not supported (reads the live table, not the historical era).</summary>
        public const string CollectionFirstUnderAsOf = "collection-first-under-asof";

        /// <summary>First/Last over a navigation collection whose element has a composite key is not supported.</summary>
        public const string CollectionFirstCompositeKey = "collection-first-composite-key";

        /// <summary>Skip/Take on a projected collection requires an OrderBy to be deterministic.</summary>
        public const string CollectionPagingRequiresOrderBy = "collection-paging-requires-orderby";

        /// <summary>Only a simple property ordering key is supported on an ordered projected collection (no computed/composite keys).</summary>
        public const string CollectionOrderingKeyNotSimple = "collection-ordering-key-not-simple";

        /// <summary>Ordering an included collection by a value-converter column is not supported (stored order may differ from model order).</summary>
        public const string CollectionOrderingValueConverter = "collection-ordering-value-converter";

        /// <summary>A projected correlated subquery returns a whole row/entity (multiple columns) where a single scalar is required.</summary>
        public const string CorrelatedSubqueryReturnsRow = "correlated-subquery-returns-row";

        // ── Client-materialized sequence tails: Append/Prepend/Chunk/Zip (QueryTranslator.SequenceTailTranslators) ──

        /// <summary>A method after a client-materialized sequence operator (Append/Prepend/Chunk/Zip/DefaultIfEmpty) would evaluate against server rows, not the reshaped sequence.</summary>
        public const string SequenceTailAfterClientOperator = "sequence-tail-after-client-operator";

        /// <summary>A method over a client-materialized sequence has no supported overload here (e.g. only a one-argument predicate, or no in-memory equivalent).</summary>
        public const string SequenceTailOverloadUnsupported = "sequence-tail-overload-unsupported";

        /// <summary>Chunk requires a constant or captured int size; a column-derived size has no SQL translation.</summary>
        public const string ChunkSizeNotConstant = "chunk-size-not-constant";

        /// <summary>An Append/Prepend/Contains element must be a constant or captured value; a row-derived element has no SQL translation.</summary>
        public const string SequenceElementNotConstant = "sequence-element-not-constant";

        /// <summary>Zip's second source must be a constant/captured local sequence or a captured-state database query (not a row-derived query).</summary>
        public const string ZipSecondSequenceUnsupported = "zip-second-sequence-unsupported";

        /// <summary>Zip over two database queries requires an explicit OrderBy/ThenBy chain so positional pairing is deterministic.</summary>
        public const string ZipRequiresOrdering = "zip-requires-ordering";

        // ── Include / ThenInclude and OfType/Cast (QueryTranslator.IncludeTranslators) ──

        /// <summary>An Include/ThenInclude navigation shape is unsupported (only plain, filtered, or ordered/top-N are).</summary>
        public const string IncludeShapeUnsupported = "include-shape-unsupported";

        /// <summary>A filtered or ordered/top-N Include on a many-to-many navigation is not supported.</summary>
        public const string IncludeM2mFilterOrderUnsupported = "include-m2m-filter-order-unsupported";

        /// <summary>OrderBy/Take/Skip on a reference navigation is meaningless (it loads a single related entity) and is not supported.</summary>
        public const string IncludeReferenceNavOrderingMeaningless = "include-reference-nav-ordering-meaningless";

        /// <summary>An OfType/Cast call requires a generic type argument.</summary>
        public const string MethodRequiresGenericTypeArgument = "method-requires-generic-type-argument";

        /// <summary>OfType&lt;T&gt; targets a type that is not a [DiscriminatorValue]-mapped subtype of the source (no TPH discriminator).</summary>
        public const string OfTypeNonDiscriminatedSubtype = "oftype-non-discriminated-subtype";

        /// <summary>An operation needs a deterministic row order but the source lacks an explicit OrderBy/ThenBy chain.</summary>
        public const string RequiresExplicitOrdering = "requires-explicit-ordering";

        // ── Paging: Skip/Take/SkipWhile/TakeWhile windows (QueryTranslator.PagingTranslators) ──

        /// <summary>A paging argument (Skip/Take/ElementAt count) could not be bound to a parameter or literal.</summary>
        public const string PagingArgumentUnbindable = "paging-argument-unbindable";

        /// <summary>A paging operator requires a one-argument or index-aware predicate overload for provider-mobile SQL.</summary>
        public const string PagingPredicateOverloadRequired = "paging-predicate-overload-required";

        /// <summary>A paging tail operator composes only with a directly-preceding Take/Skip window, not across intervening operators.</summary>
        public const string PagingTailRequiresAdjacentWindow = "paging-tail-requires-adjacent-window";

        // ── Set operations: Union/Intersect/Except/Concat (QueryTranslator.SetOperationTranslators) ──

        /// <summary>The set operation is unsupported in this shape.</summary>
        public const string SetOpUnsupported = "setop-unsupported";

        /// <summary>A set operation's second sequence must be built from captured state, not a row-derived query.</summary>
        public const string SetOpSecondSequenceUnsupported = "setop-second-sequence-unsupported";

        /// <summary>A set operation with a client-materialized sequence operator in either arm is not supported (SQL set semantics dedup by row).</summary>
        public const string SetOpClientMaterializedArm = "setop-client-materialized-arm";

        // ── SequenceEqual (QueryTranslator.SequenceEqualTranslator) ──

        /// <summary>SequenceEqual comparer overloads are not provider-mobile.</summary>
        public const string SequenceEqualComparerUnsupported = "sequenceequal-comparer-unsupported";

        /// <summary>SequenceEqual requires both sources to project the same provider-mobile row shape.</summary>
        public const string SequenceEqualRowShapeMismatch = "sequenceequal-row-shape-mismatch";

        /// <summary>SequenceEqual against a local sequence containing null rows is not provider-mobile.</summary>
        public const string SequenceEqualLocalNullRows = "sequenceequal-local-null-rows";

        // ── Remaining QueryTranslator families (terminal, group-by, joins, split, temporal, order) ──

        /// <summary>ElementAt/ElementAtOrDefault requires a constant integer index.</summary>
        public const string ElementAtIndexNotConstant = "elementat-index-not-constant";

        /// <summary>A GroupBy projection member uses an operation nORM cannot translate to SQL.</summary>
        public const string GroupByProjectionMemberUnsupported = "groupby-projection-member-unsupported";

        /// <summary>A per-group projection (g.Xxx(...)) translates only as an ordered scalar chain (Where/OrderBy/aggregate).</summary>
        public const string GroupByProjectionScalarChainOnly = "groupby-projection-scalar-chain-only";

        /// <summary>An operation (Where/OrderBy) applied after a Take/Skip window that is not syntactically visible would silently apply to the full table.</summary>
        public const string OperationAfterInvisibleWindow = "operation-after-invisible-window";

        /// <summary>The query projection requires client-side evaluation — it contains an expression with no SQL translation.</summary>
        public const string ProjectionRequiresClientEval = "projection-requires-client-eval";

        /// <summary>Projecting the same navigation collection into more than one member of a single projection is not supported.</summary>
        public const string CollectionProjectedMultipleMembers = "collection-projected-multiple-members";

        /// <summary>Ordered / top-N (OrderBy/Take/Skip) projection is not supported for owned or many-to-many collections.</summary>
        public const string OrderedProjectionOwnedM2mUnsupported = "ordered-projection-owned-m2m-unsupported";

        /// <summary>A Join/GroupJoin over a `.Distinct()` outer source of an unsupported shape is not supported.</summary>
        public const string JoinDistinctOuterUnsupported = "join-distinct-outer-unsupported";

        /// <summary>A GroupJoin inner key has no mapped column and the entity declares no primary key to use as the match probe.</summary>
        public const string GroupJoinInnerKeyNoColumn = "groupjoin-inner-key-no-column";

        /// <summary>A query cannot combine two different AsOf timestamps in one statement.</summary>
        public const string MultipleAsOfTimestamps = "multiple-asof-timestamps";

        /// <summary>A many-to-many navigation cannot be combined with AsOf (the association table is not versioned).</summary>
        public const string M2mWithAsOfUnsupported = "m2m-with-asof-unsupported";

        /// <summary>A GroupBy key references a navigation property that would require client-side grouping over unloaded navigations.</summary>
        public const string GroupByKeyReferencesNavigation = "groupby-key-references-navigation";

        // ── Provider SQL-expression capabilities (Providers/DatabaseProvider.SqlExpressions) ──

        /// <summary>Regex.IsMatch/Replace is not supported by the active provider.</summary>
        public const string RegexProviderUnsupported = "regex-provider-unsupported";

        /// <summary>A DateTime/DateOnly/TimeOnly constructor with column (non-constant) arguments is not supported by the active provider.</summary>
        public const string DateTimeCtorColumnArgsUnsupported = "datetime-ctor-column-args-unsupported";

        /// <summary>TimeSpan column arithmetic is not supported by the active provider.</summary>
        public const string TimeSpanColumnArithmeticUnsupported = "timespan-column-arithmetic-unsupported";

        /// <summary>A DateTimeOffset operation (ToOffset/LocalDateTime/UTC-instant comparison) is not supported by the active provider.</summary>
        public const string DateTimeOffsetOperationUnsupported = "datetimeoffset-operation-unsupported";

        // ── Bulk ExecuteUpdate/ExecuteDelete (Query/BulkCudBuilder) ──

        /// <summary>ExecuteUpdate/ExecuteDelete requires query-shape metadata that was not available.</summary>
        public const string ExecuteUpdateRequiresQueryMetadata = "executeupdate-requires-query-metadata";

        /// <summary>ExecuteUpdate/ExecuteDelete does not support grouped or aggregated source queries.</summary>
        public const string ExecuteUpdateGroupedUnsupported = "executeupdate-grouped-unsupported";

        /// <summary>A SetProperty value expression could not be evaluated or is an unsupported shape/node.</summary>
        public const string SetPropertyValueUnsupported = "setproperty-value-unsupported";

        /// <summary>A SetProperty target member is not a mapped column on the entity.</summary>
        public const string SetPropertyTargetNotMappedColumn = "setproperty-target-not-mapped-column";

        /// <summary>A method call inside a SetProperty value is not translatable on the active provider.</summary>
        public const string SetPropertyMethodUntranslatable = "setproperty-method-untranslatable";

        /// <summary>A predicate operator inside a SetProperty conditional value is not supported.</summary>
        public const string SetPropertyConditionalPredicateUnsupported = "setproperty-conditional-predicate-unsupported";

        /// <summary>A navigation-aggregate subquery inside a bulk ExecuteUpdate SetProperty has an unsupported shape.</summary>
        public const string BulkNavAggregateUnsupported = "bulk-nav-aggregate-unsupported";

        // ── Write-path CUD (Query/NormQueryProvider.SyncCud) ──

        /// <summary>A bulk write (ExecuteUpdate/Delete) cannot be combined with AsOf — writes target the live table.</summary>
        public const string WriteWithAsOfUnsupported = "write-with-asof-unsupported";

        /// <summary>ExecuteUpdate/Delete over an ordered or paged query requires key columns on the entity.</summary>
        public const string ExecuteUpdateOrderedPagedRequiresKeys = "executeupdate-ordered-paged-requires-keys";

        /// <summary>A write operation targets an entity configured as read-only / query-only.</summary>
        public const string WriteReadOnlyEntity = "write-readonly-entity";

        // ── Provider runtime capabilities (Providers/*Provider.Runtime) ──

        /// <summary>Savepoints are not supported for the given transaction type.</summary>
        public const string SavepointsUnsupportedForTransactionType = "savepoints-unsupported-for-transaction-type";

        // ── Temporal / multi-tenancy capabilities (DatabaseProvider.TemporalTenant, DbContext.TenantTemporal, DbContext.Connection) ──

        /// <summary>The active provider does not support provider-native temporal tables / as-of queries, or the operation is unsupported in provider-native temporal mode.</summary>
        public const string ProviderNativeTemporalUnsupported = "provider-native-temporal-unsupported";

        /// <summary>The active provider does not support provider-native tenant session context / policy DDL.</summary>
        public const string ProviderNativeTenantUnsupported = "provider-native-tenant-unsupported";

        /// <summary>Temporal tags are global and cannot be pruned/scoped under tenant mode.</summary>
        public const string TemporalTagsGlobal = "temporal-tags-global";

        /// <summary>A temporal operation requires a mapped primary key with the right number of non-null key values.</summary>
        public const string TemporalOperationKeyRequirement = "temporal-operation-key-requirement";

        /// <summary>The configured native tenant security mode is not supported.</summary>
        public const string NativeTenantSecurityModeUnsupported = "native-tenant-security-mode-unsupported";
    }
}
