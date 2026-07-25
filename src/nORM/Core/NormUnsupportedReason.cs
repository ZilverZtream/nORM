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
    }
}
