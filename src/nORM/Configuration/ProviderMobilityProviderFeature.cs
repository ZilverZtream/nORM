#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Provider target capabilities that affect whether generated nORM semantics
    /// can be certified for a concrete database/version.
    /// </summary>
    public enum ProviderMobilityProviderFeature
    {
        /// <summary>Minimum supported database server version.</summary>
        ServerVersion,
        /// <summary>Provider-backed JSON translation.</summary>
        JsonTranslation,
        /// <summary>Provider JSON path extraction and validation strategy.</summary>
        JsonPathTranslation,
        /// <summary>nORM-managed temporal history support.</summary>
        TemporalVersioning,
        /// <summary>Generated bulk insert behavior, native or emulated.</summary>
        BulkInsert,
        /// <summary>Provider transaction savepoint support.</summary>
        Savepoints,
        /// <summary>Maximum parameter count and required batching behavior.</summary>
        ParameterLimit,
        /// <summary>Provider-native tenant session context support.</summary>
        NativeTenantSessionContext,
        /// <summary>Provider-native temporal table support.</summary>
        ProviderNativeTemporalTables,
        /// <summary>Provider native or emulated row-value tuple comparison support.</summary>
        RowTupleComparison,
        /// <summary>Provider native or emulated ordered string aggregate support.</summary>
        OrderedStringAggregate,
        /// <summary>Provider native, emulated, or unsupported regular-expression translation support.</summary>
        RegexTranslation,
        /// <summary>Provider identifier escaping strategy.</summary>
        IdentifierEscaping,
        /// <summary>Provider parameter prefix and binding strategy.</summary>
        ParameterBinding,
        /// <summary>Provider paging syntax strategy.</summary>
        PagingTranslation,
        /// <summary>Provider boolean predicate translation strategy.</summary>
        BooleanPredicateTranslation,
        /// <summary>Provider null-safe equality translation strategy.</summary>
        NullSemantics,
        /// <summary>Provider LIKE wildcard escaping strategy.</summary>
        LikeEscapeTranslation,
        /// <summary>Provider string concatenation translation strategy.</summary>
        StringConcatTranslation,
        /// <summary>Provider scalar cast and conversion translation strategy.</summary>
        TypeConversionTranslation,
        /// <summary>Provider string predicate translation strategy.</summary>
        StringPredicateTranslation,
        /// <summary>Provider character code-point translation strategy.</summary>
        CharacterTranslation,
        /// <summary>Provider fixed numeric and temporal formatting translation strategy.</summary>
        FormattingTranslation,
        /// <summary>Provider DateTime comparison normalization strategy.</summary>
        DateTimeComparisonNormalization,
        /// <summary>Provider decimal comparison, sort, and aggregate normalization strategy.</summary>
        DecimalComparisonNormalization,
        /// <summary>Provider TimeSpan comparison normalization strategy.</summary>
        TimeSpanComparisonNormalization,
        /// <summary>Provider DateTime/DateOnly/TimeOnly construction from SQL parts.</summary>
        TemporalConstructionTranslation,
        /// <summary>Provider DateTime/DateOnly/TimeOnly/TimeSpan arithmetic strategy.</summary>
        TemporalArithmeticTranslation,
        /// <summary>Provider temporal tag clock-source strategy.</summary>
        TemporalClockSource,
        /// <summary>Provider generated-key retrieval strategy.</summary>
        GeneratedKeyRetrieval,
        /// <summary>Provider idempotent insert-or-ignore strategy.</summary>
        InsertOrIgnoreTranslation,
        /// <summary>Provider generated CUD subquery rewrite strategy.</summary>
        CudSubqueryRewrite,
        /// <summary>Provider bitwise XOR translation strategy.</summary>
        BitwiseXorTranslation,
        /// <summary>Provider window-function translation strategy.</summary>
        WindowFunctionTranslation,
        /// <summary>Provider case-sensitive string comparison strategy.</summary>
        CaseSensitiveStringComparison,
        /// <summary>Provider maximum SQL statement length and generated SQL splitting strategy.</summary>
        SqlStatementLengthLimit
    }
}
