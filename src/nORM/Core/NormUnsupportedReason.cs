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
    }
}
