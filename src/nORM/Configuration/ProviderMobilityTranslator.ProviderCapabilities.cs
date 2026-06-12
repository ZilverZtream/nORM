using System;
using System.Collections.Generic;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {

        /// <summary>
        /// Decides whether a provider capability can participate in provider-mobile certification.
        /// </summary>
        public static ProviderMobilityProviderDecision DecideProviderCapability(
            ProviderCapabilities capabilities,
            ProviderMobilityProviderFeature feature)
        {
            ArgumentNullException.ThrowIfNull(capabilities);

            return feature switch
            {
                ProviderMobilityProviderFeature.ServerVersion => DecideProviderVersion(capabilities, capabilities.MinimumServerVersion),
                ProviderMobilityProviderFeature.JsonTranslation => CapabilityBool(
                    capabilities,
                    feature,
                    capabilities.SupportsJson,
                    "JSON translation is available through nORM provider SQL generation.",
                    "JSON translation is not available for this provider target.",
                    "Avoid JSON query shapes for this target or add provider translations and live parity tests."),
                ProviderMobilityProviderFeature.JsonPathTranslation => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "JSON path extraction strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so JSON path SQL and validation are explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.TemporalVersioning => CapabilityBool(
                    capabilities,
                    feature,
                    capabilities.SupportsTemporalVersioning,
                    "nORM-managed temporal history is supported for this provider target.",
                    "nORM-managed temporal history is not supported for this provider target.",
                    "Disable temporal features for this target or add provider-specific history DDL/triggers with parity tests."),
                ProviderMobilityProviderFeature.BulkInsert => capabilities.SupportsNativeBulkInsert
                    ? PD(capabilities.ProviderName, feature, ProviderMobilitySupport.Portable, true, "Info",
                        "Provider-native bulk insert is available.",
                        "Use the generated nORM bulk API and keep benchmark evidence per provider.",
                        capabilities.MinimumServerVersion, null)
                    : PD(capabilities.ProviderName, feature, ProviderMobilitySupport.Emulated, true, "Warning",
                        "Provider-native bulk insert is not available; nORM must preserve generated bulk semantics through fallback SQL.",
                        "Keep fallback bulk behavior covered by provider parity tests and avoid claiming native bulk for this target.",
                        capabilities.MinimumServerVersion, null),
                ProviderMobilityProviderFeature.Savepoints => CapabilityBool(
                    capabilities,
                    feature,
                    capabilities.SupportsSavepoints,
                    "Transaction savepoints are available for nORM transaction wrappers.",
                    "Transaction savepoints are not available for this provider target.",
                    "Avoid savepoint-dependent workflows or add provider savepoint support before certification."),
                ProviderMobilityProviderFeature.ParameterLimit => capabilities.MaxParameters > 0
                    ? PD(capabilities.ProviderName, feature,
                        capabilities.MaxParameters < 1_000 ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                        true,
                        capabilities.MaxParameters < 1_000 ? "Warning" : "Info",
                        $"{capabilities.ProviderName} supports up to {capabilities.MaxParameters} parameters per command.",
                        "Generated write/query paths must batch or split commands before reaching the provider parameter limit.",
                        capabilities.MinimumServerVersion, null)
                    : PD(capabilities.ProviderName, feature, ProviderMobilitySupport.Unsupported, false, "Error",
                        $"{capabilities.ProviderName} does not expose a valid parameter limit.",
                        "Set ProviderCapabilities.MaxParameters to a positive value and add batching tests.",
                        capabilities.MinimumServerVersion, null),
                ProviderMobilityProviderFeature.NativeTenantSessionContext => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Info",
                    "Provider-native tenant session context is database-specific defense-in-depth infrastructure, not generated-path provider mobility.",
                    "Use generated tenant predicates for provider mobility; keep native RLS/session context as explicit deployment evidence.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.ProviderNativeTemporalTables => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Info",
                    "Provider-native temporal tables are database-specific infrastructure, not nORM-managed provider-neutral temporal history.",
                    "Use nORM-managed temporal history for provider mobility; keep native temporal DDL as explicit provider evidence.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.RowTupleComparison => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Row-value tuple comparison strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so native versus emulated tuple translation is explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.OrderedStringAggregate => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Ordered string aggregate strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so native versus emulated aggregate translation is explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.RegexTranslation => PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Regular-expression translation strategy requires the concrete nORM provider implementation profile.",
                    "Use DecideProviderImplementationProfile when certifying concrete targets so native, UDF-backed, and unsupported regex paths are explicit.",
                    capabilities.MinimumServerVersion,
                    null),
                ProviderMobilityProviderFeature.IdentifierEscaping
                    or ProviderMobilityProviderFeature.ParameterBinding
                    or ProviderMobilityProviderFeature.PagingTranslation
                    or ProviderMobilityProviderFeature.BooleanPredicateTranslation
                    or ProviderMobilityProviderFeature.NullSemantics
                    or ProviderMobilityProviderFeature.LikeEscapeTranslation
                    or ProviderMobilityProviderFeature.StringConcatTranslation
                    or ProviderMobilityProviderFeature.TypeConversionTranslation
                    or ProviderMobilityProviderFeature.StringPredicateTranslation
                    or ProviderMobilityProviderFeature.CharacterTranslation
                    or ProviderMobilityProviderFeature.FormattingTranslation
                    or ProviderMobilityProviderFeature.DateTimeComparisonNormalization
                    or ProviderMobilityProviderFeature.DecimalComparisonNormalization
                    or ProviderMobilityProviderFeature.TimeSpanComparisonNormalization
                    or ProviderMobilityProviderFeature.TemporalConstructionTranslation
                    or ProviderMobilityProviderFeature.TemporalArithmeticTranslation
                    or ProviderMobilityProviderFeature.TemporalClockSource
                    or ProviderMobilityProviderFeature.GeneratedKeyRetrieval
                    or ProviderMobilityProviderFeature.InsertOrIgnoreTranslation
                    or ProviderMobilityProviderFeature.CudSubqueryRewrite
                    or ProviderMobilityProviderFeature.BitwiseXorTranslation
                    or ProviderMobilityProviderFeature.WindowFunctionTranslation
                    or ProviderMobilityProviderFeature.CaseSensitiveStringComparison
                    or ProviderMobilityProviderFeature.SqlStatementLengthLimit => PD(
                        capabilities.ProviderName,
                        feature,
                        ProviderMobilitySupport.Emulated,
                        true,
                        "Warning",
                        $"{feature} strategy requires the concrete nORM provider implementation profile.",
                        "Use DecideProviderImplementationProfile when certifying concrete targets so dialect-specific rewrites are explicit.",
                        capabilities.MinimumServerVersion,
                        null),
                _ => throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown provider target feature.")
            };
        }

        /// <summary>
        /// Builds a provider target capability profile from a provider descriptor.
        /// </summary>
        public static IReadOnlyList<ProviderMobilityProviderDecision> DecideProviderCapabilityProfile(
            ProviderCapabilities capabilities,
            Version? actualServerVersion = null,
            bool requireActualServerVersion = false)
        {
            ArgumentNullException.ThrowIfNull(capabilities);

            var features = Enum.GetValues<ProviderMobilityProviderFeature>();
            var decisions = new List<ProviderMobilityProviderDecision>(features.Length);
            foreach (var feature in features)
            {
                if (feature == ProviderMobilityProviderFeature.ServerVersion)
                {
                    decisions.Add(requireActualServerVersion || actualServerVersion != null
                        ? DecideProviderVersion(capabilities, actualServerVersion)
                        : BuildDescriptorOnlyProviderVersionDecision(capabilities));
                    continue;
                }

                var decision = DecideProviderCapability(capabilities, feature);
                decisions.Add(actualServerVersion is not null && decision.ActualServerVersion is null
                    ? WithActualServerVersion(decision, actualServerVersion)
                    : decision);
            }

            return decisions;
        }

        private static ProviderMobilityProviderDecision CapabilityBool(
            ProviderCapabilities capabilities,
            ProviderMobilityProviderFeature feature,
            bool supported,
            string supportedReason,
            string unsupportedReason,
            string unsupportedFix)
            => supported
                ? PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    supportedReason,
                    "Keep provider capability tests and live-provider parity evidence current.",
                    capabilities.MinimumServerVersion,
                    null)
                : PD(
                    capabilities.ProviderName,
                    feature,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    unsupportedReason,
                    unsupportedFix,
                    capabilities.MinimumServerVersion,
                    null);
    }
}
