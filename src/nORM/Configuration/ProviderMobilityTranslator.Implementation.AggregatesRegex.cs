using System;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static ProviderMobilityProviderDecision BuildInsertOrIgnoreDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var sql = provider.GetInsertOrIgnoreSql("JoinTable", "LeftId", "RightId", "@p0", "@p1");
            var emulated = provider is SqlServerProvider;

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.InsertOrIgnoreTranslation,
                emulated ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                emulated ? "Warning" : "Info",
                emulated
                    ? $"{capabilities.ProviderName} preserves insert-or-ignore semantics through generated IF NOT EXISTS SQL ({sql})."
                    : $"{capabilities.ProviderName} uses a native idempotent insert primitive ({sql}).",
                "Keep many-to-many sync and idempotent join insert parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildOrderedStringAggregateDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            if (!provider.SupportsNativeOrderedStringAggregate)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.OrderedStringAggregate,
                    ProviderMobilitySupport.Emulated,
                    true,
                    "Warning",
                    "Provider lacks native ordered string aggregate support; nORM must preserve aggregate semantics through provider-specific rewrites or bounded fallback behavior.",
                    "Keep string aggregate ordering caveats documented and covered by provider parity tests.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }

            var featureMinimum = provider is SqlServerProvider ? new Version(14, 0) : capabilities.MinimumServerVersion;
            if (featureMinimum != null && actualServerVersion != null && actualServerVersion < featureMinimum)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.OrderedStringAggregate,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} ordered string aggregate translation requires server version {featureMinimum} or newer; actual version is {actualServerVersion}.",
                    "Upgrade the target server or avoid ordered string aggregate LINQ shapes for this provider target.",
                    featureMinimum,
                    actualServerVersion);
            }

            if (featureMinimum != null && actualServerVersion == null && featureMinimum > (capabilities.MinimumServerVersion ?? featureMinimum))
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.OrderedStringAggregate,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Warning",
                    $"{capabilities.ProviderName} ordered string aggregate translation requires server version {featureMinimum} or newer, which is above the provider floor {capabilities.MinimumServerVersion}.",
                    "Pass a live target connection to certification before relying on ordered string aggregate translation for this provider target.",
                    featureMinimum,
                    null);
            }

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.OrderedStringAggregate,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                featureMinimum != null
                    ? $"Provider supports native ordered string aggregate translation at server version {actualServerVersion ?? featureMinimum}."
                    : "Provider supports native ordered string aggregate translation.",
                "Keep aggregate ordering parity tests current.",
                featureMinimum,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildRegexTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            try
            {
                var matchSql = provider.GetRegexMatchSql("value", "'^[A-Z]'");
                var replaceSql = provider.GetRegexReplaceSql("value", "'[0-9]+'", "'#'");
                var sqliteManagedBacked = provider is SqliteProvider;
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.RegexTranslation,
                    sqliteManagedBacked ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    sqliteManagedBacked
                        ? $"{capabilities.ProviderName} emits regex SQL ({matchSql}; {replaceSql}) backed by deterministic nORM-registered managed SQLite functions."
                        : $"{capabilities.ProviderName} translates Regex.IsMatch/Regex.Replace through provider regex primitives ({matchSql}; {replaceSql}).",
                    sqliteManagedBacked
                        ? "Keep SQLite managed regex function registration and live-provider parity tests current."
                        : "Keep Regex.IsMatch/Regex.Replace shape and live-provider parity tests current.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
            catch (NormUnsupportedFeatureException ex)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.RegexTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Warning",
                    ex.Message,
                    $"Avoid Regex LINQ shapes for {capabilities.ProviderName}, rewrite simple patterns to supported string predicates, or add an explicit provider-owned regex function with live parity tests.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
        }
    }
}
