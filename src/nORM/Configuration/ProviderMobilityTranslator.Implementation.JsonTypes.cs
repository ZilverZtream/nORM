using System;
using System.Linq;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static bool IsExpandedNullSemantics(string sql)
            => sql.Contains(" OR ", StringComparison.OrdinalIgnoreCase) &&
               sql.Contains(" IS NULL", StringComparison.OrdinalIgnoreCase);

        private static bool IsAlgebraicXorRewrite(string sql)
            => sql.Contains("|", StringComparison.Ordinal) &&
               sql.Contains("&", StringComparison.Ordinal) &&
               sql.Contains("-", StringComparison.Ordinal);

        private static ProviderMobilityProviderDecision BuildJsonPathTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            try
            {
                var jsonSql = provider.TranslateJsonPathAccess("doc", "$.customer.name");
                try
                {
                    _ = provider.TranslateJsonPathAccess("doc", "$.bad'value");
                }
                catch (ArgumentException)
                {
                    return PD(
                        capabilities.ProviderName,
                        ProviderMobilityProviderFeature.JsonPathTranslation,
                        ProviderMobilitySupport.Portable,
                        true,
                        "Info",
                        $"{capabilities.ProviderName} translates JSON path access through provider-owned SQL ({jsonSql}) and rejects unsafe path text.",
                        "Keep JSON path validation and live-provider JSON parity tests current.",
                        capabilities.MinimumServerVersion,
                        actualServerVersion);
                }

                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.JsonPathTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} JSON path translation did not reject unsafe path text during capability probing.",
                    "Harden provider JSON path validation before certifying JSON query shapes for this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
            catch (Exception ex) when (ex is ArgumentException or NormUnsupportedFeatureException)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.JsonPathTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    ex.Message,
                    "Add provider JSON path translation and validation tests before certifying JSON query shapes for this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
        }

        private static ProviderMobilityProviderDecision BuildTypeConversionDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var conversions = new[]
            {
                provider.GetToStringSql("value"),
                provider.GetIntCastSql("value"),
                provider.GetIntCastSql("value", asLong: true),
                provider.GetRealCastSql("value"),
                provider.GetRealCastSql("value", asDecimal: true),
                provider.GetBoolCastSql("value"),
                provider.GetTruncateToIntSql("value")
            };

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.TypeConversionTranslation,
                conversions.All(static sql => !string.IsNullOrWhiteSpace(sql))
                    ? ProviderMobilitySupport.Portable
                    : ProviderMobilitySupport.Unsupported,
                conversions.All(static sql => !string.IsNullOrWhiteSpace(sql)),
                conversions.All(static sql => !string.IsNullOrWhiteSpace(sql)) ? "Info" : "Error",
                $"{capabilities.ProviderName} exposes provider-owned CAST/conversion SQL for string, integer, real, decimal, boolean, and truncation paths.",
                "Keep Convert.*, casts, enum casts, ToString, and numeric conversion parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildStringPredicateDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var emptySql = provider.IsNullOrEmptySql("value");
            var sqlServerByteExact = provider is SqlServerProvider;

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.StringPredicateTranslation,
                sqlServerByteExact ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                sqlServerByteExact ? "Warning" : "Info",
                sqlServerByteExact
                    ? $"{capabilities.ProviderName} uses byte-exact empty-string SQL ({emptySql}) to avoid SQL Server trailing-space equality drift."
                    : $"{capabilities.ProviderName} translates string null/empty predicates through provider-owned SQL ({emptySql}).",
                "Keep IsNullOrEmpty, IsNullOrWhiteSpace, Length, Contains, StartsWith, EndsWith, and trim predicate parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildCharacterTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var codeSql = provider.GetCharCodeSql("ch");
            var charSql = provider.GetCharFromCodeSql("65");

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.CharacterTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} translates character classification through provider-owned code-point SQL ({codeSql}; {charSql}).",
                "Keep char.IsDigit/IsLetter/IsWhiteSpace/IsControl/IsPunctuation/IsSymbol parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }

        private static ProviderMobilityProviderDecision BuildFormattingTranslationDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var decimalSql = provider.FormatFixedDecimalSql("amount", 2);
            var dateSql = provider.FormatDateUsingDotNetPattern("stamp", "yyyy-MM-dd");
            var supported = !string.IsNullOrWhiteSpace(decimalSql) && !string.IsNullOrWhiteSpace(dateSql);

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.FormattingTranslation,
                supported ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Unsupported,
                supported,
                supported ? "Info" : "Error",
                supported
                    ? $"{capabilities.ProviderName} translates fixed decimal and date/time format strings through provider-owned SQL ({decimalSql}; {dateSql})."
                    : $"{capabilities.ProviderName} does not expose both fixed decimal and supported date/time format translation.",
                "Keep ToString(format) parity tests for numeric and temporal values current; add provider mappings before certifying new format tokens.",
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }
    }
}
