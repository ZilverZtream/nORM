using System;
using System.Collections.Generic;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static void AddTemporalImplementationDecisions(
            List<ProviderMobilityProviderDecision> decisions,
            DatabaseProvider provider,
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            Replace(decisions, BuildNormalizationDecision(
                capabilities,
                actualServerVersion,
                ProviderMobilityProviderFeature.DateTimeComparisonNormalization,
                provider.NormalizeDateTimeForCompare("value"),
                "DateTime comparison",
                "Keep mixed-offset DateTime comparison parity tests current."));

            Replace(decisions, BuildNormalizationDecision(
                capabilities,
                actualServerVersion,
                ProviderMobilityProviderFeature.DecimalComparisonNormalization,
                provider.NormalizeDecimalForCompare("value"),
                "decimal comparison/sort/aggregate",
                provider.NormalizeDecimalForCompare("value").Equals("value", StringComparison.Ordinal)
                    ? "Keep decimal comparison and aggregate parity tests current."
                    : "SQLite decimal normalization uses REAL for numeric semantics; keep precision caveats documented and tolerance tests current.",
                warnWhenEmulated: provider.NormalizeDecimalForCompare("value") != "value"));

            Replace(decisions, BuildNormalizationDecision(
                capabilities,
                actualServerVersion,
                ProviderMobilityProviderFeature.TimeSpanComparisonNormalization,
                provider.NormalizeTimeSpanForCompare("value"),
                "TimeSpan comparison",
                "Keep cross-column and mixed-magnitude TimeSpan comparison parity tests current."));

            Replace(decisions, BuildTemporalConstructionDecision(provider, actualServerVersion));
            Replace(decisions, BuildTemporalArithmeticDecision(provider, actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.TemporalClockSource,
                provider.UsesDatabaseClockForTemporalTags ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                provider.UsesDatabaseClockForTemporalTags ? "Info" : "Warning",
                provider.UsesDatabaseClockForTemporalTags
                    ? $"{capabilities.ProviderName} temporal tags and history triggers use the database clock for consistent AsOf windows."
                    : $"{capabilities.ProviderName} temporal tag timestamps are caller-bound while history may use provider execution time.",
                provider.UsesDatabaseClockForTemporalTags
                    ? "Keep temporal tag and AsOf live-provider parity tests current."
                    : "Prefer a database-clock tag implementation before certifying temporal history for this provider target.",
                capabilities.MinimumServerVersion,
                actualServerVersion));
        }

        private static ProviderMobilityProviderDecision BuildTemporalConstructionDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            try
            {
                var dateTime = provider.GetDateTimeFromPartsSql("y", "m", "d");
                _ = provider.GetDateTimeFromPartsSql("y", "m", "d", "hh", "mm", "ss");
                _ = provider.GetDateTimeFromPartsSql("y", "m", "d", "hh", "mm", "ss", "ms");
                _ = provider.GetDateOnlyFromPartsSql("y", "m", "d");
                var timeOnly = provider.GetTimeOnlyFromPartsSql("hh", "mm", "ss");
                _ = provider.GetTimeOnlyFromPartsSql("hh", "mm", "ss", "ms");
                _ = provider.GetDateTimeOffsetFromPartsSql("y", "m", "d", "hh", "mm", "ss", TimeSpan.Zero);

                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.TemporalConstructionTranslation,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    $"{capabilities.ProviderName} exposes provider-owned DateTime/DateOnly/TimeOnly/DateTimeOffset from-parts translation ({dateTime}; {timeOnly}).",
                    "Keep DateTime/DateOnly/TimeOnly/DateTimeOffset from-parts parity tests current.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
            catch (NormUnsupportedFeatureException ex)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.TemporalConstructionTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    ex.Message,
                    "Add provider from-parts translations and live/provider-shape tests before certifying temporal construction on this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }
        }

        private static ProviderMobilityProviderDecision BuildTemporalArithmeticDecision(
            DatabaseProvider provider,
            Version? actualServerVersion)
        {
            var capabilities = provider.Capabilities;
            var missing = new List<string>();

            Require("DateOnly.AddDays", provider.AddDaysToDateOnlySql("d", "1"));
            Require("DateOnly.AddMonths", provider.AddMonthsToDateOnlySql("d", "1"));
            Require("DateOnly.AddYears", provider.AddYearsToDateOnlySql("d", "1"));
            Require("TimeOnly.AddSeconds", provider.AddSecondsToTimeOnlySql("t", "1"));
            Require("TimeOnly +/- TimeSpan column", provider.AddTimeSpanColumnToTimeOnlySql("t", "span", subtract: false));
            Require("DateTime +/- TimeSpan column", provider.AddTimeSpanColumnToDateTimeSql("dt", "span", subtract: false));
            Require("DateTimeOffset +/- TimeSpan column", provider.AddTimeSpanColumnToDateTimeOffsetSql("dto", "span", subtract: false));

            try
            {
                _ = provider.GetDateTimeDifferenceSecondsSql("end", "start");
                _ = provider.GetTimeOnlyDifferenceSecondsSql("end", "start");
                _ = provider.GetTimeSpanColumnSecondsSql("span");
                _ = provider.GetDateTimeOffsetWithOffsetSql("dto", TimeSpan.Zero);
                _ = provider.GetDateTimeOffsetLocalDateTimeSql("dto", TimeSpan.Zero);
                _ = provider.GetDateTimeOffsetUtcEpochSecondsSql("dto");
            }
            catch (NormUnsupportedFeatureException ex)
            {
                missing.Add(ex.Message);
            }

            if (missing.Count > 0)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.TemporalArithmeticTranslation,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} is missing temporal arithmetic translations: {string.Join("; ", missing)}.",
                    "Add the missing provider hooks and live/provider-shape tests before certifying temporal arithmetic on this target.",
                    capabilities.MinimumServerVersion,
                    actualServerVersion);
            }

            var sqliteTextArithmetic = provider is SqliteProvider;
            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.TemporalArithmeticTranslation,
                sqliteTextArithmetic ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                sqliteTextArithmetic ? "Warning" : "Info",
                sqliteTextArithmetic
                    ? $"{capabilities.ProviderName} preserves temporal arithmetic through generated TEXT/julianday/strftime/substr normalization."
                    : $"{capabilities.ProviderName} preserves temporal arithmetic through provider-native date/time/interval primitives.",
                sqliteTextArithmetic
                    ? "Keep SQLite temporal precision caveats documented and tolerance tests current."
                    : "Keep temporal arithmetic live-provider parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion);

            void Require(string label, string? sql)
            {
                if (string.IsNullOrWhiteSpace(sql))
                    missing.Add(label);
            }
        }

        private static ProviderMobilityProviderDecision BuildNormalizationDecision(
            ProviderCapabilities capabilities,
            Version? actualServerVersion,
            ProviderMobilityProviderFeature feature,
            string translatedSql,
            string label,
            string suggestedFix,
            bool warnWhenEmulated = false)
        {
            var isIdentity = translatedSql.Equals("value", StringComparison.Ordinal);
            return PD(
                capabilities.ProviderName,
                feature,
                isIdentity ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                !isIdentity && warnWhenEmulated ? "Warning" : "Info",
                isIdentity
                    ? $"{capabilities.ProviderName} uses native {label} semantics without generated normalization."
                    : $"{capabilities.ProviderName} normalizes {label} with provider SQL: {translatedSql}.",
                suggestedFix,
                capabilities.MinimumServerVersion,
                actualServerVersion);
        }
    }
}
