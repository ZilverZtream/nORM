using System;
using System.Collections.Generic;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static void AddWriteAndExpressionImplementationDecisions(
            List<ProviderMobilityProviderDecision> decisions,
            DatabaseProvider provider,
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.GeneratedKeyRetrieval,
                provider.SupportsCommandGeneratedKeyRetrieval ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                "Info",
                provider.SupportsCommandGeneratedKeyRetrieval
                    ? $"{capabilities.ProviderName} can retrieve generated keys from command execution."
                    : $"{capabilities.ProviderName} uses provider identity-returning SQL or follow-up identity retrieval where required.",
                "Keep single-insert generated-key parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, BuildInsertOrIgnoreDecision(provider, actualServerVersion));
            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.CudSubqueryRewrite,
                provider.CudWhereInSubqueryNeedsDoubleWrap ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                provider.CudWhereInSubqueryNeedsDoubleWrap ? "Warning" : "Info",
                provider.CudWhereInSubqueryNeedsDoubleWrap
                    ? $"{capabilities.ProviderName} needs a generated double-wrap around self-referencing CUD subqueries."
                    : $"{capabilities.ProviderName} accepts nORM's standard generated CUD subquery shape.",
                "Keep ExecuteUpdate/ExecuteDelete, bulk CUD, and tenant-write parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.BitwiseXorTranslation,
                IsAlgebraicXorRewrite(provider.GetBitwiseXorSql("left", "right"))
                    ? ProviderMobilitySupport.Emulated
                    : ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} bitwise XOR is translated as {provider.GetBitwiseXorSql("left", "right")}.",
                "Keep bitwise operator parity tests current for generated LINQ.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.WindowFunctionTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} is within nORM's supported window-function floor for ROW_NUMBER and aggregate-over-partition translation.",
                "Keep GroupBy, first/last per group, paging, and window-function parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.CaseSensitiveStringComparison,
                provider.ForceCaseSensitiveStringComparison("value").Equals("value", StringComparison.Ordinal)
                    ? ProviderMobilitySupport.Portable
                    : ProviderMobilitySupport.Emulated,
                true,
                "Info",
                provider.ForceCaseSensitiveStringComparison("value").Equals("value", StringComparison.Ordinal)
                    ? $"{capabilities.ProviderName} default string comparison is already case-sensitive for generated equality/IN semantics."
                    : $"{capabilities.ProviderName} applies provider-specific case-sensitive comparison wrapping ({provider.ForceCaseSensitiveStringComparison("value")}).",
                "Keep string equality and IN case-sensitivity parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.SqlStatementLengthLimit,
                provider.MaxSqlLength == int.MaxValue ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                "Info",
                provider.MaxSqlLength == int.MaxValue
                    ? $"{capabilities.ProviderName} does not expose a bounded SQL statement length through nORM's provider contract."
                    : $"{capabilities.ProviderName} limits individual SQL statements to {provider.MaxSqlLength} characters; generated SQL must split before reaching that bound.",
                "Keep generated SQL batching/splitting tests current for large IN lists, bulk operations, and migrations.",
                capabilities.MinimumServerVersion,
                actualServerVersion));
        }
    }
}
