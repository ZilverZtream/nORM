using System;
using System.Collections.Generic;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static void AddProviderFeatureStrategyDecisions(
            List<ProviderMobilityProviderDecision> decisions,
            DatabaseProvider provider,
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.RowTupleComparison,
                provider.SupportsRowTupleComparison ? ProviderMobilitySupport.Portable : ProviderMobilitySupport.Emulated,
                true,
                provider.SupportsRowTupleComparison ? "Info" : "Warning",
                provider.SupportsRowTupleComparison
                    ? "Provider supports native row-value tuple comparison for generated keyset/tuple predicates."
                    : "Provider lacks native row-value tuple comparison; nORM rewrites tuple semantics into provider-compatible predicates.",
                provider.SupportsRowTupleComparison
                    ? "Keep tuple comparison live parity tests current."
                    : "Keep emulated tuple comparison covered by provider parity tests and avoid claiming native tuple comparison for this target.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, BuildOrderedStringAggregateDecision(provider, actualServerVersion));
            Replace(decisions, BuildRegexTranslationDecision(provider, actualServerVersion));
            Replace(decisions, BuildJsonPathTranslationDecision(provider, actualServerVersion));
        }
    }
}
