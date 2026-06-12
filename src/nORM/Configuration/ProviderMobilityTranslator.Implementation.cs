using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        /// <summary>
        /// Builds a provider target profile from the concrete nORM provider implementation.
        /// This includes capability descriptors plus provider SQL translation strategies
        /// that are not exposed on <see cref="ProviderCapabilities"/>.
        /// </summary>
        public static IReadOnlyList<ProviderMobilityProviderDecision> DecideProviderImplementationProfile(
            DatabaseProvider provider,
            Version? actualServerVersion = null,
            bool requireActualServerVersion = false)
        {
            ArgumentNullException.ThrowIfNull(provider);

            var capabilities = provider.Capabilities;
            var decisions = DecideProviderCapabilityProfile(
                capabilities,
                actualServerVersion,
                requireActualServerVersion).ToList();

            AddProviderFeatureStrategyDecisions(decisions, provider, capabilities, actualServerVersion);
            AddSqlShapeImplementationDecisions(decisions, provider, capabilities, actualServerVersion);
            AddTemporalImplementationDecisions(decisions, provider, capabilities, actualServerVersion);
            AddWriteAndExpressionImplementationDecisions(decisions, provider, capabilities, actualServerVersion);

            return decisions
                .OrderBy(static decision => decision.Feature.ToString(), StringComparer.Ordinal)
                .ToArray();
        }
    }
}
