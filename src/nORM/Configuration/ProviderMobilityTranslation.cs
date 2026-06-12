using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Central provider-mobility translation classifier. This is the contract
    /// nORM uses to decide whether a feature is translated, emulated, explicitly
    /// provider-bound, or unsupported.
    /// </summary>
    public static partial class ProviderMobilityTranslator
    {
        private static readonly IReadOnlyDictionary<ProviderMobilityFeature, ProviderMobilityTranslationDecision> s_decisions =
            BuildDecisions().ToDictionary(static d => d.Feature);

        /// <summary>All known provider-mobility translation decisions.</summary>
        public static IReadOnlyList<ProviderMobilityTranslationDecision> Decisions { get; } =
            s_decisions.Values.OrderBy(static d => d.Feature.ToString(), StringComparer.Ordinal).ToArray();

        /// <summary>
        /// Gets the provider-mobility decision for a known feature.
        /// </summary>
        public static ProviderMobilityTranslationDecision Decide(ProviderMobilityFeature feature)
            => s_decisions.TryGetValue(feature, out var decision)
                ? decision
                : throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown provider mobility feature.");
    }
}
