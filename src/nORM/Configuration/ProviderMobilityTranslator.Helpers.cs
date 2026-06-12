using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static void Replace(List<ProviderMobilityProviderDecision> decisions, ProviderMobilityProviderDecision decision)
        {
            var index = decisions.FindIndex(existing => existing.Feature == decision.Feature);
            if (index >= 0)
                decisions[index] = decision;
            else
                decisions.Add(decision);
        }

        private static ProviderMobilityProviderDecision PD(
            string providerName,
            ProviderMobilityProviderFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix,
            Version? minimumServerVersion,
            Version? actualServerVersion)
            => new(providerName, feature, support, strictRuntimeAllowed, certificationSeverity, reason, suggestedFix, minimumServerVersion, actualServerVersion);
    }
}
