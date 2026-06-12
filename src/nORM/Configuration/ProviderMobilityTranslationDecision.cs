using System;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// A single provider-mobility classification used by strict runtime checks,
    /// static certification and documentation.
    /// </summary>
    public sealed class ProviderMobilityTranslationDecision
    {
        /// <summary>
        /// Creates a provider-mobility translation decision.
        /// </summary>
        public ProviderMobilityTranslationDecision(
            ProviderMobilityFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix)
        {
            Feature = feature;
            Support = support;
            StrictRuntimeAllowed = strictRuntimeAllowed;
            CertificationSeverity = certificationSeverity ?? throw new ArgumentNullException(nameof(certificationSeverity));
            Reason = reason ?? throw new ArgumentNullException(nameof(reason));
            SuggestedFix = suggestedFix ?? throw new ArgumentNullException(nameof(suggestedFix));
        }

        /// <summary>Feature being classified.</summary>
        public ProviderMobilityFeature Feature { get; }

        /// <summary>Provider mobility support class.</summary>
        public ProviderMobilitySupport Support { get; }

        /// <summary>Whether strict runtime mode may execute this feature.</summary>
        public bool StrictRuntimeAllowed { get; }

        /// <summary>Static certification severity: Error, Warning, or Info.</summary>
        public string CertificationSeverity { get; }

        /// <summary>Short explanation of the portability decision.</summary>
        public string Reason { get; }

        /// <summary>Recommended path to make or keep the code provider-mobile.</summary>
        public string SuggestedFix { get; }
    }
}
