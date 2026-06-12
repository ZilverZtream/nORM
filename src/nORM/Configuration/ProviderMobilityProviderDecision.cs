using System;

#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// A provider/version/capability decision used by certification reports,
    /// startup validation, and provider capability documentation.
    /// </summary>
    public sealed class ProviderMobilityProviderDecision
    {
        /// <summary>
        /// Creates a provider target capability decision.
        /// </summary>
        public ProviderMobilityProviderDecision(
            string providerName,
            ProviderMobilityProviderFeature feature,
            ProviderMobilitySupport support,
            bool strictRuntimeAllowed,
            string certificationSeverity,
            string reason,
            string suggestedFix,
            Version? minimumServerVersion,
            Version? actualServerVersion)
        {
            ProviderName = providerName ?? throw new ArgumentNullException(nameof(providerName));
            Feature = feature;
            Support = support;
            StrictRuntimeAllowed = strictRuntimeAllowed;
            CertificationSeverity = certificationSeverity ?? throw new ArgumentNullException(nameof(certificationSeverity));
            Reason = reason ?? throw new ArgumentNullException(nameof(reason));
            SuggestedFix = suggestedFix ?? throw new ArgumentNullException(nameof(suggestedFix));
            MinimumServerVersion = minimumServerVersion;
            ActualServerVersion = actualServerVersion;
        }

        /// <summary>Provider being classified.</summary>
        public string ProviderName { get; }

        /// <summary>Provider target feature being classified.</summary>
        public ProviderMobilityProviderFeature Feature { get; }

        /// <summary>Provider mobility support class.</summary>
        public ProviderMobilitySupport Support { get; }

        /// <summary>Whether strict runtime mode may use this provider target capability.</summary>
        public bool StrictRuntimeAllowed { get; }

        /// <summary>Certification severity: Error, Warning, or Info.</summary>
        public string CertificationSeverity { get; }

        /// <summary>Short explanation of the provider target decision.</summary>
        public string Reason { get; }

        /// <summary>Recommended remediation or review path.</summary>
        public string SuggestedFix { get; }

        /// <summary>Minimum server version required by the provider, when declared.</summary>
        public Version? MinimumServerVersion { get; }

        /// <summary>Actual connected server version, when known.</summary>
        public Version? ActualServerVersion { get; }
    }
}
