using System;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        /// <summary>
        /// Decides whether a provider target satisfies its declared minimum server version.
        /// </summary>
        public static ProviderMobilityProviderDecision DecideProviderVersion(
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            ArgumentNullException.ThrowIfNull(capabilities);

            var minimum = capabilities.MinimumServerVersion;
            if (minimum == null)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Warning",
                    $"{capabilities.ProviderName} does not declare a minimum server version, so nORM cannot certify a version floor for this provider target.",
                    "Declare ProviderCapabilities.MinimumServerVersion for release-certified providers or keep the target as a reviewed compatibility profile.",
                    null,
                    actualServerVersion);
            }

            if (actualServerVersion == null)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} server version could not be determined during provider startup validation.",
                    $"Use a provider/driver that exposes the server version or verify the target manually before certification. Minimum supported version is {minimum}.",
                    minimum,
                    null);
            }

            if (actualServerVersion < minimum)
            {
                return PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.Unsupported,
                    false,
                    "Error",
                    $"{capabilities.ProviderName} server version {actualServerVersion} is not supported. Minimum supported version is {minimum}.",
                    "Upgrade the database server or target a provider/version profile whose minimum version is satisfied.",
                    minimum,
                    actualServerVersion);
            }

            return PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.ServerVersion,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} server version {actualServerVersion} satisfies the minimum supported version {minimum}.",
                "Keep startup validation enabled for each configured provider target.",
                minimum,
                actualServerVersion);
        }

        /// <summary>
        /// Parses a provider-supplied server version string into a comparable version.
        /// </summary>
        public static Version? ParseProviderVersion(string? versionText)
        {
            if (string.IsNullOrWhiteSpace(versionText))
                return null;

            var text = versionText.Trim();
            var start = -1;
            for (var i = 0; i < text.Length; i++)
            {
                if (char.IsDigit(text[i]))
                {
                    start = i;
                    break;
                }
            }

            if (start < 0)
                return null;

            var end = start;
            while (end < text.Length && (char.IsDigit(text[end]) || text[end] == '.'))
                end++;

            var candidate = text[start..end].Trim('.');
            if (candidate.Length == 0)
                return null;

            var parts = candidate.Split('.', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 1)
                candidate += ".0";

            return Version.TryParse(candidate, out var version) ? version : null;
        }

        internal static string BuildProviderVersionViolationMessage(
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            var decision = DecideProviderVersion(capabilities, actualServerVersion);
            return $"{decision.Reason} {decision.SuggestedFix}";
        }

        private static ProviderMobilityProviderDecision WithActualServerVersion(
            ProviderMobilityProviderDecision decision,
            Version actualServerVersion)
            => new(
                decision.ProviderName,
                decision.Feature,
                decision.Support,
                decision.StrictRuntimeAllowed,
                decision.CertificationSeverity,
                decision.Reason,
                decision.SuggestedFix,
                decision.MinimumServerVersion,
                actualServerVersion);

        private static ProviderMobilityProviderDecision BuildDescriptorOnlyProviderVersionDecision(ProviderCapabilities capabilities)
        {
            var minimum = capabilities.MinimumServerVersion;
            return minimum == null
                ? PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.ProviderBound,
                    true,
                    "Warning",
                    $"{capabilities.ProviderName} does not declare a minimum server version, so nORM cannot certify a version floor for this provider target.",
                    "Declare ProviderCapabilities.MinimumServerVersion for release-certified providers or keep the target as a reviewed compatibility profile.",
                    null,
                    null)
                : PD(
                    capabilities.ProviderName,
                    ProviderMobilityProviderFeature.ServerVersion,
                    ProviderMobilitySupport.Portable,
                    true,
                    "Info",
                    $"{capabilities.ProviderName} declares minimum supported server version {minimum}. No live target was opened, so this is descriptor evidence rather than actual server-version evidence.",
                    "Pass the provider connection string to certification when a release artifact must prove the actual connected server version.",
                    minimum,
                    null);
        }
    }
}
