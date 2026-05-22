using System;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Describes stable provider capabilities and minimum server requirements.
    /// </summary>
    public sealed class ProviderCapabilities
    {
        /// <summary>
        /// Initializes a new provider capability descriptor.
        /// </summary>
        public ProviderCapabilities(
            string providerName,
            Version? minimumServerVersion,
            int maxParameters,
            bool supportsJson,
            bool supportsTemporalVersioning,
            bool supportsNativeBulkInsert,
            bool supportsSavepoints,
            string notes)
        {
            ProviderName = providerName ?? throw new ArgumentNullException(nameof(providerName));
            MinimumServerVersion = minimumServerVersion;
            MaxParameters = maxParameters;
            SupportsJson = supportsJson;
            SupportsTemporalVersioning = supportsTemporalVersioning;
            SupportsNativeBulkInsert = supportsNativeBulkInsert;
            SupportsSavepoints = supportsSavepoints;
            Notes = notes ?? string.Empty;
        }

        /// <summary>Human-readable provider name.</summary>
        public string ProviderName { get; }

        /// <summary>Minimum supported server version, or null when unknown.</summary>
        public Version? MinimumServerVersion { get; }

        /// <summary>Maximum provider-supported parameters in one command.</summary>
        public int MaxParameters { get; }

        /// <summary>Whether nORM exposes JSON translation support for this provider.</summary>
        public bool SupportsJson { get; }

        /// <summary>Whether nORM temporal versioning is supported for this provider.</summary>
        public bool SupportsTemporalVersioning { get; }

        /// <summary>Whether nORM has a provider-native bulk insert path.</summary>
        public bool SupportsNativeBulkInsert { get; }

        /// <summary>Whether transaction savepoints are supported.</summary>
        public bool SupportsSavepoints { get; }

        /// <summary>Provider-specific dependency or limitation notes.</summary>
        public string Notes { get; }
    }
}
