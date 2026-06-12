#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Describes how nORM can carry a feature across supported providers.
    /// </summary>
    public enum ProviderMobilitySupport
    {
        /// <summary>
        /// nORM can translate the feature to each supported provider with the
        /// same observable behavior.
        /// </summary>
        Portable,

        /// <summary>
        /// nORM can preserve behavior by rewriting, simulating, or using a
        /// bounded fallback rather than a single provider-native primitive.
        /// </summary>
        Emulated,

        /// <summary>
        /// The feature exposes caller-authored provider language or provider
        /// handles. It can exist in compatibility mode but cannot be certified
        /// as provider-mobile generated surface.
        /// </summary>
        ProviderBound,

        /// <summary>
        /// nORM cannot preserve semantics safely and must fail deterministically.
        /// </summary>
        Unsupported
    }
}
