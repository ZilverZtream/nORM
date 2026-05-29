namespace nORM.Configuration
{
    /// <summary>
    /// Selects the storage engine used by temporal versioning.
    /// </summary>
    public enum TemporalStorageMode
    {
        /// <summary>
        /// Use nORM-managed history tables and provider triggers/functions.
        /// This is the provider-neutral v1 default.
        /// </summary>
        NormManaged = 0,

        /// <summary>
        /// Use provider-native temporal storage when the database provider supports it.
        /// Unsupported providers fail closed during context initialization.
        /// </summary>
        ProviderNative = 1
    }
}
