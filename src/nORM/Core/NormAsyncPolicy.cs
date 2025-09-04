using System;

namespace nORM.Core
{
    /// <summary>
    /// Provides configuration for async context flow within nORM.
    /// </summary>
    public static class NormAsyncPolicy
    {
        /// <summary>
        /// Determines whether execution context flow should be suppressed for library async operations.
        /// Set environment variable NORM_PRESERVE_CONTEXT to "true" to preserve context.
        /// </summary>
        public static readonly bool SuppressExecutionContextFlow =
            !Environment.GetEnvironmentVariable("NORM_PRESERVE_CONTEXT")?.Equals("true", StringComparison.OrdinalIgnoreCase) == true;
    }
}
