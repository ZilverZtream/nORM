using System;

namespace nORM.Core
{
    /// <summary>
    /// Configuration settings for <see cref="ConnectionPool"/>.
    /// </summary>
    /// <remarks>
    /// ARCHITECTURAL WARNING (TASK 5): Configuring custom pooling creates double-pooling
    /// when combined with provider-level pooling (SQL Server, PostgreSQL, MySQL all have built-in pools).
    ///
    /// For optimal performance, use provider connection string pooling parameters instead:
    /// - SQL Server: "Max Pool Size=100;Min Pool Size=10;Connection Lifetime=0;"
    /// - PostgreSQL: "Maximum Pool Size=100;Minimum Pool Size=10;Connection Lifetime=0;"
    /// - MySQL: "Maximum Pool Size=100;Minimum Pool Size=10;Connection Lifetime=0;"
    ///
    /// See <see cref="ConnectionPool"/> remarks for detailed migration guidance.
    /// </remarks>
    [Obsolete("Consider configuring provider-level pooling via connection string parameters instead. " +
              "Custom pooling options will be evaluated for deprecation in future versions.")]
    public class ConnectionPoolOptions
    {
        /// <summary>
        /// Gets or sets the maximum number of connections that can be created by the pool.
        /// </summary>
        /// <remarks>
        /// WARNING (TASK 5): This limits nORM's custom pool, but the provider's connection string
        /// may also specify "Max Pool Size" (SQL Server) or "Maximum Pool Size" (PostgreSQL/MySQL).
        /// Actual connection count = nORM MaxPoolSize × Provider Max Pool Size, leading to resource waste.
        /// </remarks>
        public int MaxPoolSize { get; set; } = Environment.ProcessorCount * 2;

        /// <summary>
        /// Gets or sets the minimum number of idle connections the pool tries to maintain.
        /// </summary>
        /// <remarks>
        /// WARNING (TASK 5): Interacts with provider's "Min Pool Size" setting, potentially keeping
        /// more connections alive than intended. Prefer provider-level configuration.
        /// </remarks>
        public int MinPoolSize { get; set; } = 0;

        /// <summary>
        /// Gets or sets how long an idle connection can stay in the pool before being disposed.
        /// </summary>
        /// <remarks>
        /// WARNING (TASK 5): Provider connection pooling has its own "Connection Lifetime" parameter.
        /// Two lifetime settings can lead to unpredictable connection reaping behavior.
        /// </remarks>
        public TimeSpan ConnectionIdleLifetime { get; set; } = TimeSpan.FromMinutes(2);
    }
}
