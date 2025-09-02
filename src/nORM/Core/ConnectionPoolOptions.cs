using System;

namespace nORM.Core
{
    /// <summary>
    /// Configuration settings for <see cref="ConnectionPool"/>.
    /// </summary>
    public class ConnectionPoolOptions
    {
        /// <summary>
        /// Gets or sets the maximum number of connections that can be created by the pool.
        /// </summary>
        public int MaxPoolSize { get; set; } = Environment.ProcessorCount * 2;

        /// <summary>
        /// Gets or sets the minimum number of idle connections the pool tries to maintain.
        /// </summary>
        public int MinPoolSize { get; set; } = 0;

        /// <summary>
        /// Gets or sets how long an idle connection can stay in the pool before being disposed.
        /// </summary>
        public TimeSpan ConnectionIdleLifetime { get; set; } = TimeSpan.FromMinutes(2);
    }
}
