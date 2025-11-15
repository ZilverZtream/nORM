using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;

#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Defines how transient database failures should be retried.
    /// </summary>
    public class RetryPolicy
    {
        /// <summary>
        /// Maximum number of retry attempts for a single operation.
        /// </summary>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// Base delay applied between retry attempts.
        /// </summary>
        public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Delegate that determines whether a given <see cref="Exception"/> is
        /// considered transient and should trigger a retry.
        /// Now supports DbException, TimeoutException, IOException, and SocketException.
        /// </summary>
        public Func<Exception, bool> ShouldRetry { get; set; } = ex =>
        {
            // SQL Server transient errors
            if (ex is SqlException sqlEx && sqlEx.Number is 4060 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920 or 1205 or 1222)
                return true;

            // Command timeouts
            if (ex is TimeoutException)
                return true;

            // Network-level failures
            if (ex is System.IO.IOException || ex is System.Net.Sockets.SocketException)
                return true;

            return false;
        };
    }
}