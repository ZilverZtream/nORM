using System;
using System.Collections.Generic;
using nORM.Core;

namespace Microsoft.Extensions.Logging
{
    /// <summary>
    /// Extension methods for logging nORM specific database operations.
    /// </summary>
    public static class DbContextLoggingExtensions
    {
        /// <summary>
        /// Logs a database query with parameter and timing information.
        /// </summary>
        public static void LogQuery(this ILogger logger, string sql, IReadOnlyDictionary<string, object> parameters, TimeSpan duration, int recordCount)
        {
            if (logger == null) return;
            logger.LogInformation("Executed SQL query {Sql} with parameters {@Parameters} in {Duration}. Records: {RecordCount}", sql, parameters, duration, recordCount);
        }

        /// <summary>
        /// Logs a bulk database operation.
        /// </summary>
        public static void LogBulkOperation(this ILogger logger, string operation, string tableName, int recordCount, TimeSpan duration)
        {
            if (logger == null) return;
            logger.LogInformation("Bulk operation {Operation} on {TableName} affected {RecordCount} rows in {Duration}", operation, tableName, recordCount, duration);
        }

        /// <summary>
        /// Logs a nORM specific error with retry attempt information.
        /// </summary>
        public static void LogError(this ILogger logger, NormException exception, int retryAttempt)
        {
            if (logger == null) return;
            logger.LogError(exception, "Operation failed on retry {RetryAttempt}", retryAttempt);
        }
    }
}
