using System;
using System.Collections.Generic;
using nORM.Core;
using nORM.Internal;

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
            logger.LogInformation(
                "Executed SQL query {Sql} with parameters {@Parameters} in {Duration}. Records: {RecordCount}",
                SqlRedaction.RedactForLogging(sql),
                RedactParameters(parameters),
                duration,
                recordCount);
        }

        private static IReadOnlyDictionary<string, object> RedactParameters(IReadOnlyDictionary<string, object> parameters)
        {
            if (parameters.Count == 0)
                return parameters;

            var redacted = new Dictionary<string, object>(parameters.Count, StringComparer.Ordinal);
            foreach (var parameter in parameters)
                redacted[parameter.Key] = parameter.Value is null or DBNull ? DBNull.Value : "[redacted]";
            return redacted;
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
