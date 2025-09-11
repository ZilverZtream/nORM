using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace nORM.Core
{
    /// <summary>
    /// Provides rich exception handling for database operations, logging contextual information
    /// and translating low-level exceptions into <see cref="NormException"/> instances.
    /// </summary>
    public class NormExceptionHandler
    {
        private readonly ILogger _logger;
        private readonly string _correlationId;

        /// <summary>
        /// Creates a new <see cref="NormExceptionHandler"/> that uses the specified logger for diagnostics.
        /// </summary>
        /// <param name="logger">Logger used to record successes and failures.</param>
        public NormExceptionHandler(ILogger logger)
        {
            _logger = logger;
            _correlationId = Guid.NewGuid().ToString("N")[..8];
        }

        /// <summary>
        /// Executes the provided operation and wraps any thrown exception into a <see cref="NormException"/> with
        /// additional context information and logging.
        /// </summary>
        /// <typeparam name="T">Type of the result returned by the operation.</typeparam>
        /// <param name="operation">The asynchronous operation to execute.</param>
        /// <param name="operationName">Human-readable name of the operation for logging.</param>
        /// <param name="context">Optional additional context values.</param>
        /// <returns>The result of the operation if successful.</returns>
        public async Task<T> ExecuteWithExceptionHandling<T>(Func<Task<T>> operation, string operationName, Dictionary<string, object>? context = null)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var result = await operation().ConfigureAwait(false);
                _logger.LogInformation(
                    "Operation {OperationName} completed successfully in {Duration}ms [CorrelationId: {CorrelationId}]",
                    operationName, stopwatch.ElapsedMilliseconds, _correlationId);
                return result;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                var enrichedContext = new Dictionary<string, object>(context ?? new Dictionary<string, object>())
                {
                    ["CorrelationId"] = _correlationId,
                    ["Duration"] = stopwatch.ElapsedMilliseconds,
                    ["Operation"] = operationName
                };

                var normException = EnrichException(ex, enrichedContext);

                _logger.LogError(normException,
                    "Operation {OperationName} failed after {Duration}ms [CorrelationId: {CorrelationId}]",
                    operationName, stopwatch.ElapsedMilliseconds, _correlationId);

                throw normException;
            }
        }

        /// <summary>
        /// Converts low-level exceptions into richer <see cref="NormException"/>
        /// instances that include contextual information such as the executed SQL,
        /// parameters, duration and a correlation identifier. Specific exception
        /// types are translated into more specialized subclasses when possible.
        /// </summary>
        /// <param name="originalException">The exception thrown by the underlying operation.</param>
        /// <param name="context">Additional context describing the failed operation.</param>
        /// <returns>A <see cref="NormException"/> enriched with contextual data.</returns>
        private NormException EnrichException(Exception originalException, Dictionary<string, object> context)
        {
            return originalException switch
            {
                SqlException sqlEx => new NormDatabaseException(
                    $"Database operation failed: {sqlEx.Message}",
                    context.TryGetValue("Sql", out var sql) ? sql?.ToString() : null,
                    context.Where(kvp => kvp.Key.StartsWith("Param")).ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                    sqlEx),

                TimeoutException timeoutEx => new NormTimeoutException(
                    $"Operation timed out after {context.GetValueOrDefault("Duration", "unknown")}ms",
                    context.TryGetValue("Sql", out var sql2) ? sql2?.ToString() : null,
                    context.Where(kvp => kvp.Key.StartsWith("Param")).ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                    timeoutEx),

                _ => new NormException(
                    $"Unexpected error in {context.GetValueOrDefault("Operation", "unknown operation")}: {originalException.Message}",
                    context.TryGetValue("Sql", out var sql3) ? sql3?.ToString() : null,
                    context.Where(kvp => kvp.Key.StartsWith("Param")).ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                    originalException)
            };
        }
    }

    /// <summary>
    /// Represents database-specific failures such as constraint violations or other provider errors.
    /// </summary>
    public class NormDatabaseException : NormException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NormDatabaseException"/>.
        /// </summary>
        /// <param name="message">Human-readable error message.</param>
        /// <param name="sql">SQL statement that caused the failure, if available.</param>
        /// <param name="parameters">Parameters supplied with the SQL statement.</param>
        /// <param name="inner">The original provider exception.</param>
        public NormDatabaseException(string message, string? sql, IReadOnlyDictionary<string, object>? parameters, Exception? inner)
            : base(message, sql, parameters, inner)
        {
        }
    }

    /// <summary>
    /// Thrown when a database operation exceeds the configured timeout.
    /// </summary>
    public class NormTimeoutException : NormException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NormTimeoutException"/>.
        /// </summary>
        /// <param name="message">Human-readable error message.</param>
        /// <param name="sql">SQL statement being executed, if known.</param>
        /// <param name="parameters">Parameters supplied to the statement.</param>
        /// <param name="inner">The original timeout exception.</param>
        public NormTimeoutException(string message, string? sql, IReadOnlyDictionary<string, object>? parameters, Exception? inner)
            : base(message, sql, parameters, inner)
        {
        }
    }
}
