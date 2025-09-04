using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace nORM.Core
{
    public class NormExceptionHandler
    {
        private readonly ILogger _logger;
        private readonly string _correlationId;

        public NormExceptionHandler(ILogger logger)
        {
            _logger = logger;
            _correlationId = Guid.NewGuid().ToString("N")[..8];
        }

        public async Task<T> ExecuteWithExceptionHandling<T>(Func<Task<T>> operation, string operationName, Dictionary<string, object>? context = null)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var result = await operation();
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

    public class NormDatabaseException : NormException
    {
        public NormDatabaseException(string message, string? sql, IReadOnlyDictionary<string, object>? parameters, Exception? inner)
            : base(message, sql, parameters, inner)
        {
        }
    }

    public class NormTimeoutException : NormException
    {
        public NormTimeoutException(string message, string? sql, IReadOnlyDictionary<string, object>? parameters, Exception? inner)
            : base(message, sql, parameters, inner)
        {
        }
    }
}
