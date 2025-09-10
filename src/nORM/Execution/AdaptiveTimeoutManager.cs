using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;

#nullable enable

namespace nORM.Execution
{
    public class AdaptiveTimeoutManager
    {
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, TimeoutStatistics> _operationStats = new();

        public class TimeoutConfiguration
        {
            public TimeSpan BaseTimeout { get; set; } = TimeSpan.FromSeconds(30);
            public TimeSpan SimpleQueryTimeout { get; set; } = TimeSpan.FromSeconds(15);
            public TimeSpan ComplexQueryTimeout { get; set; } = TimeSpan.FromSeconds(120);
            public TimeSpan BulkOperationTimeout { get; set; } = TimeSpan.FromSeconds(300);
            public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromSeconds(180);
            public double TimeoutMultiplierPerComplexity { get; set; } = 1.5;
            public bool EnableAdaptiveTimeouts { get; set; } = true;
        }

        public class TimeoutStatistics
        {
            public TimeSpan AverageExecutionTime { get; set; }
            public TimeSpan MaxExecutionTime { get; set; }
            public int ExecutionCount { get; set; }
            public int TimeoutCount { get; set; }
            public double SuccessRate => ExecutionCount == 0 ? 1.0 : 1.0 - (double)TimeoutCount / ExecutionCount;
        }

        public enum OperationType
        {
            SimpleSelect,
            ComplexSelect,
            Insert,
            Update,
            Delete,
            BulkInsert,
            BulkUpdate,
            BulkDelete,
            Transaction,
            StoredProcedure
        }

        private readonly TimeoutConfiguration _config;

        public AdaptiveTimeoutManager(TimeoutConfiguration config, ILogger logger)
        {
            _config = config;
            _logger = logger;
        }

        /// <summary>
        /// Calculates an adaptive timeout value for a given database operation based on
        /// its type, expected complexity and past execution statistics.
        /// </summary>
        /// <param name="operationType">The type of operation to be executed.</param>
        /// <param name="recordCount">Number of records involved, used to scale timeouts for bulk operations.</param>
        /// <param name="complexityScore">A relative complexity score used to adjust the timeout.</param>
        /// <param name="operationKey">Optional key for tracking historical execution statistics.</param>
        /// <returns>The timeout <see cref="TimeSpan"/> to apply for the operation.</returns>
        public TimeSpan GetTimeoutForOperation(
            OperationType operationType,
            int recordCount = 1,
            int complexityScore = 1,
            string? operationKey = null)
        {
            var baseTimeout = GetBaseTimeoutForOperation(operationType);

            if (!_config.EnableAdaptiveTimeouts)
                return baseTimeout;

            var recordMultiplier = operationType switch
            {
                OperationType.BulkInsert or OperationType.BulkUpdate or OperationType.BulkDelete
                    => Math.Max(1.0, Math.Log10(Math.Max(1, recordCount / 1000.0))),
                _ => 1.0
            };

            var complexityMultiplier = Math.Pow(_config.TimeoutMultiplierPerComplexity, complexityScore / 1000.0);

            var historicalMultiplier = 1.0;
            if (operationKey != null && _operationStats.TryGetValue(operationKey, out var stats))
            {
                if (stats.SuccessRate < 0.95)
                {
                    historicalMultiplier = 1.5;
                }
                else if (stats.AverageExecutionTime > TimeSpan.Zero)
                {
                    var suggestedTimeout = TimeSpan.FromMilliseconds(stats.AverageExecutionTime.TotalMilliseconds * 3);
                    if (suggestedTimeout > baseTimeout)
                    {
                        historicalMultiplier = suggestedTimeout.TotalMilliseconds / baseTimeout.TotalMilliseconds;
                    }
                }
            }

            var finalTimeout = TimeSpan.FromMilliseconds(
                baseTimeout.TotalMilliseconds * recordMultiplier * complexityMultiplier * historicalMultiplier);

            var maxTimeout = TimeSpan.FromMinutes(30);
            return finalTimeout > maxTimeout ? maxTimeout : finalTimeout;
        }

        /// <summary>
        /// Determines the baseline timeout for a given operation type before any
        /// adaptive adjustments are applied. The value is derived from the configured
        /// <see cref="TimeoutConfiguration"/>.
        /// </summary>
        /// <param name="operationType">The type of operation being executed.</param>
        /// <returns>The base timeout to use for the operation.</returns>
        private TimeSpan GetBaseTimeoutForOperation(OperationType operationType)
        {
            return operationType switch
            {
                OperationType.SimpleSelect => _config.SimpleQueryTimeout,
                OperationType.ComplexSelect => _config.ComplexQueryTimeout,
                OperationType.Insert or OperationType.Update or OperationType.Delete => _config.BaseTimeout,
                OperationType.BulkInsert or OperationType.BulkUpdate or OperationType.BulkDelete => _config.BulkOperationTimeout,
                OperationType.Transaction => _config.TransactionTimeout,
                OperationType.StoredProcedure => _config.ComplexQueryTimeout,
                _ => _config.BaseTimeout
            };
        }

        public async Task<T> ExecuteWithAdaptiveTimeout<T>(
            Func<CancellationToken, Task<T>> operation,
            OperationType operationType,
            string operationKey,
            int recordCount = 1,
            int complexityScore = 1)
        {
            var timeout = GetTimeoutForOperation(operationType, recordCount, complexityScore, operationKey);

            using var cts = new CancellationTokenSource(timeout);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var result = await operation(cts.Token).ConfigureAwait(false);
                stopwatch.Stop();

                UpdateOperationStatistics(operationKey, stopwatch.Elapsed, success: true);

                var threshold = TimeSpan.FromMilliseconds(timeout.TotalMilliseconds * 0.8);
                if (stopwatch.Elapsed > threshold)
                {
                    _logger.LogWarning("Operation {OperationKey} completed but used {Percentage:P} of available timeout",
                        operationKey, stopwatch.Elapsed.TotalMilliseconds / timeout.TotalMilliseconds);
                }

                return result;
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                stopwatch.Stop();
                UpdateOperationStatistics(operationKey, stopwatch.Elapsed, success: false);

                _logger.LogError("Operation {OperationKey} timed out after {Timeout}",
                    operationKey, timeout);

                throw new NormTimeoutException(
                    $"Operation '{operationKey}' timed out after {timeout}. Consider optimizing the query or increasing timeout.",
                    null, null, null);
            }
            catch (Exception)
            {
                stopwatch.Stop();
                UpdateOperationStatistics(operationKey, stopwatch.Elapsed, success: false);
                throw;
            }
        }

        /// <summary>
        /// Updates the rolling execution statistics for the specified operation. These
        /// metrics are used to adapt future timeout calculations based on past behavior.
        /// </summary>
        /// <param name="operationKey">Unique identifier for the operation.</param>
        /// <param name="executionTime">Actual execution duration.</param>
        /// <param name="success">Indicates whether the operation completed successfully.</param>
        private void UpdateOperationStatistics(string operationKey, TimeSpan executionTime, bool success)
        {
            _operationStats.AddOrUpdate(operationKey,
                new TimeoutStatistics
                {
                    AverageExecutionTime = executionTime,
                    MaxExecutionTime = executionTime,
                    ExecutionCount = 1,
                    TimeoutCount = success ? 0 : 1
                },
                (_, existing) =>
                {
                    var newCount = existing.ExecutionCount + 1;
                    var newAverage = TimeSpan.FromMilliseconds(
                        (existing.AverageExecutionTime.TotalMilliseconds * existing.ExecutionCount + executionTime.TotalMilliseconds) / newCount);

                    return new TimeoutStatistics
                    {
                        AverageExecutionTime = newAverage,
                        MaxExecutionTime = executionTime > existing.MaxExecutionTime ? executionTime : existing.MaxExecutionTime,
                        ExecutionCount = newCount,
                        TimeoutCount = existing.TimeoutCount + (success ? 0 : 1)
                    };
                });
        }

        /// <summary>
        /// Retrieves a snapshot of the recorded timeout statistics for all operations.
        /// </summary>
        /// <returns>A dictionary keyed by operation identifier containing statistical data.</returns>
        public Dictionary<string, TimeoutStatistics> GetOperationStatistics()
        {
            return new Dictionary<string, TimeoutStatistics>(_operationStats);
        }
    }
}

