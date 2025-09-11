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
    /// <summary>
    /// Provides heuristics for determining and tracking database command timeouts based on
    /// operation type, complexity and historical execution statistics.
    /// </summary>
    public class AdaptiveTimeoutManager
    {
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, TimeoutStatistics> _operationStats = new();

        /// <summary>
        /// Configuration options that influence how adaptive timeouts are calculated.
        /// </summary>
        public class TimeoutConfiguration
        {
            /// <summary>Baseline timeout for standard operations.</summary>
            public TimeSpan BaseTimeout { get; set; } = TimeSpan.FromSeconds(30);
            /// <summary>Timeout applied to simple read-only queries.</summary>
            public TimeSpan SimpleQueryTimeout { get; set; } = TimeSpan.FromSeconds(15);
            /// <summary>Timeout applied to complex read-only queries.</summary>
            public TimeSpan ComplexQueryTimeout { get; set; } = TimeSpan.FromSeconds(120);
            /// <summary>Timeout applied to bulk operations such as inserts or updates.</summary>
            public TimeSpan BulkOperationTimeout { get; set; } = TimeSpan.FromSeconds(300);
            /// <summary>Timeout applied to explicit database transactions.</summary>
            public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromSeconds(180);
            /// <summary>Multiplier used to scale timeouts based on operation complexity.</summary>
            public double TimeoutMultiplierPerComplexity { get; set; } = 1.5;
            /// <summary>Indicates whether adaptive timeout calculation is enabled.</summary>
            public bool EnableAdaptiveTimeouts { get; set; } = true;
        }

        /// <summary>
        /// Captures statistics about past executions to guide future timeout adjustments.
        /// </summary>
        public class TimeoutStatistics
        {
            /// <summary>The average execution duration of the operation.</summary>
            public TimeSpan AverageExecutionTime { get; set; }
            /// <summary>The maximum observed execution time.</summary>
            public TimeSpan MaxExecutionTime { get; set; }
            /// <summary>Total number of executions recorded.</summary>
            public int ExecutionCount { get; set; }
            /// <summary>Number of executions that resulted in a timeout.</summary>
            public int TimeoutCount { get; set; }
            /// <summary>The ratio of successful executions to total executions.</summary>
            public double SuccessRate => ExecutionCount == 0 ? 1.0 : 1.0 - (double)TimeoutCount / ExecutionCount;
        }

        /// <summary>
        /// Enumerates the types of database operations for which timeouts can be calculated.
        /// </summary>
        public enum OperationType
        {
            /// <summary>A simple read-only query.</summary>
            SimpleSelect,
            /// <summary>A complex read-only query.</summary>
            ComplexSelect,
            /// <summary>An insert operation.</summary>
            Insert,
            /// <summary>An update operation.</summary>
            Update,
            /// <summary>A delete operation.</summary>
            Delete,
            /// <summary>A bulk insert operation.</summary>
            BulkInsert,
            /// <summary>A bulk update operation.</summary>
            BulkUpdate,
            /// <summary>A bulk delete operation.</summary>
            BulkDelete,
            /// <summary>An explicit transaction.</summary>
            Transaction,
            /// <summary>A stored procedure invocation.</summary>
            StoredProcedure
        }

        private readonly TimeoutConfiguration _config;

        /// <summary>
        /// Initializes a new instance of the <see cref="AdaptiveTimeoutManager"/> class.
        /// </summary>
        /// <param name="config">Configuration options governing timeout behavior.</param>
        /// <param name="logger">Logger used to record diagnostic information.</param>
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

        /// <summary>
        /// Executes the provided asynchronous operation enforcing an adaptive timeout based on the
        /// configured heuristics and records the outcome for future adjustments.
        /// </summary>
        /// <typeparam name="T">Type of the result produced by the operation.</typeparam>
        /// <param name="operation">Delegate representing the work to execute.</param>
        /// <param name="operationType">Classification of the operation being executed.</param>
        /// <param name="operationKey">Identifier used for tracking execution statistics.</param>
        /// <param name="recordCount">Number of records involved, used for scaling bulk operation timeouts.</param>
        /// <param name="complexityScore">Relative complexity score of the operation.</param>
        /// <returns>The result produced by the <paramref name="operation"/>.</returns>
        /// <exception cref="NormTimeoutException">Thrown when the operation exceeds the calculated timeout.</exception>
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

