using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using nORM.Mapping;

namespace nORM.Providers
{
    /// <summary>
    /// Provides heuristics for determining efficient batch sizes for bulk
    /// operations. The sizing takes into account historical performance,
    /// estimated memory usage and operation characteristics.
    /// </summary>
    public class DynamicBatchSizer
    {
        private const int MaxMemoryPerBatch = 16 * 1024 * 1024; // 16 MB — conservative ceiling for in-memory batch assembly
        private const int MinBatchSize = 10;               // Floor to avoid excessive round-trip overhead
        private const int MaxBatchSize = 10000;            // Ceiling to bound memory and parameter counts
        private const int BaseObjectOverhead = 100;         // Estimated fixed per-record overhead in bytes (CLR object header + refs)
        private const int DefaultFallbackSize = 100;        // Fallback byte estimate for complex/unserializable types
        private const int AssumedBandwidthBytesPerSec = 12_500_000; // 100 Mbps network — conservative for cloud/local scenarios
        private const double TargetTransferTimeSec = 1.5;   // Target network transfer window per batch
        private const int HistoricalBaseBatchSize = 2000;   // Starting point before column/index adjustments
        private const int MaxHistoryEntries = 20;           // Sliding window for performance history

        /// <summary>
        /// Represents the outcome of a batch size calculation including various
        /// estimates that influenced the decision.
        /// </summary>
        public class BatchSizingResult
        {
            /// <summary>Chosen number of records to include in each batch.</summary>
            public int OptimalBatchSize { get; set; }

            /// <summary>Estimated memory consumption for a batch of the chosen size.</summary>
            public long EstimatedMemoryUsage { get; set; }

            /// <summary>Estimated time required to process a batch of the chosen size.</summary>
            public TimeSpan EstimatedBatchTime { get; set; }

            /// <summary>Description of the strategy and factors used to derive the batch size.</summary>
            public string Strategy { get; set; } = "";
        }

        private readonly ConcurrentDictionary<string, BatchPerformanceHistory> _performanceHistory = new();
        private readonly ConcurrentDictionary<Type, (int RecordSize, int MemoryBasedBatchSize)> _entityBatchSizeCache = new();

        private class BatchPerformanceHistory
        {
            public List<(int BatchSize, TimeSpan Duration, int RecordCount)> History { get; } = new();
            public DateTime LastUpdate { get; set; }
            public int OptimalBatchSize { get; set; } = 1000;
        }

        /// <summary>
        /// Determines an efficient batch size for bulk operations by analyzing a sample of
        /// records and considering historical performance, memory usage and provider limits.
        /// </summary>
        /// <typeparam name="T">Type of the entity being processed.</typeparam>
        /// <param name="sample">Sample records used to estimate sizes and costs.</param>
        /// <param name="mapping">Mapping information for the entity.</param>
        /// <param name="operationKey">Key identifying the operation for caching performance history.</param>
        /// <param name="totalRecords">Optional total record count to further constrain the batch size.</param>
        /// <returns>Calculated sizing information including optimal batch size and estimates.</returns>
        public BatchSizingResult CalculateOptimalBatchSize<T>(
            IEnumerable<T> sample,
            TableMapping mapping,
            string operationKey,
            int totalRecords = -1) where T : class
        {
            var sampleList = sample.Take(100).ToList();
            if (sampleList.Count == 0)
                return new BatchSizingResult { OptimalBatchSize = MinBatchSize };

            var cacheEntry = _entityBatchSizeCache.GetOrAdd(typeof(T), _ =>
            {
                var size = EstimateRecordSize(sampleList[0], mapping);
                // Guard against zero/negative size: ensure at least BaseObjectOverhead
                size = Math.Max(size, BaseObjectOverhead);
                var memorySize = Math.Max(MinBatchSize, MaxMemoryPerBatch / size);
                return (size, memorySize);
            });

            var recordSize = cacheEntry.RecordSize;
            var memoryBasedBatchSize = cacheEntry.MemoryBasedBatchSize;

            var historicalOptimal = GetHistoricalOptimalBatchSize(operationKey);

            var networkOptimal = EstimateNetworkOptimalBatchSize(recordSize);
            var databaseOptimal = EstimateDatabaseOptimalBatchSize(mapping);

            var candidates = new[] { memoryBasedBatchSize, historicalOptimal, networkOptimal, databaseOptimal };
            var optimalSize = Math.Max(MinBatchSize, Math.Min(MaxBatchSize, candidates.Min()));

            optimalSize = AdjustForDataCharacteristics(optimalSize, sampleList, mapping, totalRecords);

            return new BatchSizingResult
            {
                OptimalBatchSize = optimalSize,
                // Use long multiplication to avoid int overflow for large batch sizes
                EstimatedMemoryUsage = (long)optimalSize * recordSize,
                EstimatedBatchTime = EstimateBatchTime(optimalSize, recordSize, operationKey),
                Strategy = $"Memory:{memoryBasedBatchSize}, Historical:{historicalOptimal}, Network:{networkOptimal}, DB:{databaseOptimal}"
            };
        }

        private int EstimateRecordSize<T>(T sampleRecord, TableMapping mapping) where T : class
        {
            var baseSize = BaseObjectOverhead;
            var columnSizes = 0;

            foreach (var column in mapping.Columns)
            {
                var value = column.Getter(sampleRecord);
                columnSizes += EstimateValueSize(value, column.Prop.PropertyType);
            }

            return baseSize + columnSizes;
        }

        private int EstimateValueSize(object? value, Type type)
        {
            if (value == null) return 4;

            return value switch
            {
                string str => str.Length * 2 + 8,
                byte[] bytes => bytes.Length + 8,
                int => 4,
                long => 8,
                decimal => 16,
                DateTime => 8,
                DateTimeOffset => 12,
                Guid => 16,
                bool => 1,
                float => 4,
                double => 8,
                _ => EstimateComplexObjectSize(value, type)
            };
        }

        private int EstimateComplexObjectSize(object value, Type type)
        {
            if (type.IsEnum) return 4;

            try
            {
                var json = System.Text.Json.JsonSerializer.Serialize(value);
                return json.Length * 2;
            }
            catch (System.Text.Json.JsonException)
            {
                // Unserializable type (circular refs, unsupported converters) -- use fallback
                return DefaultFallbackSize;
            }
            catch (NotSupportedException)
            {
                return DefaultFallbackSize;
            }
            catch (InvalidOperationException)
            {
                // JsonSerializer can throw InvalidOperationException for certain type configurations
                return DefaultFallbackSize;
            }
        }

        private int GetHistoricalOptimalBatchSize(string operationKey)
        {
            if (!_performanceHistory.TryGetValue(operationKey, out var history))
                return 1000;

            // Lock to read History safely — it is mutated under lock in RecordBatchPerformance
            lock (history.History)
            {
                if (history.History.Count < 3)
                    return history.OptimalBatchSize;

                var bestThroughput = 0.0;
                var bestBatchSize = 1000;

                foreach (var entry in history.History.TakeLast(10))
                {
                    // Guard against zero-duration entries that would produce Infinity throughput
                    if (entry.Duration.TotalSeconds <= 0 || entry.RecordCount <= 0)
                        continue;
                    var throughput = entry.RecordCount / entry.Duration.TotalSeconds;
                    if (throughput > bestThroughput)
                    {
                        bestThroughput = throughput;
                        bestBatchSize = entry.BatchSize;
                    }
                }

                return bestBatchSize;
            }
        }

        private int EstimateNetworkOptimalBatchSize(int recordSize)
        {
            // Guard against zero recordSize to avoid division by zero
            if (recordSize <= 0) return MaxBatchSize;
            var maxBytesPerBatch = (int)(AssumedBandwidthBytesPerSec * TargetTransferTimeSec);
            return Math.Max(MinBatchSize, maxBytesPerBatch / recordSize);
        }

        private int EstimateDatabaseOptimalBatchSize(TableMapping mapping)
        {
            var columnCount = mapping.Columns.Length;
            var indexCount = mapping.KeyColumns.Length;

            // Scale down from HistoricalBaseBatchSize as column/index counts grow.
            // Columns <= 5 and indexes <= 1 keep a factor of 1.0 (no inflation above base).
            // Each column beyond 5 reduces by 10%; each index beyond 1 reduces by 20%.
            var columnFactor = Math.Max(0.1, Math.Min(1.0, 1.0 - (columnCount - 5) * 0.1));
            var indexFactor = Math.Max(0.1, Math.Min(1.0, 1.0 - (indexCount - 1) * 0.2));

            return (int)(HistoricalBaseBatchSize * columnFactor * indexFactor);
        }

        private int AdjustForDataCharacteristics<T>(int baseBatchSize, List<T> sample, TableMapping mapping, int totalRecords) where T : class
        {
            var adjustedSize = baseBatchSize;

            if (sample.Count > 50)
            {
                var avgStringLength = CalculateAverageStringLength(sample, mapping);
                if (avgStringLength < 50)
                {
                    adjustedSize = (int)(adjustedSize * 1.5);
                }
                else if (avgStringLength > 1000)
                {
                    adjustedSize = (int)(adjustedSize * 0.7);
                }
            }

            if (totalRecords > 0 && totalRecords < 1000)
            {
                // Ensure totalRecords / 4 is at least MinBatchSize to avoid clamping to zero
                adjustedSize = Math.Min(adjustedSize, Math.Max(MinBatchSize, totalRecords / 4));
            }

            return Math.Max(MinBatchSize, Math.Min(MaxBatchSize, adjustedSize));
        }

        private double CalculateAverageStringLength<T>(List<T> sample, TableMapping mapping) where T : class
        {
            var stringColumns = mapping.Columns.Where(c => c.Prop.PropertyType == typeof(string)).ToArray();
            if (stringColumns.Length == 0) return 0;

            var totalLength = 0;
            var totalCount = 0;

            foreach (var record in sample.Take(20))
            {
                foreach (var column in stringColumns)
                {
                    if (column.Getter(record) is string str)
                    {
                        totalLength += str.Length;
                        totalCount++;
                    }
                }
            }

            return totalCount > 0 ? (double)totalLength / totalCount : 0;
        }

        private TimeSpan EstimateBatchTime(int batchSize, int recordSize, string operationKey)
        {
            if (_performanceHistory.TryGetValue(operationKey, out var history))
            {
                // Lock to read History safely — it is mutated under lock in RecordBatchPerformance
                lock (history.History)
                {
                    if (history.History.Count > 0)
                    {
                        var recentEntry = history.History[history.History.Count - 1];
                        if (recentEntry.RecordCount > 0)
                        {
                            var timePerRecord = recentEntry.Duration.TotalMilliseconds / recentEntry.RecordCount;
                            return TimeSpan.FromMilliseconds(timePerRecord * batchSize);
                        }
                    }
                }
            }

            var estimatedMs = (batchSize * recordSize) / 1024.0;
            return TimeSpan.FromMilliseconds(Math.Max(100, estimatedMs));
        }

        /// <summary>
        /// Records the outcome of a batch execution so that future sizing decisions
        /// can incorporate observed performance characteristics.
        /// </summary>
        /// <param name="operationKey">Identifier of the operation being tracked.</param>
        /// <param name="batchSize">Number of records processed.</param>
        /// <param name="duration">Time taken to process the batch.</param>
        /// <param name="recordCount">Actual record count processed.</param>
        public void RecordBatchPerformance(string operationKey, int batchSize, TimeSpan duration, int recordCount)
        {
            var history = _performanceHistory.GetOrAdd(operationKey, _ => new BatchPerformanceHistory());

            lock (history.History)
            {
                history.History.Add((batchSize, duration, recordCount));
                history.LastUpdate = DateTime.UtcNow;

                if (history.History.Count > MaxHistoryEntries)
                {
                    history.History.RemoveRange(0, history.History.Count - MaxHistoryEntries);
                }

                if (history.History.Count >= 3)
                {
                    var bestEntry = history.History
                        .Where(e => e.Duration.TotalSeconds > 0 && e.RecordCount > 0)
                        .OrderByDescending(e => (double)e.RecordCount / e.Duration.TotalSeconds)
                        .FirstOrDefault();
                    if (bestEntry.BatchSize > 0)
                        history.OptimalBatchSize = bestEntry.BatchSize;
                }
            }
        }
    }
}
