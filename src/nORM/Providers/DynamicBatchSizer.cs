using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using nORM.Mapping;

namespace nORM.Providers
{
    public class DynamicBatchSizer
    {
        private const int MaxMemoryPerBatch = 16 * 1024 * 1024; // 16MB
        private const int MinBatchSize = 10;
        private const int MaxBatchSize = 10000;
        private const int DefaultTargetBatchTime = 2000; // 2 seconds

        public class BatchSizingResult
        {
            public int OptimalBatchSize { get; set; }
            public int EstimatedMemoryUsage { get; set; }
            public TimeSpan EstimatedBatchTime { get; set; }
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

        public BatchSizingResult CalculateOptimalBatchSize<T>(
            IEnumerable<T> sample,
            TableMapping mapping,
            string operationKey,
            int totalRecords = -1) where T : class
        {
            var sampleList = sample.Take(100).ToList();
            if (!sampleList.Any())
                return new BatchSizingResult { OptimalBatchSize = MinBatchSize };

            var cacheEntry = _entityBatchSizeCache.GetOrAdd(typeof(T), _ =>
            {
                var size = EstimateRecordSize(sampleList.First(), mapping);
                var memorySize = Math.Max(MinBatchSize, MaxMemoryPerBatch / size);
                return (size, memorySize);
            });

            var recordSize = cacheEntry.RecordSize;
            var memoryBasedBatchSize = cacheEntry.MemoryBasedBatchSize;

            var historicalOptimal = GetHistoricalOptimalBatchSize(operationKey, recordSize);

            var networkOptimal = EstimateNetworkOptimalBatchSize(recordSize);
            var databaseOptimal = EstimateDatabaseOptimalBatchSize(mapping);

            var candidates = new[] { memoryBasedBatchSize, historicalOptimal, networkOptimal, databaseOptimal };
            var optimalSize = Math.Max(MinBatchSize, Math.Min(MaxBatchSize, candidates.Min()));

            optimalSize = AdjustForDataCharacteristics(optimalSize, sampleList, mapping, totalRecords);

            return new BatchSizingResult
            {
                OptimalBatchSize = optimalSize,
                EstimatedMemoryUsage = optimalSize * recordSize,
                EstimatedBatchTime = EstimateBatchTime(optimalSize, recordSize, operationKey),
                Strategy = $"Memory:{memoryBasedBatchSize}, Historical:{historicalOptimal}, Network:{networkOptimal}, DB:{databaseOptimal}"
            };
        }

        private int EstimateRecordSize<T>(T sampleRecord, TableMapping mapping) where T : class
        {
            var baseSize = 100;
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
            catch
            {
                return 100;
            }
        }

        private int GetHistoricalOptimalBatchSize(string operationKey, int recordSize)
        {
            if (!_performanceHistory.TryGetValue(operationKey, out var history))
                return 1000;

            if (history.History.Count < 3)
                return history.OptimalBatchSize;

            var bestThroughput = 0.0;
            var bestBatchSize = 1000;

            foreach (var entry in history.History.TakeLast(10))
            {
                var throughput = entry.RecordCount / entry.Duration.TotalSeconds;
                if (throughput > bestThroughput)
                {
                    bestThroughput = throughput;
                    bestBatchSize = entry.BatchSize;
                }
            }

            return bestBatchSize;
        }

        private int EstimateNetworkOptimalBatchSize(int recordSize)
        {
            var assumedBandwidthBytesPerSecond = 12_500_000;
            var targetTransferTime = 1.5;

            var maxBytesPerBatch = (int)(assumedBandwidthBytesPerSecond * targetTransferTime);
            return Math.Max(MinBatchSize, maxBytesPerBatch / recordSize);
        }

        private int EstimateDatabaseOptimalBatchSize(TableMapping mapping)
        {
            var columnCount = mapping.Columns.Length;
            var indexCount = mapping.KeyColumns.Length;

            var columnFactor = Math.Max(0.1, 1.0 - (columnCount - 5) * 0.1);
            var indexFactor = Math.Max(0.1, 1.0 - (indexCount - 1) * 0.2);

            var baseBatchSize = 2000;
            return (int)(baseBatchSize * columnFactor * indexFactor);
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
                adjustedSize = Math.Min(adjustedSize, totalRecords / 4);
            }

            return Math.Max(MinBatchSize, Math.Min(MaxBatchSize, adjustedSize));
        }

        private double CalculateAverageStringLength<T>(List<T> sample, TableMapping mapping) where T : class
        {
            var stringColumns = mapping.Columns.Where(c => c.Prop.PropertyType == typeof(string)).ToArray();
            if (!stringColumns.Any()) return 0;

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
            if (_performanceHistory.TryGetValue(operationKey, out var history) && history.History.Any())
            {
                var recentEntry = history.History.Last();
                var timePerRecord = recentEntry.Duration.TotalMilliseconds / recentEntry.RecordCount;
                return TimeSpan.FromMilliseconds(timePerRecord * batchSize);
            }

            var estimatedMs = (batchSize * recordSize) / 1024.0;
            return TimeSpan.FromMilliseconds(Math.Max(100, estimatedMs));
        }

        public void RecordBatchPerformance(string operationKey, int batchSize, TimeSpan duration, int recordCount)
        {
            var history = _performanceHistory.GetOrAdd(operationKey, _ => new BatchPerformanceHistory());

            lock (history.History)
            {
                history.History.Add((batchSize, duration, recordCount));
                history.LastUpdate = DateTime.UtcNow;

                if (history.History.Count > 20)
                {
                    history.History.RemoveRange(0, history.History.Count - 20);
                }

                if (history.History.Count >= 3)
                {
                    var bestEntry = history.History.OrderByDescending(e => (double)e.RecordCount / e.Duration.TotalSeconds).First();
                    history.OptimalBatchSize = bestEntry.BatchSize;
                }
            }
        }
    }
}
