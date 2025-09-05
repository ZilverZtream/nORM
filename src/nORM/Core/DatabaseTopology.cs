using System;
using System.Collections.Generic;
using System.Threading;

namespace nORM.Core
{
    /// <summary>
    /// Represents the topology of database nodes used for failover and read replica support.
    /// </summary>
    public class DatabaseTopology
    {
        public class DatabaseNode
        {
            public string ConnectionString { get; set; } = string.Empty;
            public DatabaseRole Role { get; set; }
                = DatabaseRole.Primary;
            public int Priority { get; set; }
                = 0;
            private int _isHealthy = 1;
            public bool IsHealthy
            {
                get => Volatile.Read(ref _isHealthy) == 1;
                set => Volatile.Write(ref _isHealthy, value ? 1 : 0);
            }

            private long _lastHealthCheckTicks = DateTime.UtcNow.Ticks;
            public DateTime LastHealthCheck
            {
                get => new DateTime(Volatile.Read(ref _lastHealthCheckTicks), DateTimeKind.Utc);
                set => Volatile.Write(ref _lastHealthCheckTicks, value.Ticks);
            }

            private long _averageLatencyTicks;
            public TimeSpan AverageLatency
            {
                get => TimeSpan.FromTicks(Volatile.Read(ref _averageLatencyTicks));
                set => Volatile.Write(ref _averageLatencyTicks, value.Ticks);
            }
            public string Region { get; set; } = string.Empty;
        }

        public enum DatabaseRole
        {
            Primary,
            ReadReplica,
            SecondaryMaster
        }

        public List<DatabaseNode> Nodes { get; } = new();

        /// <summary>
        /// Amount of time to wait before failing over to a secondary node.
        /// </summary>
        public TimeSpan FailoverTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Whether automatic failover is enabled.
        /// </summary>
        public bool EnableAutomaticFailover { get; set; } = true;

        /// <summary>
        /// Prefer replicas that are in the same region as the current machine.
        /// </summary>
        public bool PreferLocalReplicas { get; set; } = true;
    }
}
