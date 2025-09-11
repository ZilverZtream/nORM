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
        /// <summary>
        /// Describes a single database node participating in the topology.
        /// </summary>
        public class DatabaseNode
        {
            /// <summary>Connection string used to reach the node.</summary>
            public string ConnectionString { get; set; } = string.Empty;

            /// <summary>The role the node plays within the topology.</summary>
            public DatabaseRole Role { get; set; }
                = DatabaseRole.Primary;

            /// <summary>Relative priority used when selecting nodes.</summary>
            public int Priority { get; set; }
                = 0;

            private int _isHealthy = 1;

            /// <summary>
            /// Indicates whether the node is considered healthy and eligible for use.
            /// </summary>
            public bool IsHealthy
            {
                get => Volatile.Read(ref _isHealthy) == 1;
                set => Volatile.Write(ref _isHealthy, value ? 1 : 0);
            }

            private long _lastHealthCheckTicks = DateTime.UtcNow.Ticks;

            /// <summary>Timestamp of the last health check performed against the node.</summary>
            public DateTime LastHealthCheck
            {
                get => new DateTime(Volatile.Read(ref _lastHealthCheckTicks), DateTimeKind.Utc);
                set => Volatile.Write(ref _lastHealthCheckTicks, value.Ticks);
            }

            private long _averageLatencyTicks;

            /// <summary>
            /// Gets or sets the moving average latency observed when communicating with the node.
            /// </summary>
            public TimeSpan AverageLatency
            {
                get => TimeSpan.FromTicks(Volatile.Read(ref _averageLatencyTicks));
                set => Volatile.Write(ref _averageLatencyTicks, value.Ticks);
            }

            /// <summary>Geographic region where the node is located.</summary>
            public string Region { get; set; } = string.Empty;
        }

        /// <summary>
        /// Enumerates the roles that a database node can fulfill.
        /// </summary>
        public enum DatabaseRole
        {
            /// <summary>The primary writable node.</summary>
            Primary,
            /// <summary>A read-only replica used for load balancing.</summary>
            ReadReplica,
            /// <summary>A secondary writable node that can assume the primary role during failover.</summary>
            SecondaryMaster
        }

        /// <summary>
        /// Collection of nodes that make up the topology.
        /// </summary>
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
