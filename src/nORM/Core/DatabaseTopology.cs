using System;
using System.Collections.Generic;

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
            public bool IsHealthy { get; set; } = true;
            public DateTime LastHealthCheck { get; set; }
                = DateTime.UtcNow;
            public TimeSpan AverageLatency { get; set; }
                = TimeSpan.Zero;
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
