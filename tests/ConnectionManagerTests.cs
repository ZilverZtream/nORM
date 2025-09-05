using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class ConnectionManagerTests
{
    private static SqliteProvider Provider => new();

    [Fact]
    public async Task Failover_to_secondary_when_primary_unhealthy()
    {
        var topology = new DatabaseTopology();
        var primary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        };
        var secondary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.SecondaryMaster,
            Priority = 2,
            IsHealthy = true
        };
        topology.Nodes.Add(primary);
        topology.Nodes.Add(secondary);

        using var manager = new ConnectionManager(topology, Provider, NullLogger.Instance);

        await using var conn1 = await manager.GetWriteConnectionAsync();
        Assert.Equal(primary.ConnectionString, conn1.ConnectionString);

        primary.IsHealthy = false;

        await using var conn2 = await manager.GetWriteConnectionAsync();
        Assert.Equal(secondary.ConnectionString, conn2.ConnectionString);
    }

    [Fact]
    public async Task Read_replica_failure_falls_back_to_primary()
    {
        var topology = new DatabaseTopology();
        var primary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        };
        var failingPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString(), "db.db");
        var failingReplica = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = $"Data Source={failingPath}",
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 1,
            IsHealthy = true
        };
        topology.Nodes.Add(primary);
        topology.Nodes.Add(failingReplica);

        using var manager = new ConnectionManager(topology, Provider, NullLogger.Instance);

        await using var cn = await manager.GetReadConnectionAsync();
        Assert.Equal(primary.ConnectionString, cn.ConnectionString);
        Assert.False(failingReplica.IsHealthy);
    }

    [Fact]
    public async Task Circuit_breaker_opens_after_failure()
    {
        var topology = new DatabaseTopology();
        var badPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString(), "db.db");
        var primary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = $"Data Source={badPath}",
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        };
        topology.Nodes.Add(primary);

        using var manager = new ConnectionManager(topology, Provider, NullLogger.Instance);

        await Assert.ThrowsAnyAsync<Exception>(() => manager.GetWriteConnectionAsync());
        var ex = await Assert.ThrowsAsync<NormConnectionException>(() => manager.GetWriteConnectionAsync());
        Assert.Contains("Circuit breaker is open", ex.Message);
    }
}

