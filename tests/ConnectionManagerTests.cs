using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class ConnectionManagerTests
{
    private static SqliteProvider Provider => new();

    private static string CreateSqliteConnectionString()
    {
        var directory = Path.Combine(Path.GetTempPath(), "norm-cm-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        return $"Data Source={Path.Combine(directory, "db.sqlite")}";
    }

    [Fact]
    public async Task Failover_to_secondary_when_primary_unhealthy()
    {
        var topology = new DatabaseTopology();
        var primary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        };
        var secondary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
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

    [Fact]
    public async Task Concurrent_read_requests_and_dispose_do_not_throw_unexpected_exceptions()
    {
        var topology = new DatabaseTopology();
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        });
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 1,
            IsHealthy = true
        });
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 2,
            IsHealthy = true
        });

        using var manager = new ConnectionManager(topology, Provider, NullLogger.Instance, TimeSpan.FromMilliseconds(10));
        var tasks = Enumerable.Range(0, 64).Select(async _ =>
        {
            try
            {
                await using var cn = await manager.GetReadConnectionAsync();
                Assert.NotNull(cn);
            }
            catch (ObjectDisposedException)
            {
                // Dispose may win the race; that is the documented boundary.
            }
        }).ToArray();

        manager.Dispose();
        await Task.WhenAll(tasks);
    }
}

