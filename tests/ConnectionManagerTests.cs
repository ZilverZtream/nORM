using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
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
        Assert.True(
            ex.Message.Contains("Circuit breaker is open", StringComparison.Ordinal)
            || ex.Message.Contains("No healthy primary node available", StringComparison.Ordinal),
            $"Expected circuit breaker or failover exhaustion message, got: {ex.Message}");
    }

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task HealthCheck_ChurnStress_ConcurrentAccessors_DoNotThrowUnexpectedException()
    {
        // 1 primary + 2 replicas; all pointing to valid SQLite databases so OpenAsync always succeeds
        var topology = new DatabaseTopology();
        var primary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        };
        var replica1 = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 1,
            IsHealthy = true
        };
        var replica2 = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = CreateSqliteConnectionString(),
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 2,
            IsHealthy = true
        };
        topology.Nodes.Add(primary);
        topology.Nodes.Add(replica1);
        topology.Nodes.Add(replica2);

        // Very fast health-check interval to maximize concurrent health-check + accessor races.
        using var manager = new ConnectionManager(topology, Provider, NullLogger.Instance, TimeSpan.FromMilliseconds(5));

        var unexpected = new ConcurrentBag<Exception>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        // Churn task: toggle primary and replicas between healthy/unhealthy on a rotating pattern.
        // Tick 12 makes all three unhealthy (12%4==0, 12%3==0, 12%5!=0 → actually only two),
        // but tick 60 makes all three unhealthy simultaneously — this is the hard case.
        var churnTask = Task.Run(async () =>
        {
            var tick = 0;
            while (!cts.IsCancellationRequested)
            {
                tick++;
                primary.IsHealthy  = (tick % 4) != 0;
                replica1.IsHealthy = (tick % 3) != 0;
                replica2.IsHealthy = (tick % 5) != 0;
                await Task.Delay(1, CancellationToken.None);
            }
            // Restore all nodes to healthy so post-churn assertion can use the manager.
            primary.IsHealthy = replica1.IsHealthy = replica2.IsHealthy = true;
        });

        // 16 concurrent accessor tasks: half writes, half reads.
        var accessorTasks = Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    if (i % 2 == 0)
                    {
                        await using var cn = await manager.GetWriteConnectionAsync(cts.Token);
                    }
                    else
                    {
                        await using var cn = await manager.GetReadConnectionAsync(cts.Token);
                    }
                }
                catch (NormConnectionException) { /* expected: circuit breaker open or no healthy node */ }
                catch (OperationCanceledException) { /* expected: CTS fired */ }
                catch (DbException) { /* expected: transient SQLite error */ }
                catch (ObjectDisposedException) { /* expected: manager disposed */ }
                catch (Exception ex) { unexpected.Add(ex); }
            }
        })).ToArray();

        await Task.WhenAll(accessorTasks);
        await churnTask;

        Assert.Empty(unexpected);

        // Post-churn: all nodes restored to healthy; manager must still serve connections.
        await using var writeConn = await manager.GetWriteConnectionAsync();
        Assert.NotNull(writeConn);
        await using var readConn = await manager.GetReadConnectionAsync();
        Assert.NotNull(readConn);
    }

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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

