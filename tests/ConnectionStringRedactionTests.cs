using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// T1 — Connection string credential redaction in ConnectionManager logs
//      (Gate 4.0→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that ConnectionManager never emits raw connection-string credentials
/// (Password, Token, Secret, etc.) to the logger in the read-replica failure
/// path or the failover path.
///
/// T1 root cause: GetReadConnectionAsync logged replica.ConnectionString directly
/// on failure; TriggerFailoverAsync logged newPrimary.ConnectionString on
/// promotion — both without redaction.
///
/// Fix: RedactConnectionString() replaces sensitive key values with "***" using
/// DbConnectionStringBuilder before any log call.
/// </summary>
public class ConnectionStringRedactionTests
{
    // ── Minimal capturing ILogger ─────────────────────────────────────────────

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Messages { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
            => Messages.Add(formatter(state, exception));

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }

    // ── Unit tests for RedactConnectionString ─────────────────────────────────

    [Theory]
    [InlineData("Data Source=db;Password=SuperSecret;User ID=admin",   "SuperSecret")]
    [InlineData("Server=host;Database=db;Pwd=P@ssw0rd!",              "P@ssw0rd!")]
    [InlineData("Host=db;Port=5432;Token=eyJsecrettoken",             "eyJsecrettoken")]
    [InlineData("Server=db;Access Token=tok123",                       "tok123")]
    [InlineData("Server=db;Secret=mysecret123",                        "mysecret123")]
    public void RedactConnectionString_SensitiveValue_IsReplaced(
        string raw, string sensitiveValue)
    {
        var redacted = ConnectionManager.RedactConnectionString(raw);
        Assert.DoesNotContain(sensitiveValue, redacted, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("***", redacted);
    }

    [Theory]
    [InlineData("Data Source=:memory:")]
    [InlineData("Data Source=mydb.sqlite")]
    [InlineData("Server=myhost;Database=mydb;User ID=readonly")]
    public void RedactConnectionString_NoSensitiveKeys_NoStarsInOutput(string raw)
    {
        var redacted = ConnectionManager.RedactConnectionString(raw);
        Assert.DoesNotContain("***", redacted);
    }

    [Fact]
    public void RedactConnectionString_PasswordKey_NonSensitiveKeysPreserved()
    {
        const string raw = "Data Source=mydb;User ID=alice;Password=topsecret";
        var redacted = ConnectionManager.RedactConnectionString(raw);
        Assert.DoesNotContain("topsecret", redacted, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("alice", redacted, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RedactConnectionString_MalformedString_DoesNotThrow()
    {
        var result = ConnectionManager.RedactConnectionString("not=valid;;===malformed");
        Assert.NotNull(result);
    }

    // ── Integration: replica failure path does not log raw credentials ─────────

    [Fact]
    public async Task GetReadConnectionAsync_ReplicaFailure_DoesNotLogCredentials()
    {
        const string secret = "TopSecret999";
        var logger = new CapturingLogger();

        // Replica uses a path that will fail to open (ReadOnly + non-existent file).
        var replicaConnStr =
            $"Data Source=/nonexistent/__norm_test_{Guid.NewGuid():N}.db;Password={secret};Mode=ReadOnly";
        var primaryConnStr = "Data Source=:memory:";

        var topology = new DatabaseTopology();
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
            { ConnectionString = primaryConnStr, Role = DatabaseTopology.DatabaseRole.Primary });
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
            { ConnectionString = replicaConnStr, Role = DatabaseTopology.DatabaseRole.ReadReplica });

        using var mgr = new ConnectionManager(
            topology, new SqliteProvider(), logger,
            healthCheckInterval: TimeSpan.FromHours(1));

        // Attempt a read — replica fails, falls back to primary.
        try
        {
            var cn = await mgr.GetReadConnectionAsync();
            cn.Dispose();
        }
        catch { /* primary may also fail; we only care about log content */ }

        foreach (var msg in logger.Messages)
            Assert.DoesNotContain(secret, msg, StringComparison.OrdinalIgnoreCase);
    }

    // ── Integration: failover path does not log raw credentials ───────────────

    [Fact]
    public async Task Failover_PromotionLog_DoesNotLogCredentials()
    {
        const string secret = "FailoverSecret777";
        var logger = new CapturingLogger();

        var node1 = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = $"Data Source=:memory:;Password={secret}",
            Role             = DatabaseTopology.DatabaseRole.Primary
        };
        var node2 = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = $"Data Source=:memory:;Password={secret}",
            Role             = DatabaseTopology.DatabaseRole.ReadReplica
        };
        var topology = new DatabaseTopology();
        topology.Nodes.Add(node1);
        topology.Nodes.Add(node2);
        using var mgr = new ConnectionManager(
            topology, new SqliteProvider(), logger,
            healthCheckInterval: TimeSpan.FromHours(1));

        // Force failover: mark primary unhealthy, make replica look healthy.
        node1.IsHealthy = false;
        node2.IsHealthy = true;

        try
        {
            var cn = await mgr.GetWriteConnectionAsync();
            cn.Dispose();
        }
        catch { }

        foreach (var msg in logger.Messages)
            Assert.DoesNotContain(secret, msg, StringComparison.OrdinalIgnoreCase);
    }
}
