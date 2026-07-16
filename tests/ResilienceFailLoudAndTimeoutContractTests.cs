using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contracts for fail-loud failure surfacing and timeout wiring (resilience matrix cells). A
/// broken execution without a retry policy - or with one whose predicate rejects the error -
/// surfaces a loud exception and never a silent partial list. And the configured base timeout
/// actually reaches the commands the pipeline executes, observed through the interceptor on
/// read and set-based-write paths.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ResilienceFailLoudAndTimeoutContractTests
{
    [Table("FailLoud_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private sealed class InjectedTransientException : DbException
    {
        public InjectedTransientException() : base("injected transient failure") { }
    }

    private sealed class ProbeInterceptor : BaseDbCommandInterceptor
    {
        public bool ThrowNonDb;
        public bool ThrowTransient;
        public readonly List<(string Sql, int Timeout)> Seen = new();
        public ProbeInterceptor() : base(NullLogger.Instance) { }

        private void Observe(DbCommand command)
        {
            Seen.Add((command.CommandText.Split('\n')[0], command.CommandTimeout));
            if (ThrowNonDb) throw new InvalidOperationException("connection dropped");
            if (ThrowTransient) throw new InjectedTransientException();
        }

        // SQLite prefers sync execution: observe on BOTH hook variants.
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        { Observe(command); return base.ReaderExecuting(command, context); }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { Observe(command); return base.ReaderExecutingAsync(command, context, ct); }
        public override InterceptionResult<int> NonQueryExecuting(DbCommand command, DbContext context)
        { Observe(command); return base.NonQueryExecuting(command, context); }
        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { Observe(command); return base.NonQueryExecutingAsync(command, context, ct); }
    }

    private static (SqliteConnection Cn, DbContext Ctx, ProbeInterceptor Probe) Create(RetryPolicy? retry = null, TimeSpan? baseTimeout = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE FailLoud_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);" +
                "INSERT INTO FailLoud_Row VALUES (1, 10), (2, 20);";
            cmd.ExecuteNonQuery();
        }
        var probe = new ProbeInterceptor();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            RetryPolicy = retry
        };
        if (baseTimeout is { } bt)
            opts.TimeoutConfiguration.BaseTimeout = bt;
        opts.CommandInterceptors.Add(probe);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx, probe);
    }

    [Fact]
    public async Task Broken_execution_without_retry_policy_is_loud_never_partial()
    {
        var (cn, ctx, probe) = Create();
        using var _cn = cn; await using var _ = ctx;

        probe.ThrowNonDb = true;
        await Assert.ThrowsAnyAsync<Exception>(() => ctx.Query<Row>().Where(r => r.V >= 10).ToListAsync());

        probe.ThrowNonDb = false;
        var rows = await ctx.Query<Row>().Where(r => r.V >= 10).ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task Non_transient_errors_stay_loud_under_a_retry_policy()
    {
        var (cn, ctx, probe) = Create(new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = static _ => false   // the policy REJECTS this error class
        });
        using var _cn = cn; await using var _ = ctx;

        probe.ThrowTransient = true;
        var before = probe.Seen.Count;
        await Assert.ThrowsAnyAsync<NormException>(() => ctx.Query<Row>().Where(r => r.V >= 10).ToListAsync());
        // Rejected errors are not retried: exactly one execution attempt.
        Assert.Equal(before + 1, probe.Seen.Count);
    }

    [Fact]
    public async Task Configured_base_timeout_reaches_read_and_write_commands()
    {
        var (cn, ctx, probe) = Create(baseTimeout: TimeSpan.FromSeconds(77));
        using var _cn = cn; await using var _ = ctx;

        await ctx.Query<Row>().Where(r => r.V >= 10).ToListAsync();
        await ctx.Query<Row>().Where(r => r.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(r => r.V, 11));

        var read = probe.Seen.First(s => s.Sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(77, read.Timeout);
        var update = probe.Seen.First(s => s.Sql.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(77, update.Timeout);
    }
}
