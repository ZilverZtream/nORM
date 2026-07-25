using System;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A <see cref="BaseDbCommandInterceptor"/> subclass must observe commands IDENTICALLY on the synchronous
/// and asynchronous execution paths. SQLite sets <c>PrefersSyncExecution</c>, and the small-query fast
/// paths / compiled-query / query-plan paths run synchronously on every provider — so a consumer audit
/// log or policy guard derived from the base would fail OPEN (silently miss most commands) if the base's
/// sync hooks stayed no-ops while only its async hooks logged. These pin that the base's built-in redacted
/// logging fires on both paths, so deriving from the base is path-agnostic by default.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class BaseInterceptorSyncPathObservationTests
{
    private sealed class CapturingLogger : ILogger
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? ex, Func<TState, Exception?, string> formatter)
            => Messages.Enqueue(formatter(state, ex));
    }

    // Adds NO overrides — it relies entirely on the base's built-in logging, exactly as a consumer that
    // "just wants the redacted command log / audit trail on both paths" would.
    private sealed class BaseOnlyInterceptor : BaseDbCommandInterceptor
    {
        public BaseOnlyInterceptor(ILogger logger) : base(logger) { }
    }

    [Table("Item")]
    private sealed class Item
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static SqliteConnection SeededDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL); INSERT INTO Item (Name) VALUES ('a')";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public void Base_interceptor_logs_the_reader_command_on_the_synchronous_path()
    {
        using var cn = SeededDb();
        var logger = new CapturingLogger();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(new BaseOnlyInterceptor(logger));
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        _ = ctx.Query<Item>().ToList();   // SQLite executes this synchronously (PrefersSyncExecution)

        Assert.Contains(logger.Messages, m => m.Contains("Executing reader", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Base_interceptor_logs_the_reader_command_on_the_asynchronous_path()
    {
        using var cn = SeededDb();
        var logger = new CapturingLogger();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(new BaseOnlyInterceptor(logger));
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        _ = await ctx.Query<Item>().ToListAsync();

        Assert.Contains(logger.Messages, m => m.Contains("Executing reader", StringComparison.Ordinal));
    }

    [Fact]
    public void Base_interceptor_logs_the_scalar_command_on_the_synchronous_count_path()
    {
        using var cn = SeededDb();
        var logger = new CapturingLogger();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(new BaseOnlyInterceptor(logger));
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        _ = ctx.Query<Item>().Count();   // synchronous scalar path

        // Count executes as a scalar (or, on some fast paths, a reader) — either way the base must observe it.
        Assert.Contains(logger.Messages,
            m => m.Contains("Executing scalar", StringComparison.Ordinal) || m.Contains("Executing reader", StringComparison.Ordinal));
    }
}
