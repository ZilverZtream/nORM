using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
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
/// CT-1: Verifies that post-commit interceptor exceptions are swallowed (logged, not propagated)
/// so that a successful database commit never surfaces as a false SaveChanges failure.
/// </summary>
public class PostCommitInterceptorExceptionTests
{
    [Table("PceItem")]
    private class PceItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── Fake logger that captures log calls ──────────────────────────────────

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add((logLevel, formatter(state, exception), exception));
        }
    }

    // ── Interceptors ─────────────────────────────────────────────────────────

    private sealed class ThrowingInterceptor : ISaveChangesInterceptor
    {
        public int SavingCalls { get; private set; }
        public int SavedCalls { get; private set; }
        public Exception ThrownException { get; } = new InvalidOperationException("Interceptor intentional failure");

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            CancellationToken cancellationToken)
        {
            SavingCalls++;
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            int result, CancellationToken cancellationToken)
        {
            SavedCalls++;
            throw ThrownException;
        }
    }

    private sealed class RecordingInterceptor : ISaveChangesInterceptor
    {
        public int SavedCalls { get; private set; }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            int result, CancellationToken cancellationToken)
        {
            SavedCalls++;
            return Task.CompletedTask;
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx, CapturingLogger Logger) CreateContext(
        params ISaveChangesInterceptor[] interceptors)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PceItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var logger = new CapturingLogger();
        var opts = new DbContextOptions { Logger = logger };
        foreach (var i in interceptors)
            opts.SaveChangesInterceptors.Add(i);

        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx, logger);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// CT-1: SaveChangesAsync must NOT throw when a post-commit interceptor throws.
    /// The database row is already committed; propagating the exception would cause
    /// false-failure reports and potential duplicate-retry side effects.
    /// </summary>
    [Fact]
    public async Task SavedChangesAsync_InterceptorThrows_DoesNotPropagateException()
    {
        var throwing = new ThrowingInterceptor();
        var (cn, ctx, _) = CreateContext(throwing);
        await using var _ = cn;

        ctx.Add(new PceItem { Name = "safe-commit" });

        // CT-1: Must not throw even though the interceptor does.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    /// <summary>
    /// CT-1: The row committed to the database must be retrievable even when the
    /// post-commit interceptor throws.
    /// </summary>
    [Fact]
    public async Task SavedChangesAsync_InterceptorThrows_RowIsStillCommitted()
    {
        var throwing = new ThrowingInterceptor();
        var (cn, ctx, _) = CreateContext(throwing);
        await using var _ = cn;

        ctx.Add(new PceItem { Name = "persisted" });
        await ctx.SaveChangesAsync();

        // Verify the row is in the database.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PceItem WHERE Name = 'persisted'";
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(1, count);
    }

    /// <summary>
    /// CT-1: When a post-commit interceptor throws, a warning must be logged with
    /// the interceptor type name and the exception, so the failure is observable
    /// without surfacing as a false operation failure.
    /// </summary>
    [Fact]
    public async Task SavedChangesAsync_InterceptorThrows_LogsWarning()
    {
        var throwing = new ThrowingInterceptor();
        var (cn, ctx, logger) = CreateContext(throwing);
        await using var _ = cn;

        ctx.Add(new PceItem { Name = "logged-warning" });
        await ctx.SaveChangesAsync();

        // CT-1: A warning must have been logged.
        Assert.Contains(logger.Entries, e =>
            e.Level == LogLevel.Warning &&
            e.Exception == throwing.ThrownException);
    }

    /// <summary>
    /// CT-1: When multiple interceptors are registered and the first throws, the second
    /// must still be called. Each interceptor is wrapped independently.
    /// </summary>
    [Fact]
    public async Task SavedChangesAsync_MultipleInterceptors_AllRunDespiteFirstThrow()
    {
        var throwing = new ThrowingInterceptor();
        var recording = new RecordingInterceptor();
        var (cn, ctx, _) = CreateContext(throwing, recording);
        await using var _ = cn;

        ctx.Add(new PceItem { Name = "multi-interceptor" });
        await ctx.SaveChangesAsync();

        // CT-1: Both interceptors must have been called.
        Assert.Equal(1, throwing.SavedCalls);
        Assert.Equal(1, recording.SavedCalls);
    }
}
