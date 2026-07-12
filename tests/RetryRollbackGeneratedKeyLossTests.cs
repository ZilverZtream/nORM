using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// When a SaveChanges attempt inserts a DB-generated-key entity (stamping its key in memory) and
/// then fails transiently before commit, the transaction rolls back and discards the row - but the
/// entity keeps the key the rolled-back INSERT assigned. On the automatic retry the
/// "skip already-inserted" guard sees a non-default key and silently drops the row, so a
/// SaveChanges that reports success leaves the principal permanently unpersisted. The rolled-back
/// keys must be reset so the retry re-inserts them.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class RetryRollbackGeneratedKeyLossTests
{
    [Table("RrParent")]
    private class RrParent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("RrChild")]
    private class RrChild
    {
        // Explicit (non-generated) key -> plain NonQuery INSERT, distinct from the parent's
        // reader-based identity insert, so the interceptor targets only this row.
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    /// <summary>
    /// Throws a transient <see cref="DbException"/> exactly once, on the first INSERT into RrChild,
    /// forcing a pre-commit rollback after RrParent's key has already been assigned.
    /// </summary>
    private sealed class OneShotChildInsertFailure : IDbCommandInterceptor
    {
        private int _fired;
        public int FireCount => _fired;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            if (command.CommandText.Contains("RrChild", StringComparison.Ordinal)
                && Interlocked.Exchange(ref _fired, 1) == 0)
                throw new SqliteException("Injected transient lock", 6 /* SQLITE_LOCKED */);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand c, DbContext ctx, int r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand c, DbContext ctx, object? r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        public Task ReaderExecutedAsync(DbCommand c, DbContext ctx, DbDataReader r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand c, DbContext ctx, Exception ex, CancellationToken ct) => Task.CompletedTask;
    }

    private static (int Parents, int Children) Counts(SqliteConnection cn)
    {
        using var q = cn.CreateCommand();
        q.CommandText = "SELECT (SELECT COUNT(*) FROM RrParent), (SELECT COUNT(*) FROM RrChild)";
        using var r = q.ExecuteReader();
        r.Read();
        return (r.GetInt32(0), r.GetInt32(1));
    }

    [Fact]
    public async Task Retry_after_rollback_reinserts_db_generated_key_rows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE RrParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                "CREATE TABLE RrChild (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        var interceptor = new OneShotChildInsertFailure();
        var opts = new DbContextOptions
        {
            RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = ex => ex is SqliteException
            }
        };
        opts.CommandInterceptors.Add(interceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var parent = new RrParent { Name = "keep-me" };
        ctx.Add(parent);                       // added first -> inserted first (key assigned)
        ctx.Add(new RrChild { Id = 100, Name = "child" }); // its insert throws once -> rollback

        await ctx.SaveChangesAsync();

        Assert.Equal(1, interceptor.FireCount);      // the transient failure really fired
        var (parents, children) = Counts(cn);
        Assert.Equal(1, parents);                    // silently 0 before the fix: the parent row is lost
        Assert.Equal(1, children);
    }
}
