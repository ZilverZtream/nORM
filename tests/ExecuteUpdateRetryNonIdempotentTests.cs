using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A set-based ExecuteUpdate/ExecuteDelete is a single non-idempotent autocommit statement. Under a retry
/// policy it must NOT be retried after the statement has executed: a transient error raised after the server
/// already applied the statement (e.g. a dropped connection while reading the ack) would otherwise re-run it
/// and double-apply a relative update / delete a second page of rows. Matches how the Bulk* operations are
/// guarded.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ExecuteUpdateRetryNonIdempotentTests
{
    [Table("A2Acct")]
    public class Acct
    {
        [Key] public int Id { get; set; }
        public int Balance { get; set; }
    }

    private sealed class FakeTransientException : DbException
    {
        public FakeTransientException() : base("simulated transient failure after execution") { }
    }

    // Throws a transient DbException from the FIRST NonQueryExecuted callback — after the UPDATE has already
    // applied — then no-ops, so a retry would run the statement a second time.
    private sealed class ThrowOnceAfterExecuteInterceptor : IDbCommandInterceptor
    {
        private int _thrown;
        public int NonQueryExecutions;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand c, DbContext x, CancellationToken t)
            => Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand c, DbContext x, int r, TimeSpan d, CancellationToken t)
        {
            Interlocked.Increment(ref NonQueryExecutions);
            if (Interlocked.Exchange(ref _thrown, 1) == 0)
                throw new FakeTransientException();
            return Task.CompletedTask;
        }
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand c, DbContext x, CancellationToken t)
            => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand c, DbContext x, object? r, TimeSpan d, CancellationToken t) => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand c, DbContext x, CancellationToken t)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        public Task ReaderExecutedAsync(DbCommand c, DbContext x, DbDataReader r, TimeSpan d, CancellationToken t) => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand c, DbContext x, Exception e, CancellationToken t) => Task.CompletedTask;
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create(ThrowOnceAfterExecuteInterceptor interceptor)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE A2Acct (Id INTEGER PRIMARY KEY, Balance INTEGER NOT NULL); INSERT INTO A2Acct (Id, Balance) VALUES (1, 100);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            RetryPolicy = new RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.FromMilliseconds(1), ShouldRetry = ex => ex is DbException },
        };
        opts.CommandInterceptors.Add(interceptor);
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static long Balance(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Balance FROM A2Acct WHERE Id = 1";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task ExecuteUpdate_relative_set_is_not_retried_after_it_executed()
    {
        var interceptor = new ThrowOnceAfterExecuteInterceptor();
        var (cn, ctx) = Create(interceptor);
        using var _cn = cn;
        await using var _ctx = ctx;

        // Balance += 100 on a row currently at 100. The statement applies once (100 -> 200), then the
        // interceptor raises a transient failure. It must NOT be retried (which would make it 300).
        await Record.ExceptionAsync(() =>
            ctx.Query<Acct>().Where(a => a.Id == 1)
                .ExecuteUpdateAsync(s => s.SetProperty(a => a.Balance, a => a.Balance + 100)));

        Assert.Equal(200L, Balance(cn));         // BUG: 300 — the relative update was double-applied by the retry
        Assert.Equal(1, interceptor.NonQueryExecutions); // executed exactly once
    }
}
