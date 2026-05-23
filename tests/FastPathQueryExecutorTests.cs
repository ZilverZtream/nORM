using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class FastPathQueryExecutorTests
{
    private class User
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    private class Alpha
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class Beta
    {
        public int Id { get; set; }
        public bool IsEnabled { get; set; }
    }

    [Fact]
    public async Task Where_boolean_member_uses_fast_path()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE \"User\"(Id INTEGER, IsActive INTEGER);" +
                "INSERT INTO \"User\" VALUES(1,1);" +
                "INSERT INTO \"User\" VALUES(2,0);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<User>().Where(u => u.IsActive);
        var success = FastPathQueryExecutor.TryExecute<User>(query.Expression, ctx, CancellationToken.None, out var task);
        Assert.True(success);

        var results = (List<User>)await task.ConfigureAwait(true);
        Assert.Single(results);
        Assert.True(results[0].IsActive);
    }

    [Fact]
    public async Task Where_boolean_member_sync_fast_path_invokes_reader_interceptor()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE \"User\"(Id INTEGER, IsActive INTEGER);" +
                "INSERT INTO \"User\" VALUES(1,1);" +
                "INSERT INTO \"User\" VALUES(2,0);";
            cmd.ExecuteNonQuery();
        }

        var interceptor = new CountingReaderInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var results = await ctx.Query<User>().Where(u => u.IsActive).ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, interceptor.SyncReaderExecutingCount);
        Assert.Equal(0, interceptor.AsyncReaderExecutingCount);
    }

    [Fact]
    public async Task Count_without_predicate_uses_fast_path()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE \"User\"(Id INTEGER, IsActive INTEGER);" +
                "INSERT INTO \"User\" VALUES(1,1);" +
                "INSERT INTO \"User\" VALUES(2,0);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<User>();
        var expr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Count),
            new[] { typeof(User) },
            query.Expression);

        var success = FastPathQueryExecutor.TryExecute<User>(expr, ctx, CancellationToken.None, out var task);
        Assert.True(success);

        var count = (int)await task.ConfigureAwait(true);
        Assert.Equal(2, count);
    }

    [Fact]
    public void Sql_templates_are_cached_per_context()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE \"Alpha\"(Id INTEGER, Name TEXT);" +
                "CREATE TABLE \"Beta\"(Id INTEGER, IsEnabled INTEGER);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.FastPathSqlCache.Clear();

        var alphaTemplate = InvokeGetSqlTemplate(ctx, typeof(Alpha));
        var betaTemplate = InvokeGetSqlTemplate(ctx, typeof(Beta));

        var alphaMap = ctx.GetMapping(typeof(Alpha));
        var betaMap = ctx.GetMapping(typeof(Beta));
        var expectedAlpha = $"SELECT {string.Join(", ", alphaMap.Columns.Select(c => c.EscCol))} FROM {alphaMap.EscTable}";
        var expectedBeta = $"SELECT {string.Join(", ", betaMap.Columns.Select(c => c.EscCol))} FROM {betaMap.EscTable}";

        Assert.Equal(expectedAlpha, alphaTemplate);
        Assert.Equal(expectedBeta, betaTemplate);

        Assert.True(ctx.FastPathSqlCache.TryGetValue(typeof(Alpha), out var cachedAlpha));
        Assert.True(ctx.FastPathSqlCache.TryGetValue(typeof(Beta), out var cachedBeta));
        Assert.Equal(expectedAlpha, cachedAlpha);
        Assert.Equal(expectedBeta, cachedBeta);
    }

    private static string InvokeGetSqlTemplate(DbContext ctx, Type type)
    {
        var method = typeof(FastPathQueryExecutor)
            .GetMethod("GetSqlTemplate", BindingFlags.NonPublic | BindingFlags.Static)!;
        var generic = method.MakeGenericMethod(type);
        return (string)generic.Invoke(null, new object[] { ctx })!;
    }

    private sealed class CountingReaderInterceptor : IDbCommandInterceptor
    {
        public int SyncReaderExecutingCount;
        public int AsyncReaderExecutingCount;

        public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            Interlocked.Increment(ref SyncReaderExecutingCount);
            return InterceptionResult<DbDataReader>.Continue();
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AsyncReaderExecutingCount);
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
