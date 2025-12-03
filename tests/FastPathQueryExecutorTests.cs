using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

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
        var success = FastPathQueryExecutor.TryExecute<User>(query.Expression, ctx, out var task);
        Assert.True(success);

        var results = (List<User>)await task.ConfigureAwait(true);
        Assert.Single(results);
        Assert.True(results[0].IsActive);
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

        var success = FastPathQueryExecutor.TryExecute<User>(expr, ctx, out var task);
        Assert.True(success);

        var count = (int)await task.ConfigureAwait(true);
        Assert.Equal(2, count);
    }

    [Fact]
    public void Sql_templates_are_cached_per_type()
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
        var cacheField = typeof(FastPathQueryExecutor)
            .GetField("_sqlTemplateCache", BindingFlags.NonPublic | BindingFlags.Static)!;
        var cache = Assert.IsType<ConcurrentDictionary<Type, string>>(cacheField.GetValue(null));
        cache.Clear();

        var alphaTemplate = InvokeGetSqlTemplate(ctx, typeof(Alpha));
        var betaTemplate = InvokeGetSqlTemplate(ctx, typeof(Beta));

        var alphaMap = ctx.GetMapping(typeof(Alpha));
        var betaMap = ctx.GetMapping(typeof(Beta));
        var expectedAlpha = $"SELECT {string.Join(", ", alphaMap.Columns.Select(c => c.EscCol))} FROM {alphaMap.EscTable}";
        var expectedBeta = $"SELECT {string.Join(", ", betaMap.Columns.Select(c => c.EscCol))} FROM {betaMap.EscTable}";

        Assert.Equal(expectedAlpha, alphaTemplate);
        Assert.Equal(expectedBeta, betaTemplate);

        Assert.True(cache.TryGetValue(typeof(Alpha), out var cachedAlpha));
        Assert.True(cache.TryGetValue(typeof(Beta), out var cachedBeta));
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
}
