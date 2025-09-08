using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

        var results = (List<User>)await task.ConfigureAwait(false);
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

        var count = (int)await task.ConfigureAwait(false);
        Assert.Equal(2, count);
    }
}
