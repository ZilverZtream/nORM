using System.Linq;
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
        var success = FastPathQueryExecutor.TryExecute(query.Expression, ctx, out var task);
        Assert.True(success);

        var results = await task.ConfigureAwait(false);
        Assert.Single(results);
        Assert.True(results[0].IsActive);
    }
}
