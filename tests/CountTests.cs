using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CountTests
{
    private class User
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    [Fact]
    public async Task Count_returns_matching_rows()
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
        var count = await ctx.Query<User>().Where(u => u.IsActive).CountAsync();
        Assert.Equal(1, count);
    }
}
