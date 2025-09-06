using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class JoinMaterializationTests
{
    private class BenchmarkUser
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class BenchmarkOrder
    {
        public int Id { get; set; }
        public int UserId { get; set; }
        public double Amount { get; set; }
        public string ProductName { get; set; } = string.Empty;
    }

    private class JoinDto
    {
        public string Name { get; set; } = string.Empty;
        public double Amount { get; set; }
        public string ProductName { get; set; } = string.Empty;
    }

    [Fact]
    public async Task Join_projection_materializes_into_dto()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BenchmarkUser(Id INTEGER, Name TEXT);" +
                             "CREATE TABLE BenchmarkOrder(Id INTEGER, UserId INTEGER, Amount REAL, ProductName TEXT);" +
                             "INSERT INTO BenchmarkUser VALUES(1, 'A');" +
                             "INSERT INTO BenchmarkOrder VALUES(1, 1, 150, 'P1');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<BenchmarkUser>()
            .Join(ctx.Query<BenchmarkOrder>(), u => u.Id, o => o.UserId,
                (u, o) => new JoinDto { Name = u.Name, Amount = o.Amount, ProductName = o.ProductName })
            .ToListAsync();

        Assert.Single(results);
        var dto = results[0];
        Assert.Equal("A", dto.Name);
        Assert.Equal(150d, dto.Amount);
        Assert.Equal("P1", dto.ProductName);
    }
}
