using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that same-shape queries with different literal constant values produce
/// distinct cache entries and return correct rows instead of reusing stale parameter values.
/// </summary>
public class PlanCacheLiteralVarianceTests
{
    public class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static SqliteConnection CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE User(Id INTEGER PRIMARY KEY, Name TEXT);" +
            "INSERT INTO User VALUES(1,'Alice');" +
            "INSERT INTO User VALUES(2,'Bob');" +
            "INSERT INTO User VALUES(3,'Carol');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Where_int_literal_1_then_2_returns_correct_rows()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Execute WHERE Id = 1 first — warms the cache
        var r1 = await ctx.Query<User>().Where(u => u.Id == 1).ToListAsync();
        Assert.Single(r1);
        Assert.Equal("Alice", r1[0].Name);

        // Execute WHERE Id = 2 — must NOT reuse the plan with @p0 = 1
        var r2 = await ctx.Query<User>().Where(u => u.Id == 2).ToListAsync();
        Assert.Single(r2);
        Assert.Equal("Bob", r2[0].Name);

        // Execute WHERE Id = 3 — a third distinct value
        var r3 = await ctx.Query<User>().Where(u => u.Id == 3).ToListAsync();
        Assert.Single(r3);
        Assert.Equal("Carol", r3[0].Name);
    }

    [Fact]
    public async Task Where_string_literal_Alice_then_Bob_returns_correct_rows()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var r1 = await ctx.Query<User>().Where(u => u.Name == "Alice").ToListAsync();
        Assert.Single(r1);
        Assert.Equal(1, r1[0].Id);

        var r2 = await ctx.Query<User>().Where(u => u.Name == "Bob").ToListAsync();
        Assert.Single(r2);
        Assert.Equal(2, r2[0].Id);
    }

    [Fact]
    public async Task Closure_variable_null_vs_nonnull_still_works()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Non-null closure variable
        string? name = "Alice";
        var r1 = await ctx.Query<User>().Where(u => u.Name == name).ToListAsync();
        Assert.Single(r1);
        Assert.Equal("Alice", r1[0].Name);

        // Null closure variable — should generate IS NULL SQL, not reuse Alice plan
        name = null;
        var r2 = await ctx.Query<User>().Where(u => u.Name == name).ToListAsync();
        Assert.Empty(r2); // no row has NULL Name
    }
}
