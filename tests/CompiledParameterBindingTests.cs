using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Validates that compiled parameter extraction produces correct values
/// for runtime parameters (closure captures) in LINQ queries. Verifies that
/// lambda parameters and IQueryable sources do not contaminate the extracted
/// parameter values.
/// </summary>
public class CompiledParameterBindingTests
{
    public class Widget
    {
        public int Id { get; set; }
        public string Color { get; set; } = string.Empty;
        public int Size { get; set; }
    }

    [Fact]
    public async Task SingleClosureCapture_BindsCorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Widget(Id INTEGER, Color TEXT, Size INTEGER);" +
                              "INSERT INTO Widget VALUES(1,'Red',10);" +
                              "INSERT INTO Widget VALUES(2,'Blue',20);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        int targetId = 1;
        var results = await ctx.Query<Widget>().Where(w => w.Id == targetId).ToListAsync();

        Assert.Single(results);
        Assert.Equal("Red", results[0].Color);
    }

    [Fact]
    public async Task MultipleClosureCaptures_AllBindCorrectly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Widget(Id INTEGER, Color TEXT, Size INTEGER);" +
                              "INSERT INTO Widget VALUES(1,'Red',10);" +
                              "INSERT INTO Widget VALUES(2,'Blue',20);" +
                              "INSERT INTO Widget VALUES(3,'Red',30);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        string targetColor = "Red";
        int minSize = 15;
        var results = await ctx.Query<Widget>()
            .Where(w => w.Color == targetColor && w.Size > minSize)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(3, results[0].Id);
        Assert.Equal(30, results[0].Size);
    }

    [Fact]
    public async Task CompiledQuery_WithRuntimeParam_BindsNewValueOnEachCall()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Widget(Id INTEGER, Color TEXT, Size INTEGER);" +
                              "INSERT INTO Widget VALUES(1,'Red',10);" +
                              "INSERT INTO Widget VALUES(2,'Blue',20);";
            cmd.ExecuteNonQuery();
        }

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<Widget>().Where(w => w.Id == id));

        using var ctx = new DbContext(cn, new SqliteProvider());

        // First call
        var r1 = await compiled(ctx, 1);
        Assert.Single(r1);
        Assert.Equal("Red", r1[0].Color);

        // Second call with different value — compiled params must rebind correctly
        var r2 = await compiled(ctx, 2);
        Assert.Single(r2);
        Assert.Equal("Blue", r2[0].Color);
    }

    [Fact]
    public async Task QueryPlan_Reused_WithDifferentClosureValues_BindsCorrectly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Widget(Id INTEGER, Color TEXT, Size INTEGER);" +
                              "INSERT INTO Widget VALUES(1,'Red',10);" +
                              "INSERT INTO Widget VALUES(2,'Blue',20);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        // Execute same query shape twice with different closure values to exercise plan caching + rebinding
        for (int targetId = 1; targetId <= 2; targetId++)
        {
            int capturedId = targetId;
            var results = await ctx.Query<Widget>().Where(w => w.Id == capturedId).ToListAsync();
            Assert.Single(results);
            Assert.Equal(targetId, results[0].Id);
        }
    }
}
