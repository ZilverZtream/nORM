using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the EF Core-style raw-SQL scalar/DTO surface: SqlQueryRawAsync / SqlQueryInterpolatedAsync
/// materialize scalars (from the first column) and arbitrary DTOs (by column name), unlike FromSqlRaw
/// which is limited to mapped entities.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DatabaseSqlQueryContractTests
{
    public class RegionTotal
    {
        public string Region { get; set; } = "";
        public int Total { get; set; }
    }

    private static DbContext NewCtx(SqliteConnection cn)
        => new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SqRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Total INTEGER NOT NULL);
            INSERT INTO SqRow VALUES (1,'east',10),(2,'west',20),(3,'east',5);
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task SqlQueryRawAsync_reads_a_scalar_count()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var counts = await ctx.SqlQueryRawAsync<int>("SELECT COUNT(*) FROM SqRow");

        Assert.Equal(new[] { 3 }, counts);
    }

    [Fact]
    public async Task SqlQueryRawAsync_reads_scalar_strings_from_the_first_column()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var regions = await ctx.SqlQueryRawAsync<string>("SELECT Region FROM SqRow ORDER BY Id");

        Assert.Equal(new[] { "east", "west", "east" }, regions);
    }

    [Fact]
    public async Task SqlQueryRawAsync_maps_columns_to_a_dto_by_name()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var rows = await ctx.SqlQueryRawAsync<RegionTotal>(
            "SELECT Region, SUM(Total) AS Total FROM SqRow GROUP BY Region ORDER BY Region");

        Assert.Equal(2, rows.Count);
        Assert.Equal("east", rows[0].Region);
        Assert.Equal(15, rows[0].Total);
        Assert.Equal("west", rows[1].Region);
        Assert.Equal(20, rows[1].Total);
    }

    [Fact]
    public async Task SqlQueryInterpolatedAsync_parameterizes_holes()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var threshold = 8;
        var counts = await ctx.SqlQueryInterpolatedAsync<int>(
            $"SELECT COUNT(*) FROM SqRow WHERE Total > {threshold}");

        Assert.Equal(new[] { 2 }, counts);   // 10 and 20 exceed 8
    }
}
