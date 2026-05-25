using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of <see cref="Math.Clamp(double, double, double)"/>
/// and <see cref="Math.IEEERemainder(double, double)"/> when applied to column
/// expressions. Both already work on SQLite; the implementation extends the same
/// translations to SqlServer, Postgres, and MySQL via their Math switches.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMathClampIeeeRemainderTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE MciRow (Id INTEGER PRIMARY KEY, V REAL NOT NULL, Divisor REAL NOT NULL);
            INSERT INTO MciRow VALUES
                (1,   5.0, 3.0),
                (2,  -7.0, 4.0),
                (3,  15.0, 2.5),
                (4, 100.0, 6.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Clamp_with_column_value_constrains_to_range()
    {
        var rows = (await _ctx.Query<MciRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Clamped = Math.Clamp(r.V, 0.0, 10.0) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(5.0,  rows[0].Clamped);
        Assert.Equal(0.0,  rows[1].Clamped);
        Assert.Equal(10.0, rows[2].Clamped);
        Assert.Equal(10.0, rows[3].Clamped);
    }

    [Fact]
    public async Task IEEERemainder_on_column_pair_matches_dotnet_semantics()
    {
        var rows = (await _ctx.Query<MciRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Rem = Math.IEEERemainder(r.V, r.Divisor) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(Math.IEEERemainder(5.0,   3.0), rows[0].Rem, 10);
        Assert.Equal(Math.IEEERemainder(-7.0,  4.0), rows[1].Rem, 10);
        Assert.Equal(Math.IEEERemainder(15.0,  2.5), rows[2].Rem, 10);
        Assert.Equal(Math.IEEERemainder(100.0, 6.0), rows[3].Rem, 10);
    }

    [Table("MciRow")]
    public sealed class MciRow
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
        public double Divisor { get; set; }
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqMathClampIeeeRemainderLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Clamp_on_column_constrains_to_range_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var rows = (await ctx.Query<LiveMciRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Clamped = Math.Clamp(r.V, 0.0, 10.0) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(5.0,  rows[0].Clamped);
                Assert.Equal(0.0,  rows[1].Clamped);
                Assert.Equal(10.0, rows[2].Clamped);
                Assert.Equal(10.0, rows[3].Clamped);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task IEEERemainder_on_column_pair_matches_dotnet_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var rows = (await ctx.Query<LiveMciRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Rem = Math.IEEERemainder(r.V, r.Divisor) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(Math.IEEERemainder(5.0,   3.0), rows[0].Rem, 10);
                Assert.Equal(Math.IEEERemainder(-7.0,  4.0), rows[1].Rem, 10);
                Assert.Equal(Math.IEEERemainder(15.0,  2.5), rows[2].Rem, 10);
                Assert.Equal(Math.IEEERemainder(100.0, 6.0), rows[3].Rem, 10);
            }
            finally { await Teardown(ctx); }
        }
    }

    private static async Task Setup(DbContext ctx, ProviderKind kind)
    {
        await Teardown(ctx);
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = kind switch
        {
            ProviderKind.SqlServer => "CREATE TABLE LiveMciRow (Id INT PRIMARY KEY, V FLOAT NOT NULL, Divisor FLOAT NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveMciRow\" (\"Id\" INT PRIMARY KEY, \"V\" DOUBLE PRECISION NOT NULL, \"Divisor\" DOUBLE PRECISION NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveMciRow (Id INT PRIMARY KEY, V DOUBLE NOT NULL, Divisor DOUBLE NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveMciRow\" VALUES (1,5.0,3.0),(2,-7.0,4.0),(3,15.0,2.5),(4,100.0,6.0);"
            : "INSERT INTO LiveMciRow VALUES (1,5.0,3.0),(2,-7.0,4.0),(3,15.0,2.5),(4,100.0,6.0);";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveMciRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveMciRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveMciRow")]
    public sealed class LiveMciRow
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
        public double Divisor { get; set; }
    }
}
