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
/// Pins server-side translation of <c>TimeSpan.FromMilliseconds(column)</c> and
/// <c>TimeSpan.FromTicks(column)</c> in a projection. Both factories accept a
/// numeric column and produce a TimeSpan; nORM lowers them to REAL-seconds SQL
/// (the materializer's <c>double → TimeSpan.FromSeconds</c> path handles the
/// reconstruction).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeSpanFromMillisFromTicksColumnProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TsfRow (Id INTEGER PRIMARY KEY, Millis INTEGER NOT NULL, Ticks INTEGER NOT NULL);
            INSERT INTO TsfRow VALUES
                (1,   1000,  10000000),     -- 1 s
                (2,  60000, 600000000),     -- 60 s = 1 min
                (3,    500,   5000000),     -- 0.5 s
                (4, 123456,1234560000);     -- 123.456 s
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
    public async Task FromMilliseconds_on_int_column_lowers_to_seconds_and_round_trips()
    {
        var rows = (await _ctx.Query<TsfRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { Span = TimeSpan.FromMilliseconds(r.Millis) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(TimeSpan.FromMilliseconds(1000),   rows[0].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(60000),  rows[1].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(500),    rows[2].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(123456), rows[3].Span);
    }

    [Fact]
    public async Task FromTicks_on_long_column_lowers_to_seconds_and_round_trips()
    {
        var rows = (await _ctx.Query<TsfRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { Span = TimeSpan.FromTicks(r.Ticks) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(TimeSpan.FromTicks(10000000),   rows[0].Span);
        Assert.Equal(TimeSpan.FromTicks(600000000),  rows[1].Span);
        Assert.Equal(TimeSpan.FromTicks(5000000),    rows[2].Span);
        Assert.Equal(TimeSpan.FromTicks(1234560000), rows[3].Span);
    }

    [Fact]
    public async Task FromMilliseconds_inside_anonymous_projection()
    {
        var rows = (await _ctx.Query<TsfRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Span = TimeSpan.FromMilliseconds(r.Millis) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Equal(TimeSpan.FromMilliseconds(60000), rows[1].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(500),   rows[2].Span);
    }

    [Table("TsfRow")]
    public sealed class TsfRow
    {
        [Key] public int Id { get; set; }
        public int Millis { get; set; }
        public long Ticks { get; set; }
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqTimeSpanFromMillisFromTicksColumnProjectionLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task FromMilliseconds_on_int_column_lowers_to_seconds_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveTsfRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Span = TimeSpan.FromMilliseconds(r.Millis) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(TimeSpan.FromMilliseconds(1000),   rows[0].Span);
                Assert.Equal(TimeSpan.FromMilliseconds(60000),  rows[1].Span);
                Assert.Equal(TimeSpan.FromMilliseconds(500),    rows[2].Span);
                Assert.Equal(TimeSpan.FromMilliseconds(123456), rows[3].Span);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task FromTicks_on_long_column_lowers_to_seconds_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveTsfRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Span = TimeSpan.FromTicks(r.Ticks) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(TimeSpan.FromTicks(10000000),   rows[0].Span);
                Assert.Equal(TimeSpan.FromTicks(600000000),  rows[1].Span);
                Assert.Equal(TimeSpan.FromTicks(5000000),    rows[2].Span);
                Assert.Equal(TimeSpan.FromTicks(1234560000), rows[3].Span);
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
            ProviderKind.SqlServer => "CREATE TABLE LiveTsfRow (Id INT PRIMARY KEY, Millis INT NOT NULL, Ticks BIGINT NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveTsfRow\" (\"Id\" INT PRIMARY KEY, \"Millis\" INT NOT NULL, \"Ticks\" BIGINT NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveTsfRow (Id INT PRIMARY KEY, Millis INT NOT NULL, Ticks BIGINT NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveTsfRow\" VALUES (1,1000,10000000),(2,60000,600000000),(3,500,5000000),(4,123456,1234560000);"
            : "INSERT INTO LiveTsfRow VALUES (1,1000,10000000),(2,60000,600000000),(3,500,5000000),(4,123456,1234560000);";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveTsfRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveTsfRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveTsfRow")]
    public sealed class LiveTsfRow
    {
        [Key] public int Id { get; set; }
        public int Millis { get; set; }
        public long Ticks { get; set; }
    }
}
