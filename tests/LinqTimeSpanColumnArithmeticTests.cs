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
/// Pins server-side translation of <c>col1 + col2</c> and <c>col1 - col2</c>
/// where both operands are <see cref="TimeSpan"/> columns. Companion of
/// <c>TimeSpan.From*(column)</c> (commit c750535) and the existing
/// <c>(DateTime - DateTime).TotalSeconds</c> path — exercises addition and
/// subtraction between two TimeSpan columns rather than between a DateTime
/// pair.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeSpanColumnArithmeticTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TsaRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO TsaRow VALUES
                (1, '01:00:00',    '00:30:00'),     -- 1h + 30m, 1h - 30m
                (2, '02:15:00',    '01:45:00'),     -- 4h, 30m
                (3, '00:00:30',    '00:00:15');     -- 45s, 15s
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
    public async Task Addition_of_two_TimeSpan_columns_yields_sum_in_projection()
    {
        var rows = (await _ctx.Query<TsaRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Sum = r.A + r.B })
            .ToListAsync())
            .ToArray();
        Assert.Equal(TimeSpan.Parse("01:30:00"), rows[0].Sum);
        Assert.Equal(TimeSpan.Parse("04:00:00"), rows[1].Sum);
        Assert.Equal(TimeSpan.Parse("00:00:45"), rows[2].Sum);
    }

    [Fact]
    public async Task Subtraction_of_two_TimeSpan_columns_yields_diff_in_projection()
    {
        var rows = (await _ctx.Query<TsaRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Diff = r.A - r.B })
            .ToListAsync())
            .ToArray();
        Assert.Equal(TimeSpan.Parse("00:30:00"), rows[0].Diff);
        Assert.Equal(TimeSpan.Parse("00:30:00"), rows[1].Diff);
        Assert.Equal(TimeSpan.Parse("00:00:15"), rows[2].Diff);
    }

    [Table("TsaRow")]
    public sealed class TsaRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan A { get; set; }
        public TimeSpan B { get; set; }
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqTimeSpanColumnArithmeticLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Addition_of_two_TimeSpan_columns_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveTsaRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Sum = r.A + r.B })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(TimeSpan.Parse("01:30:00"), rows[0].Sum);
                Assert.Equal(TimeSpan.Parse("04:00:00"), rows[1].Sum);
                Assert.Equal(TimeSpan.Parse("00:00:45"), rows[2].Sum);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Subtraction_of_two_TimeSpan_columns_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveTsaRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Diff = r.A - r.B })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(TimeSpan.Parse("00:30:00"), rows[0].Diff);
                Assert.Equal(TimeSpan.Parse("00:30:00"), rows[1].Diff);
                Assert.Equal(TimeSpan.Parse("00:00:15"), rows[2].Diff);
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
            ProviderKind.SqlServer => "CREATE TABLE LiveTsaRow (Id INT PRIMARY KEY, A TIME(7) NOT NULL, B TIME(7) NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveTsaRow\" (\"Id\" INT PRIMARY KEY, \"A\" INTERVAL NOT NULL, \"B\" INTERVAL NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveTsaRow (Id INT PRIMARY KEY, A TIME(6) NOT NULL, B TIME(6) NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveTsaRow\" VALUES (1,'01:00:00','00:30:00'),(2,'02:15:00','01:45:00'),(3,'00:00:30','00:00:15');"
            : "INSERT INTO LiveTsaRow VALUES (1,'01:00:00','00:30:00'),(2,'02:15:00','01:45:00'),(3,'00:00:30','00:00:15');";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveTsaRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveTsaRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveTsaRow")]
    public sealed class LiveTsaRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan A { get; set; }
        public TimeSpan B { get; set; }
    }
}
