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
/// Pins server-side translation of the 7-arg
/// <c>new DateTimeOffset(year, month, day, hour, minute, second, offset)</c>
/// constructor when at least one date/time part is a column expression and the
/// offset is a constant <see cref="TimeSpan"/>. Companion of the 7-arg
/// <c>new DateTime</c> translation (commit 1faa09d); the offset arg is what
/// differentiates this from DateTime — the canonical text the materialiser
/// parses must include the trailing <c>+HH:MM</c> / <c>-HH:MM</c> tag.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeOffsetFromPartsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtoPartsRow (Id INTEGER PRIMARY KEY, Y INT NOT NULL, M INT NOT NULL, D INT NOT NULL, H INT NOT NULL, Mi INT NOT NULL, S INT NOT NULL);
            INSERT INTO DtoPartsRow VALUES
                (1, 2026, 5, 25, 12, 30, 45),
                (2, 2025, 12, 31, 23, 59, 59),
                (3, 2024,  2, 29,  0,  0,  0);
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
    public async Task Seven_arg_ctor_with_column_parts_and_zero_offset_round_trips()
    {
        var rows = (await _ctx.Query<DtoPartsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Dto = new DateTimeOffset(r.Y, r.M, r.D, r.H, r.Mi, r.S, TimeSpan.Zero) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(new DateTimeOffset(2026,  5, 25, 12, 30, 45, TimeSpan.Zero), rows[0].Dto);
        Assert.Equal(new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.Zero), rows[1].Dto);
        Assert.Equal(new DateTimeOffset(2024,  2, 29,  0,  0,  0, TimeSpan.Zero), rows[2].Dto);
    }

    [Fact]
    public async Task Seven_arg_ctor_with_column_parts_and_positive_offset_round_trips()
    {
        var rows = (await _ctx.Query<DtoPartsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Dto = new DateTimeOffset(r.Y, r.M, r.D, r.H, r.Mi, r.S, TimeSpan.FromHours(2)) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(new DateTimeOffset(2026,  5, 25, 12, 30, 45, TimeSpan.FromHours(2)), rows[0].Dto);
        Assert.Equal(new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.FromHours(2)), rows[1].Dto);
    }

    [Fact]
    public async Task Seven_arg_ctor_with_column_parts_and_negative_fractional_offset_round_trips()
    {
        // -05:30 (India inverse / Newfoundland-ish): exercises the negative-sign branch
        // and a non-zero minute part.
        var offset = TimeSpan.FromHours(-5) - TimeSpan.FromMinutes(30);
        var rows = (await _ctx.Query<DtoPartsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Dto = new DateTimeOffset(r.Y, r.M, r.D, r.H, r.Mi, r.S, offset) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(new DateTimeOffset(2026,  5, 25, 12, 30, 45, offset), rows[0].Dto);
        Assert.Equal(new DateTimeOffset(2024,  2, 29,  0,  0,  0, offset), rows[2].Dto);
    }

    [Table("DtoPartsRow")]
    public sealed class DtoPartsRow
    {
        [Key] public int Id { get; set; }
        public int Y { get; set; }
        public int M { get; set; }
        public int D { get; set; }
        public int H { get; set; }
        public int Mi { get; set; }
        public int S { get; set; }
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqProjectionDateTimeOffsetFromPartsLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Seven_arg_ctor_with_column_parts_round_trips_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveDtoPartsRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Dto = new DateTimeOffset(r.Y, r.M, r.D, r.H, r.Mi, r.S, TimeSpan.FromHours(2)) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(new DateTimeOffset(2026, 5, 25, 12, 30, 45, TimeSpan.FromHours(2)), rows[0].Dto);
                Assert.Equal(new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.FromHours(2)), rows[1].Dto);
                Assert.Equal(new DateTimeOffset(2024, 2, 29, 0, 0, 0, TimeSpan.FromHours(2)), rows[2].Dto);
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
            ProviderKind.SqlServer => "CREATE TABLE LiveDtoPartsRow (Id INT PRIMARY KEY, Y INT NOT NULL, M INT NOT NULL, D INT NOT NULL, H INT NOT NULL, Mi INT NOT NULL, S INT NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveDtoPartsRow\" (\"Id\" INT PRIMARY KEY, \"Y\" INT NOT NULL, \"M\" INT NOT NULL, \"D\" INT NOT NULL, \"H\" INT NOT NULL, \"Mi\" INT NOT NULL, \"S\" INT NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveDtoPartsRow (Id INT PRIMARY KEY, Y INT NOT NULL, M INT NOT NULL, D INT NOT NULL, H INT NOT NULL, Mi INT NOT NULL, S INT NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveDtoPartsRow\" VALUES (1,2026,5,25,12,30,45),(2,2025,12,31,23,59,59),(3,2024,2,29,0,0,0);"
            : "INSERT INTO LiveDtoPartsRow VALUES (1,2026,5,25,12,30,45),(2,2025,12,31,23,59,59),(3,2024,2,29,0,0,0);";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveDtoPartsRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveDtoPartsRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveDtoPartsRow")]
    public sealed class LiveDtoPartsRow
    {
        [Key] public int Id { get; set; }
        public int Y { get; set; }
        public int M { get; set; }
        public int D { get; set; }
        public int H { get; set; }
        public int Mi { get; set; }
        public int S { get; set; }
    }
}
