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
/// Pins server-side translation of <see cref="DateTimeOffset.ToOffset(TimeSpan)"/>
/// on a column with a compile-time constant offset. The UTC instant is invariant;
/// the projection emits canonical text at the new offset that the materialiser
/// routes through <see cref="DateTimeOffset.Parse(string)"/>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeOffsetToOffsetTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtoOffRow (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL);
            INSERT INTO DtoOffRow VALUES
                (1, '2026-05-25 12:30:45+00:00'),
                (2, '2025-12-31 23:00:00+01:00'),
                (3, '2024-02-29 00:00:00-05:00');
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
    public async Task ToOffset_with_zero_offset_returns_same_utc_instant_in_utc_wall_clock()
    {
        var rows = (await _ctx.Query<DtoOffRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Shifted = r.Dto.ToOffset(TimeSpan.Zero) })
            .ToListAsync())
            .ToArray();
        // Same UTC instant rendered at offset +00:00.
        Assert.Equal(new DateTimeOffset(2026,  5, 25, 12, 30, 45, TimeSpan.Zero),                       rows[0].Shifted);
        Assert.Equal(new DateTimeOffset(2025, 12, 31, 23,  0,  0, TimeSpan.FromHours(1)).ToOffset(TimeSpan.Zero),
                     rows[1].Shifted);
        Assert.Equal(new DateTimeOffset(2024,  2, 29,  0,  0,  0, TimeSpan.FromHours(-5)).ToOffset(TimeSpan.Zero),
                     rows[2].Shifted);
    }

    [Fact]
    public async Task ToOffset_with_positive_offset_shifts_wall_clock_preserving_instant()
    {
        var rows = (await _ctx.Query<DtoOffRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Shifted = r.Dto.ToOffset(TimeSpan.FromHours(2)) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(new DateTimeOffset(2026,  5, 25, 12, 30, 45, TimeSpan.Zero).ToOffset(TimeSpan.FromHours(2)),
                     rows[0].Shifted);
        Assert.Equal(new DateTimeOffset(2025, 12, 31, 23,  0,  0, TimeSpan.FromHours(1)).ToOffset(TimeSpan.FromHours(2)),
                     rows[1].Shifted);
        Assert.Equal(new DateTimeOffset(2024,  2, 29,  0,  0,  0, TimeSpan.FromHours(-5)).ToOffset(TimeSpan.FromHours(2)),
                     rows[2].Shifted);
    }

    [Table("DtoOffRow")]
    public sealed class DtoOffRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqProjectionDateTimeOffsetToOffsetLiveProviderTests
{
    private static readonly DateTimeOffset Row1Utc = new DateTimeOffset(2026,  5, 25, 12, 30, 45, TimeSpan.Zero);
    private static readonly DateTimeOffset Row2Utc = new DateTimeOffset(2025, 12, 31, 22,  0,  0, TimeSpan.Zero); // 2025-12-31 23:00:00+01:00 ≡ UTC
    private static readonly DateTimeOffset Row3Utc = new DateTimeOffset(2024,  2, 29,  5,  0,  0, TimeSpan.Zero); // 2024-02-29 00:00:00-05:00 ≡ UTC

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ToOffset_with_positive_offset_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveDtoOffRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Shifted = r.Dto.ToOffset(TimeSpan.FromHours(2)) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(Row1Utc.ToOffset(TimeSpan.FromHours(2)), rows[0].Shifted);
                Assert.Equal(Row2Utc.ToOffset(TimeSpan.FromHours(2)), rows[1].Shifted);
                Assert.Equal(Row3Utc.ToOffset(TimeSpan.FromHours(2)), rows[2].Shifted);
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
            ProviderKind.SqlServer => "CREATE TABLE LiveDtoOffRow (Id INT PRIMARY KEY, Dto DATETIMEOFFSET NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveDtoOffRow\" (\"Id\" INT PRIMARY KEY, \"Dto\" TIMESTAMPTZ NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveDtoOffRow (Id INT PRIMARY KEY, Dto DATETIME(6) NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();

        // Insert via parameterized command so each provider stores the same
        // UTC instant regardless of native storage shape.
        for (int i = 0; i < 3; i++)
        {
            var utc = i == 0 ? Row1Utc : (i == 1 ? Row2Utc : Row3Utc);
            await using var ins = ctx.Connection.CreateCommand();
            ins.CommandText = kind switch
            {
                ProviderKind.SqlServer => $"INSERT INTO LiveDtoOffRow VALUES ({i + 1}, @v)",
                ProviderKind.Postgres  => $"INSERT INTO \"LiveDtoOffRow\" VALUES ({i + 1}, @v)",
                ProviderKind.MySql     => $"INSERT INTO LiveDtoOffRow VALUES ({i + 1}, @v)",
                _ => throw new ArgumentOutOfRangeException()
            };
            var p = ins.CreateParameter();
            p.ParameterName = "@v";
            // For MySQL DATETIME we want UTC wall clock with no tz info (MySQL DATETIME has no tz).
            p.Value = kind == ProviderKind.MySql ? (object)utc.UtcDateTime : utc;
            ins.Parameters.Add(p);
            await ins.ExecuteNonQueryAsync();
        }
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveDtoOffRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveDtoOffRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveDtoOffRow")]
    public sealed class LiveDtoOffRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}
