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
/// Pins <c>dtoCol == dateTimeLiteral</c> equality in WHERE. .NET defines
/// equality across DateTimeOffset and DateTime as a comparison of UTC
/// instants — DateTime.Kind=Utc keeps offset 0; Unspecified/Local is
/// interpreted at the local-machine offset (captured at query-build time
/// for snapshot semantics, mirroring [[dto-local-datetime]]). The
/// SQLite-text storage of DTO means a naive CAST to TEXT would compare
/// canonical text and miss matches across equivalent UTC instants
/// expressed in different offsets, so the comparison must lower to
/// instant equality (UTC second-of-epoch or canonical UTC text).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeOffsetEqualsDateTimeLiteralTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DeqRow (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL);
            INSERT INTO DeqRow VALUES
                (1, '2026-05-25 12:30:45.123+00:00'),
                (2, '2026-05-25 14:30:45.123+02:00'),
                (3, '2026-05-25 12:30:45.987+00:00'),
                (4, '2026-05-25 12:30:46.123+00:00'),
                (5, '2026-05-25 12:30:45.123456+00:00'),
                (6, '2026-05-25 12:30:45.123999+00:00');
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
    public async Task DateTimeOffset_column_equals_UTC_DateTime_literal_matches_by_UTC_instant()
    {
        // Rows 1 and 2 represent the same UTC instant (12:30:45Z) in two
        // different offsets. Equality must match BOTH.
        var literal = new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc);
        var rows = await _ctx.Query<DeqRow>()
            .Where(r => r.Dto == literal)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task DateTimeOffset_column_not_equals_UTC_DateTime_literal_excludes_matching_instants()
    {
        var literal = new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc);
        var rows = await _ctx.Query<DeqRow>()
            .Where(r => r.Dto != literal)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4, 5, 6 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task DateTimeOffset_column_equals_UTC_DateTime_literal_matches_microsecond_precision()
    {
        var literal = new DateTime(2026, 5, 25, 12, 30, 45, DateTimeKind.Utc).AddTicks(1234560);
        var rows = await _ctx.Query<DeqRow>()
            .Where(r => r.Dto == literal)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 5 }, rows.Select(r => r.Id).ToArray());
    }

    // No OrderBy: this shape takes the SIMPLE-WHERE fast path, which must lower the DTO==DateTime
    // comparison to UTC-instant equality exactly as the filtered-ordered path and full translator do —
    // a raw `Dto = @p` is a lexical TEXT compare that misses the offset-shifted equivalent instant (row 2).
    [Fact]
    public async Task DateTimeOffset_equals_UTC_DateTime_literal_via_simple_where_matches_by_UTC_instant()
    {
        var literal = new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc);
        var rows = await _ctx.Query<DeqRow>().Where(r => r.Dto == literal).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    // Count(predicate) takes the direct-count fast path, which must lower the same way — a raw `Dto = @p`
    // counts only the exact-offset text and under-counts equivalent instants.
    [Fact]
    public async Task DateTimeOffset_equals_UTC_DateTime_literal_via_count_matches_by_UTC_instant()
    {
        var literal = new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc);
        var count = await _ctx.Query<DeqRow>().CountAsync(r => r.Dto == literal);
        Assert.Equal(2, count);
    }

    // DateTimeOffset == DateTimeOffset literal must match by UTC instant: rows 1 and 2 are the same
    // instant in different offsets, so both must match a literal for that instant. A raw `Dto = @p` on
    // any fast path is a lexical TEXT compare that finds only the exact-offset row (1) — the full
    // translator already lowers to instant equality, so every read fast path must defer here.
    private static readonly DateTimeOffset SameInstantAsRows1And2 =
        new DateTimeOffset(2026, 5, 25, 12, 30, 45, 123, TimeSpan.Zero);

    [Fact]
    public async Task DateTimeOffset_equals_DateTimeOffset_literal_full_translator_matches_by_instant()
    {
        var ids = await _ctx.Query<DeqRow>().Where(r => r.Dto == SameInstantAsRows1And2)
            .Select(r => new { r.Id }).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task DateTimeOffset_equals_DateTimeOffset_literal_filtered_ordered_matches_by_instant()
    {
        var rows = await _ctx.Query<DeqRow>().Where(r => r.Dto == SameInstantAsRows1And2)
            .OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task DateTimeOffset_equals_DateTimeOffset_literal_simple_where_matches_by_instant()
    {
        var rows = await _ctx.Query<DeqRow>().Where(r => r.Dto == SameInstantAsRows1And2).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task DateTimeOffset_equals_DateTimeOffset_literal_via_count_matches_by_instant()
    {
        var count = await _ctx.Query<DeqRow>().CountAsync(r => r.Dto == SameInstantAsRows1And2);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task DateTimeOffset_column_equals_unspecified_local_DateTime_literal_uses_literal_instant_offset()
    {
        var utcInstant = new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc);
        var localWall = TimeZoneInfo.ConvertTimeFromUtc(utcInstant, TimeZoneInfo.Local);
        var literal = DateTime.SpecifyKind(localWall, DateTimeKind.Unspecified);

        var rows = await _ctx.Query<DeqRow>()
            .Where(r => r.Dto == literal)
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("DeqRow")]
    public sealed class DeqRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}
