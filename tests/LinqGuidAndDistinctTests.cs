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
/// Verifies a handful of frequently-used LINQ shapes that don't have a more specific test home:
/// Guid columns roundtrip cleanly through Where + projection, comparisons against Guid.Empty,
/// Distinct() after a column projection deduplicates correctly server-side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGuidAndDistinctTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    private static readonly Guid IdA = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid IdB = Guid.Parse("22222222-2222-2222-2222-222222222222");

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GdRow (Id INTEGER PRIMARY KEY, Token TEXT NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO GdRow VALUES
                (1, '11111111-1111-1111-1111-111111111111', 'red'),
                (2, '22222222-2222-2222-2222-222222222222', 'red'),
                (3, '00000000-0000-0000-0000-000000000000', 'green'),
                (4, '11111111-1111-1111-1111-111111111111', 'green');
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
    public async Task Guid_equality_filter_returns_matching_rows()
    {
        var ids = (await _ctx.Query<GdRow>().Where(r => r.Token == IdA).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 4 }, ids);
    }

    [Fact]
    public async Task Guid_Empty_in_where_resolves_to_zero_guid()
    {
        var ids = (await _ctx.Query<GdRow>().Where(r => r.Token == Guid.Empty).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3 }, ids);
    }

    [Fact]
    public async Task Distinct_after_column_projection_deduplicates_server_side()
    {
        var tags = (await _ctx.Query<GdRow>()
            .Select(r => new { r.Tag })
            .Distinct()
            .ToListAsync())
            .Select(r => r.Tag).OrderBy(t => t).ToArray();
        Assert.Equal(new[] { "green", "red" }, tags);
    }

    [Fact]
    public async Task Distinct_on_scalar_column_projection_returns_unique_values()
    {
        var tags = (await _ctx.Query<GdRow>()
            .Select(r => new { Color = r.Tag })
            .Distinct()
            .ToListAsync())
            .Select(x => x.Color).OrderBy(t => t).ToArray();
        Assert.Equal(2, tags.Length);
    }

    [Table("GdRow")]
    public sealed class GdRow
    {
        [Key] public int Id { get; set; }
        public Guid Token { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
