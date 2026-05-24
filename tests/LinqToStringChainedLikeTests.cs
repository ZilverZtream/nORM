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
/// Pins <c>x.IntCol.ToString().StartsWith("prefix")</c> and the
/// <c>EndsWith</c> / <c>Contains</c> companions. Verifies that the
/// per-provider ToString CAST from ebb6f89 composes with the existing
/// LIKE-emitting StartsWith / EndsWith / Contains handlers — the user
/// shape "filter rows whose id begins with N" should run server-side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqToStringChainedLikeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TscRow (Id INTEGER PRIMARY KEY, Quantity INTEGER NOT NULL);
            INSERT INTO TscRow VALUES
                (1, 5),
                (2, 12),
                (3, 123),
                (4, 1000),
                (5, 251);
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
    public async Task ToString_then_starts_with_filters_by_textual_prefix()
    {
        // Quantity values 12, 123, 1000 all start with "1" as text. Row Ids: 2, 3, 4 (5=251 starts with '2').
        var ids = (await _ctx.Query<TscRow>()
            .Where(r => r.Quantity.ToString().StartsWith("1"))
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 2, 3, 4 }, ids);
    }

    [Fact]
    public async Task ToString_then_ends_with_filters_by_textual_suffix()
    {
        // "5" suffix matches 5 (row 1). Nothing else ends in "5".
        var ids = (await _ctx.Query<TscRow>()
            .Where(r => r.Quantity.ToString().EndsWith("5"))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    [Fact]
    public async Task ToString_then_contains_filters_by_textual_substring()
    {
        // "23" substring matches 123 (row 3) and 251 — wait 251 doesn't contain "23" either,
        // it has "25". Let me recheck. 12, 123, 1000, 5, 251. "23" → only 123.
        var ids = (await _ctx.Query<TscRow>()
            .Where(r => r.Quantity.ToString().Contains("23"))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    [Table("TscRow")]
    public sealed class TscRow
    {
        [Key] public int Id { get; set; }
        public int Quantity { get; set; }
    }
}
