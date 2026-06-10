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
/// Pins <c>GroupBy(s => new {…composite…}).Select(g => new {g.Key, Total=g.Sum(...)})</c>:
/// the bare-<c>g.Key</c> projection of a COMPOSITE group key emits one SELECT column
/// per composite member and reconstructs the nested anonymous type at materialisation
/// time. Pre-implementation, the translator threw because the SELECT would emit member
/// columns aliased only as the last one and the materialiser couldn't fold the row
/// back into the nested anon type; the implementation emits prefixed aliases
/// (<c>Key__Region</c>, <c>Key__Type</c>) and a nested-construction materialiser.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByHavingCompositeKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GhcSale (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Type TEXT NOT NULL, Amount REAL NOT NULL);
            INSERT INTO GhcSale VALUES
                (1, 'EU', 'Book', 10.0),
                (2, 'EU', 'Book', 20.0),
                (3, 'EU', 'Game',  5.0),
                (4, 'US', 'Book', 50.0),
                (5, 'US', 'Game', 30.0);
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
    public async Task Bare_composite_key_projection_emits_per_member_columns_and_rebuilds_nested_anon()
    {
        var rows = (await _ctx.Query<GhcSale>()
            .GroupBy(s => new { s.Region, s.Type })
            .Select(g => new { g.Key, Total = g.Sum(s => s.Amount) })
            .ToListAsync())
            .OrderBy(r => r.Key.Region).ThenBy(r => r.Key.Type)
            .ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Equal(("EU", "Book", 30.0), (rows[0].Key.Region, rows[0].Key.Type, rows[0].Total));
        Assert.Equal(("EU", "Game",  5.0), (rows[1].Key.Region, rows[1].Key.Type, rows[1].Total));
        Assert.Equal(("US", "Book", 50.0), (rows[2].Key.Region, rows[2].Key.Type, rows[2].Total));
        Assert.Equal(("US", "Game", 30.0), (rows[3].Key.Region, rows[3].Key.Type, rows[3].Total));
    }

    [Fact]
    public async Task Flattened_composite_key_projection_with_where_on_key_member_filters_correctly()
    {
        // The supported alternative to the bare-Key projection: lift each composite member
        // to its own top-level column. This is the shape the pin recommends, and exercises
        // the WHERE-on-key-column path through HAVING (regression coverage for e72ca37).
        var rows = (await _ctx.Query<GhcSale>()
            .GroupBy(s => new { s.Region, s.Type })
            .Select(g => new { g.Key.Region, g.Key.Type, Total = g.Sum(s => s.Amount) })
            .Where(x => x.Region == "EU")
            .ToListAsync())
            .OrderBy(r => r.Type)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("EU",   rows[0].Region); Assert.Equal("Book", rows[0].Type); Assert.Equal(30.0, rows[0].Total);
        Assert.Equal("EU",   rows[1].Region); Assert.Equal("Game", rows[1].Type); Assert.Equal(5.0,  rows[1].Total);
    }

    [Fact]
    public async Task Composite_key_having_before_flattened_key_projection_translates_key_members()
    {
        var rows = (await _ctx.Query<GhcSale>()
            .GroupBy(s => new { s.Region, s.Type })
            .Where(g => g.Count() >= 2)
            .Select(g => new { g.Key.Region, g.Key.Type, Count = g.Count() })
            .ToListAsync())
            .OrderBy(r => r.Region).ThenBy(r => r.Type)
            .ToArray();

        var row = Assert.Single(rows);
        Assert.Equal(("EU", "Book", 2), (row.Region, row.Type, row.Count));
    }

    [Table("GhcSale")]
    public sealed class GhcSale
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
