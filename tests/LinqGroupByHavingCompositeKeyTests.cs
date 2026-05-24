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
/// the bare-<c>g.Key</c> projection of a COMPOSITE group key isn't supported because
/// SQL has no way to materialise a single column into a nested anonymous type — the
/// SELECT would have to emit multiple columns (one per composite member) and the
/// outer materialiser would have to reconstruct the composite from them, which nORM
/// doesn't yet plumb. Pre-fix, the SELECT was emitting all member columns aliased as
/// the last one (`"T0"."Region", "T0"."Type" AS "Key"`), which produced an opaque
/// `InvalidCastException: Unable to cast object of type 'System.String' to type
/// '&lt;&gt;f__AnonymousType…'` deep inside the materialiser. Surface a clear exception
/// with the supported flattened workaround.
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
    public async Task Bare_composite_key_projection_throws_with_flatten_workaround()
    {
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await _ctx.Query<GhcSale>()
                .GroupBy(s => new { s.Region, s.Type })
                .Select(g => new { g.Key, Total = g.Sum(s => s.Amount) })
                .ToListAsync();
        });
        Assert.Contains("composite", ex.Message, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Key", ex.Message, System.StringComparison.OrdinalIgnoreCase);
        // Workaround must mention member-by-member flattening so the caller knows what to do.
        Assert.Contains("g.Key.", ex.Message, System.StringComparison.OrdinalIgnoreCase);
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

    [Table("GhcSale")]
    public sealed class GhcSale
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public double Amount { get; set; }
    }
}
