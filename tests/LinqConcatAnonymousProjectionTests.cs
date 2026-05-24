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
/// Sister of <see cref="LinqUnionAnonymousProjectionTests"/> for Concat
/// (UNION ALL — preserves duplicates instead of de-duplicating like Union).
/// The 81da1d0 projection-lift fix is generic over the set-operation family,
/// so this should work end-to-end — pinning to catch regressions that
/// special-case Union vs Concat.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqConcatAnonymousProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CapA (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Year INTEGER NOT NULL);
            CREATE TABLE CapB (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Founded INTEGER NOT NULL);
            -- Identical (Label, Year) pair across both tables so we can verify Concat keeps the duplicate.
            INSERT INTO CapA VALUES (1,'Acme',2001),(2,'Globex',2002);
            INSERT INTO CapB VALUES (1,'Acme',2001),(2,'Initech',2003);
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
    public async Task Concat_with_anonymous_projection_preserves_duplicate_rows_unlike_union()
    {
        // Concat (UNION ALL) returns ALL 4 rows including the duplicate Acme/2001.
        // (Union would collapse to 3.)
        var rows = (await _ctx.Query<CapA>()
            .Select(a => new { Name = a.Label, Year = a.Year })
            .Concat(_ctx.Query<CapB>().Select(b => new { Name = b.Title, Year = b.Founded }))
            .ToListAsync())
            .OrderBy(r => r.Name).ThenBy(r => r.Year)
            .ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal("Acme",    rows[0].Name); Assert.Equal(2001, rows[0].Year);
        Assert.Equal("Acme",    rows[1].Name); Assert.Equal(2001, rows[1].Year); // duplicate preserved
        Assert.Equal("Globex",  rows[2].Name); Assert.Equal(2002, rows[2].Year);
        Assert.Equal("Initech", rows[3].Name); Assert.Equal(2003, rows[3].Year);
    }

    [Table("CapA")] public sealed class CapA { [Key] public int Id { get; set; } public string Label { get; set; } = ""; public int Year    { get; set; } }
    [Table("CapB")] public sealed class CapB { [Key] public int Id { get; set; } public string Title { get; set; } = ""; public int Founded { get; set; } }
}
