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
/// Sister of <see cref="LinqUnionAnonymousProjectionTests"/> for the remaining
/// two set operations: Intersect (rows present in BOTH) and Except (rows
/// present in left but NOT right). The 81da1d0 projection-lift fix is generic
/// over the set-operation family — pin both shapes so a future special-case
/// regression is caught.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqIntersectExceptAnonymousProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IeaA (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Year INTEGER NOT NULL);
            CREATE TABLE IeaB (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Founded INTEGER NOT NULL);
            -- A: {Acme/2001, Globex/2002, OnlyInA/2099}
            -- B: {Acme/2001, Initech/2003, Globex/2002}
            -- Intersect = {Acme/2001, Globex/2002}; A.Except(B) = {OnlyInA/2099}.
            INSERT INTO IeaA VALUES (1,'Acme',2001),(2,'Globex',2002),(3,'OnlyInA',2099);
            INSERT INTO IeaB VALUES (1,'Acme',2001),(2,'Initech',2003),(3,'Globex',2002);
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
    public async Task Intersect_with_anonymous_projection_returns_rows_present_in_both_sides()
    {
        var rows = (await _ctx.Query<IeaA>()
            .Select(a => new { Name = a.Label, Year = a.Year })
            .Intersect(_ctx.Query<IeaB>().Select(b => new { Name = b.Title, Year = b.Founded }))
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Acme",   rows[0].Name); Assert.Equal(2001, rows[0].Year);
        Assert.Equal("Globex", rows[1].Name); Assert.Equal(2002, rows[1].Year);
    }

    [Fact]
    public async Task Except_with_anonymous_projection_returns_rows_only_on_left_side()
    {
        var rows = (await _ctx.Query<IeaA>()
            .Select(a => new { Name = a.Label, Year = a.Year })
            .Except(_ctx.Query<IeaB>().Select(b => new { Name = b.Title, Year = b.Founded }))
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal("OnlyInA", rows[0].Name); Assert.Equal(2099, rows[0].Year);
    }

    [Table("IeaA")] public sealed class IeaA { [Key] public int Id { get; set; } public string Label { get; set; } = ""; public int Year    { get; set; } }
    [Table("IeaB")] public sealed class IeaB { [Key] public int Id { get; set; } public string Title { get; set; } = ""; public int Founded { get; set; } }
}
