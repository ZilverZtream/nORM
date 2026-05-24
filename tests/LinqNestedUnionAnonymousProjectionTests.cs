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
/// Sister of <see cref="LinqUnionAnonymousProjectionTests"/> for the 3-source
/// nested-Union shape <c>A.Union(B).Union(C)</c>. The 81da1d0 fix walks the left
/// source's call chain past projection-preserving methods to find the trailing
/// Select; for nested Union, the lookup must walk past the inner Union to the
/// leftmost source's projection. If the walk stops at the inner Union, the outer
/// materialiser sees null projection and crashes on
/// `No suitable constructor for <>f__AnonymousType…`.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNestedUnionAnonymousProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NuaA (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Year INTEGER NOT NULL);
            CREATE TABLE NuaB (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Founded INTEGER NOT NULL);
            CREATE TABLE NuaC (Id INTEGER PRIMARY KEY, Name  TEXT NOT NULL, Born    INTEGER NOT NULL);
            INSERT INTO NuaA VALUES (1,'Alpha',2001);
            INSERT INTO NuaB VALUES (1,'Bravo',2002);
            INSERT INTO NuaC VALUES (1,'Charlie',2003);
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
    public async Task Three_way_nested_union_with_anonymous_projection_materialises_all_sources()
    {
        var rows = (await _ctx.Query<NuaA>()
            .Select(a => new { Name = a.Label, Year = a.Year })
            .Union(_ctx.Query<NuaB>().Select(b => new { Name = b.Title, Year = b.Founded }))
            .Union(_ctx.Query<NuaC>().Select(c => new { Name = c.Name,  Year = c.Born }))
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Alpha",   rows[0].Name); Assert.Equal(2001, rows[0].Year);
        Assert.Equal("Bravo",   rows[1].Name); Assert.Equal(2002, rows[1].Year);
        Assert.Equal("Charlie", rows[2].Name); Assert.Equal(2003, rows[2].Year);
    }

    [Table("NuaA")] public sealed class NuaA { [Key] public int Id { get; set; } public string Label { get; set; } = ""; public int Year    { get; set; } }
    [Table("NuaB")] public sealed class NuaB { [Key] public int Id { get; set; } public string Title { get; set; } = ""; public int Founded { get; set; } }
    [Table("NuaC")] public sealed class NuaC { [Key] public int Id { get; set; } public string Name  { get; set; } = ""; public int Born    { get; set; } }
}
