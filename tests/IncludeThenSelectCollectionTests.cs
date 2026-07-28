using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// `Include(a => a.Books).Select(a => new { a.Id, a.Books })` must return the projection (EF ignores the
/// Include once the query projects to a non-entity shape, loading the collection via the projection). nORM
/// never cleared the leftover Include plan when a projection changed the result shape, so the Include ran
/// against anonymous rows and threw InvalidCastException.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IncludeThenSelectCollectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE ItsAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE ItsBook (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);" +
            "INSERT INTO ItsAuthor VALUES (1,'Ada');" +
            "INSERT INTO ItsBook VALUES (10,1,'B1'),(11,1,'B2');";
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ItsAuthor>().HasKey(a => a.Id);
                mb.Entity<ItsBook>().HasKey(b => b.Id);
                mb.Entity<ItsAuthor>().HasMany(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId, a => a.Id);
            }
        });
    }

    public async Task DisposeAsync() { _ctx.Dispose(); await _cn.DisposeAsync(); }

    [Fact]
    public async Task Include_then_select_projecting_collection_returns_projection()
    {
        var rows = await ((INormQueryable<ItsAuthor>)_ctx.Query<ItsAuthor>())
            .Include(a => a.Books)
            .Where(a => a.Id == 1)
            .Select(a => new { a.Id, a.Name, a.Books })
            .ToListAsync();
        var row = Assert.Single(rows);
        Assert.Equal(1, row.Id);
        Assert.Equal(2, row.Books.Count);
    }

    [Table("ItsAuthor")]
    public sealed class ItsAuthor { [Key] public int Id { get; set; } public string Name { get; set; } = ""; public List<ItsBook> Books { get; set; } = new(); }
    [Table("ItsBook")]
    public sealed class ItsBook { [Key] public int Id { get; set; } public int AuthorId { get; set; } public string Title { get; set; } = ""; }
}
