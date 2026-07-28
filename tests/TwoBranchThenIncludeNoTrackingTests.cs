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
/// Two ThenInclude branches off the SAME navigation — `Include(a=>a.Nav).ThenInclude(x).Include(a=>a.Nav)
/// .ThenInclude(y)` — must populate BOTH deeper collections/references, matching EF Core's merged include
/// tree. Under no-tracking nORM built two independent linear include plans; the second re-materialized the
/// shared navigation into fresh instances (no identity map) and overwrote the parent's nav, silently dropping
/// the first branch's grandchildren.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TwoBranchThenIncludeNoTrackingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE TbAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE TbBook (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);" +
            "CREATE TABLE TbChapter (Id INTEGER PRIMARY KEY, BookId INTEGER NOT NULL, Heading TEXT NOT NULL);" +
            "CREATE TABLE TbPage (Id INTEGER PRIMARY KEY, BookId INTEGER NOT NULL, Num INTEGER NOT NULL);" +
            "INSERT INTO TbAuthor VALUES (1,'Ada');" +
            "INSERT INTO TbBook VALUES (10,1,'B1');" +
            "INSERT INTO TbChapter VALUES (100,10,'C1'),(101,10,'C2');" +
            "INSERT INTO TbPage VALUES (200,10,1),(201,10,2),(202,10,3);";
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TbAuthor>().HasKey(a => a.Id);
                mb.Entity<TbBook>().HasKey(b => b.Id);
                mb.Entity<TbChapter>().HasKey(c => c.Id);
                mb.Entity<TbPage>().HasKey(p => p.Id);
                mb.Entity<TbAuthor>().HasMany(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId, a => a.Id);
                mb.Entity<TbBook>().HasMany(b => b.Chapters).WithOne().HasForeignKey(c => c.BookId, b => b.Id);
                mb.Entity<TbBook>().HasMany(b => b.Pages).WithOne().HasForeignKey(p => p.BookId, b => b.Id);
            }
        });
    }

    public async Task DisposeAsync() { _ctx.Dispose(); await _cn.DisposeAsync(); }

    [Fact]
    public async Task Two_thenInclude_branches_no_tracking_both_populated()
    {
        var authors = await ((INormQueryable<TbAuthor>)_ctx.Query<TbAuthor>())
            .Include(a => a.Books).ThenInclude(b => b.Chapters)
            .Include(a => a.Books).ThenInclude(b => b.Pages)
            .AsNoTracking()
            .ToListAsync();

        var book = Assert.Single(Assert.Single(authors).Books);
        Assert.Equal(2, book.Chapters.Count);
        Assert.Equal(3, book.Pages.Count);
    }

    [Table("TbAuthor")]
    public sealed class TbAuthor { [Key] public int Id { get; set; } public string Name { get; set; } = ""; public List<TbBook> Books { get; set; } = new(); }
    [Table("TbBook")]
    public sealed class TbBook { [Key] public int Id { get; set; } public int AuthorId { get; set; } public string Title { get; set; } = ""; public List<TbChapter> Chapters { get; set; } = new(); public List<TbPage> Pages { get; set; } = new(); }
    [Table("TbChapter")]
    public sealed class TbChapter { [Key] public int Id { get; set; } public int BookId { get; set; } public string Heading { get; set; } = ""; }
    [Table("TbPage")]
    public sealed class TbPage { [Key] public int Id { get; set; } public int BookId { get; set; } public int Num { get; set; } }
}
