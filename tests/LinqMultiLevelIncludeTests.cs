using System;
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

namespace nORM.Tests;

/// <summary>
/// Verifies the multi-level eager-loading chain Include().ThenInclude() reaches a third-level
/// navigation and materializes the nested collections correctly. The path must register the
/// chain as separate dependent queries (split query) so each level fetches only its own rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMultiLevelIncludeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE MlAuthor  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE MlBook    (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);
            CREATE TABLE MlChapter (Id INTEGER PRIMARY KEY, BookId INTEGER NOT NULL, Heading TEXT NOT NULL);
            INSERT INTO MlAuthor VALUES (1,'Ada'),(2,'Grace');
            INSERT INTO MlBook   VALUES (10,1,'B1'),(11,1,'B2'),(20,2,'B3');
            INSERT INTO MlChapter VALUES (100,10,'C1a'),(101,10,'C1b'),(110,11,'C2a'),(200,20,'C3a');
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<MlAuthor>().HasKey(a => a.Id);
                mb.Entity<MlBook>().HasKey(b => b.Id);
                mb.Entity<MlChapter>().HasKey(c => c.Id);
                mb.Entity<MlAuthor>().HasMany(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId, a => a.Id);
                mb.Entity<MlBook>().HasMany(b => b.Chapters).WithOne().HasForeignKey(c => c.BookId, b => b.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Include_then_ThenInclude_loads_nested_collections()
    {
        var authors = (await ((INormQueryable<MlAuthor>)_ctx.Query<MlAuthor>())
            .Include(a => a.Books)
                .ThenInclude(b => b.Chapters)
            .AsSplitQuery()
            .OrderBy(a => a.Id)
            .ToListAsync())
            .ToArray();

        Assert.Equal(2, authors.Length);

        var ada = authors.First(a => a.Name == "Ada");
        Assert.Equal(2, ada.Books.Count);
        var b1 = ada.Books.First(b => b.Title == "B1");
        Assert.Equal(2, b1.Chapters.Count);
        var b2 = ada.Books.First(b => b.Title == "B2");
        Assert.Single(b2.Chapters);

        var grace = authors.First(a => a.Name == "Grace");
        Assert.Single(grace.Books);
        Assert.Single(grace.Books[0].Chapters);
    }

    [Table("MlAuthor")]
    public sealed class MlAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<MlBook> Books { get; set; } = new();
    }

    [Table("MlBook")]
    public sealed class MlBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = string.Empty;
        public List<MlChapter> Chapters { get; set; } = new();
    }

    [Table("MlChapter")]
    public sealed class MlChapter
    {
        [Key] public int Id { get; set; }
        public int BookId { get; set; }
        public string Heading { get; set; } = string.Empty;
    }
}
