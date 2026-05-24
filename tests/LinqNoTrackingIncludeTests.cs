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
/// Verifies AsNoTracking() composes correctly with Include + AsSplitQuery: the parent + child
/// graph still materializes, but the entities are not pinned in the ChangeTracker. Catches a
/// regression where the no-tracking flag would silently disable the dependent-query population.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNoTrackingIncludeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NtAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NtBook   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);
            INSERT INTO NtAuthor VALUES (1,'Ada'),(2,'Grace');
            INSERT INTO NtBook   VALUES (10,1,'A1'),(11,1,'A2'),(20,2,'B1');
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NtAuthor>().HasKey(a => a.Id);
                mb.Entity<NtBook>().HasKey(b => b.Id);
                mb.Entity<NtAuthor>().HasMany(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId, a => a.Id);
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
    public async Task AsNoTracking_with_Include_split_query_loads_children_and_skips_tracker()
    {
        var trackerCountBefore = Enumerable.Count(_ctx.ChangeTracker.Entries);

        var authors = (await ((INormQueryable<NtAuthor>)_ctx.Query<NtAuthor>())
            .Include(a => a.Books)
            .AsSplitQuery()
            .AsNoTracking()
            .OrderBy(a => a.Id)
            .ToListAsync())
            .ToArray();

        Assert.Equal(2, authors.Length);
        Assert.Equal(2, authors.First(a => a.Name == "Ada").Books.Count);
        Assert.Single(authors.First(a => a.Name == "Grace").Books);

        // AsNoTracking must keep the tracker out of the picture.
        Assert.Equal(trackerCountBefore, Enumerable.Count(_ctx.ChangeTracker.Entries));
    }

    [Table("NtAuthor")]
    public sealed class NtAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NtBook> Books { get; set; } = new();
    }

    [Table("NtBook")]
    public sealed class NtBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = string.Empty;
    }
}
