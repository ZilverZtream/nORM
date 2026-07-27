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
/// A query that eager-loads a nested collection (a collection projection / split query, or a many-to-many
/// navigation) cannot stream row-by-row, because the collection is filled by a dependent fetch that runs
/// only after the full root set is materialized. The streaming guard only rejected plan.Includes, so a plan
/// carrying DependentQueries (collection projection) or M2MIncludes silently fell through to the row-by-row
/// loop and yielded the roots with their nested collections EMPTY — silent data loss on a query that
/// "succeeds". The guard must reject these shapes with an actionable message (like Include already did),
/// preserving AsAsyncEnumerable's bounded-memory streaming contract; ToListAsync remains the correct way to
/// get the fully-loaded set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsAsyncEnumerableCollectionProjectionTests
{
    [Table("AaeAuthor")]
    public class Author
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Book> Books { get; set; } = new();
    }

    [Table("AaeBook")]
    public class Book
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = "";
    }

    private static DbContext Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AaeAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE AaeBook (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);
                INSERT INTO AaeAuthor (Id, Name) VALUES (1, 'a'), (2, 'b');
                INSERT INTO AaeBook (Id, AuthorId, Title) VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'z');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Author>().HasKey(a => a.Id);
                mb.Entity<Book>().HasKey(b => b.Id);
                mb.Entity<Author>().HasMany(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId, a => a.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // ToListAsync populates the nested collection correctly — the actionable alternative the guard points to.
    [Fact]
    public async Task Collection_projection_ToListAsync_populates_nested_collections()
    {
        await using var ctx = Create();

        var byId = (await ctx.Query<Author>()
                .OrderBy(a => a.Id)
                .Select(a => new { a.Id, Titles = a.Books.OrderBy(b => b.Id).Select(b => b.Title).ToList() })
                .ToListAsync())
            .ToDictionary(r => r.Id, r => r.Titles);

        Assert.Equal(new[] { "x", "y" }, byId[1]);
        Assert.Equal(new[] { "z" }, byId[2]);
    }

    // A collection projection must NOT silently stream empty collections — it must fail loud (like Include),
    // because populating the collection needs the full root set (incompatible with row-by-row streaming).
    [Fact]
    public async Task Collection_projection_AsAsyncEnumerable_fails_loud_instead_of_dropping_data()
    {
        await using var ctx = Create();

        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await foreach (var _ in ctx.Query<Author>()
                .OrderBy(a => a.Id)
                .Select(a => new { a.Id, Titles = a.Books.OrderBy(b => b.Id).Select(b => b.Title).ToList() })
                .AsAsyncEnumerable())
            {
                /* must not silently yield rows with empty Titles */
            }
        });

        Assert.Contains("ToListAsync", ex.Message, System.StringComparison.OrdinalIgnoreCase);
    }
}
