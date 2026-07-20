using System.Collections.Generic;
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
/// A cascade delete must only follow the relationship it is cascading. A dependent that is
/// linked to the deleted principal ONLY through a different, non-cascading navigation to the
/// same principal CLR type must survive. Regression for a silent-data-loss vector where the
/// cascade nav-fallback matched a principal by type alone.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CascadeSecondNavigationDataLossTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("Cs2_Users")]
    public class Cs2User
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<Cs2Doc> AuthoredDocs { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("Cs2_Docs")]
    public class Cs2Doc
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public int EditorId { get; set; }
        public string Title { get; set; } = string.Empty;
        public Cs2User? Author { get; set; }
        public Cs2User? Editor { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // No FK constraints: isolate nORM's cascade decision from DB-level enforcement.
            cmd.CommandText = """
                CREATE TABLE Cs2_Users (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE Cs2_Docs (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, EditorId INTEGER NOT NULL, Title TEXT NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Cs2User>().HasKey(u => u.Id);
                mb.Entity<Cs2Doc>().HasKey(d => d.Id);
                // Only the Author side cascades (User.AuthoredDocs, FK AuthorId).
                mb.Entity<Cs2User>().HasMany(u => u.AuthoredDocs).WithOne()
                                    .HasForeignKey(d => d.AuthorId, u => u.Id);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), options));
    }

    [Fact]
    public async Task Deleting_a_principal_does_not_cascade_via_an_unrelated_second_navigation_to_the_same_type()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var alice = new Cs2User { Id = 1, Name = "Alice" };
        var bob = new Cs2User { Id = 2, Name = "Bob" };
        // Document authored by Alice (survivor), only last-edited by Bob (deleted).
        var doc = new Cs2Doc { Id = 10, AuthorId = 1, EditorId = 2, Title = "D", Author = alice, Editor = bob };
        ctx.Add(alice);
        ctx.Add(bob);
        ctx.Add(doc);
        await ctx.SaveChangesAsync();
        Assert.Single(await ctx.Query<Cs2Doc>().ToListAsync());

        // doc.Editor is the tracked Bob instance; doc.Author is the tracked Alice instance.
        ctx.Remove(bob);
        await ctx.SaveChangesAsync();

        var docs = await ctx.Query<Cs2Doc>().ToListAsync();
        var users = (await ctx.Query<Cs2User>().ToListAsync()).Select(u => u.Id).ToList();

        // Bob is gone; Alice survives.
        Assert.Equal(new[] { 1 }, users.OrderBy(i => i).ToArray());
        // The document authored by the SURVIVING Alice must NOT be cascade-deleted just because
        // its non-cascading Editor navigation pointed at the deleted Bob.
        Assert.Single(docs);
        Assert.Equal(10, docs[0].Id);
    }
}
