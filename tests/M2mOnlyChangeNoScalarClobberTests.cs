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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A change to ONLY a many-to-many collection must not rewrite the principal row's scalar columns.
/// Change detection marks the owner Modified for an association edit, but no scalar column changed; emitting
/// a full-column principal UPDATE rewrites every scalar with the loaded (possibly stale) values, silently
/// clobbering a concurrent writer's scalar change (a lost update) — EF Core issues no principal UPDATE for a
/// skip-navigation-only change. The join-table sync applies the association change on its own.
/// </summary>
[Trait("Category", "Fast")]
public class M2mOnlyChangeNoScalarClobberTests
{
    [Table("M2mLu_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mLu_Tag")]
    public class Tag { [Key] public int Id { get; set; } }

    private static DbContext Ctx(string connStr)
    {
        var cn = new SqliteConnection(connStr);
        cn.Open();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mLu_PostTag", "PostId", "TagId")
        });
    }

    [Fact]
    public async Task M2m_only_change_does_not_clobber_a_concurrent_scalar_update()
    {
        var keeper = new SqliteConnection($"Data Source=file:m2mlu_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using var _ = keeper;
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE M2mLu_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);" +
                "CREATE TABLE M2mLu_Tag (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE M2mLu_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO M2mLu_Post VALUES (1, 'orig');" +
                "INSERT INTO M2mLu_Tag VALUES (1),(2),(3);" +
                "INSERT INTO M2mLu_PostTag VALUES (1, 1);";
            cmd.ExecuteNonQuery();
        }

        // Writer 1 loads the post (Title = "orig") and its tags.
        await using var ctx1 = Ctx(keeper.ConnectionString);
        var post1 = ((INormQueryable<Post>)ctx1.Query<Post>()).Include(p => p.Tags).ToList().Single();

        // Writer 2 changes ONLY the scalar Title and commits.
        await using (var ctx2 = Ctx(keeper.ConnectionString))
        {
            var post2 = ctx2.Query<Post>().ToList().Single();
            post2.Title = "updated";
            await ctx2.SaveChangesAsync();
        }

        // Writer 1 changes ONLY the M2M collection (links Tag 2) and commits.
        post1.Tags.Add(new Tag { Id = 2 });
        await ctx1.SaveChangesAsync();

        // The M2M change must have applied, and writer 1 must NOT have rewritten Title back to its stale "orig".
        await using var verify = Ctx(keeper.ConnectionString);
        var final = ((INormQueryable<Post>)verify.Query<Post>()).Include(p => p.Tags).AsNoTracking().ToList().Single();
        Assert.Equal(new[] { 1, 2 }, final.Tags.Select(t => t.Id).OrderBy(i => i).ToArray()); // association applied
        Assert.Equal("updated", final.Title);                                                 // scalar NOT clobbered
    }
}
