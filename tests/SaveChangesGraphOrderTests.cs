using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that SaveChanges respects FK graph ordering: principals are inserted before
/// dependents, and dependents are deleted before principals, regardless of the order
/// in which entities were added to the tracker.
///
/// This relies on the topological sort in DbContext (CT-1 fix). Additional ordering
/// edge cases (namespace collisions, circular FK detection) are in
/// <see cref="FkOrderingTests"/> and <see cref="FkOrderingAdvancedTests"/>.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SaveChangesGraphOrderTests
{
    // ── Schema helpers ────────────────────────────────────────────────────────

    [Table("GraphAuthor")]
    private class GraphAuthor
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("GraphPost")]
    private class GraphPost
    {
        [Key]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public int GraphAuthorId { get; set; }  // auto-detected FK → GraphAuthor
    }

    [Table("GraphComment")]
    private class GraphComment
    {
        [Key]
        public int Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public int GraphPostId { get; set; }   // auto-detected FK → GraphPost
    }

    private static async Task<(SqliteConnection Cn, DbContext Ctx)> CreateContextAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        await using (var pragma = cn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            await pragma.ExecuteNonQueryAsync();
        }
        await using (var ddl = cn.CreateCommand())
        {
            ddl.CommandText =
                "CREATE TABLE GraphAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE GraphPost   (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, GraphAuthorId INTEGER NOT NULL REFERENCES GraphAuthor(Id));" +
                "CREATE TABLE GraphComment(Id INTEGER PRIMARY KEY, Body  TEXT NOT NULL, GraphPostId   INTEGER NOT NULL REFERENCES GraphPost(Id));";
            await ddl.ExecuteNonQueryAsync();
        }

        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── Inserts ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_PrincipalAndDependent_AddedInReverseOrder_PrincipalGoesFirst()
    {
        // Add dependent before principal — FK constraint must not fire.
        var (cn, ctx) = await CreateContextAsync();
        await using var _cn = cn;
        await using var _ctx = ctx;

        var author = new GraphAuthor { Id = 1, Name = "Alice" };
        var post   = new GraphPost   { Id = 1, Title = "Hello World", GraphAuthorId = 1 };

        // Intentionally add dependent first
        ctx.Add(post);
        ctx.Add(author);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(2, affected);
    }

    [Fact]
    public async Task Insert_ThreeLevelHierarchy_AllInsertedWithoutFkViolation()
    {
        // author → post → comment: add all three in leaf-first order.
        var (cn, ctx) = await CreateContextAsync();
        await using var _cn = cn;
        await using var _ctx = ctx;

        var author  = new GraphAuthor  { Id = 1, Name = "Bob" };
        var post    = new GraphPost    { Id = 1, Title = "Deep", GraphAuthorId = 1 };
        var comment = new GraphComment { Id = 1, Body  = "Nice", GraphPostId   = 1 };

        // Worst case: deepest leaf first
        ctx.Add(comment);
        ctx.Add(post);
        ctx.Add(author);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(3, affected);
    }

    [Fact]
    public async Task Insert_MultipleChildren_AllInsertedAfterParent()
    {
        var (cn, ctx) = await CreateContextAsync();
        await using var _cn = cn;
        await using var _ctx = ctx;

        var author = new GraphAuthor { Id = 1, Name = "Carol" };
        var post1  = new GraphPost   { Id = 1, Title = "Post One", GraphAuthorId = 1 };
        var post2  = new GraphPost   { Id = 2, Title = "Post Two", GraphAuthorId = 1 };

        ctx.Add(post2);
        ctx.Add(post1);
        ctx.Add(author);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(3, affected);
    }

    // ── Deletes ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Delete_PrincipalAndDependent_DependentDeletedFirst()
    {
        var (cn, ctx) = await CreateContextAsync();
        await using var _cn = cn;
        await using var _ctx = ctx;

        // Seed
        await using (var ins = cn.CreateCommand())
        {
            ins.CommandText =
                "INSERT INTO GraphAuthor VALUES (1,'Dave');" +
                "INSERT INTO GraphPost   VALUES (1,'My Post',1);";
            await ins.ExecuteNonQueryAsync();
        }

        var author = new GraphAuthor { Id = 1, Name = "Dave" };
        var post   = new GraphPost   { Id = 1, Title = "My Post", GraphAuthorId = 1 };

        // Mark principal for deletion first
        ctx.Remove(author);
        ctx.Remove(post);

        var affected = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(2, affected);
    }

    [Fact]
    public async Task Delete_ThreeLevelHierarchy_LeafDeletedBeforeRoot()
    {
        var (cn, ctx) = await CreateContextAsync();
        await using var _cn = cn;
        await using var _ctx = ctx;

        await using (var ins = cn.CreateCommand())
        {
            ins.CommandText =
                "INSERT INTO GraphAuthor  VALUES (1,'Eve');" +
                "INSERT INTO GraphPost    VALUES (1,'A Post',1);" +
                "INSERT INTO GraphComment VALUES (1,'A Comment',1);";
            await ins.ExecuteNonQueryAsync();
        }

        var author  = new GraphAuthor  { Id = 1, Name = "Eve" };
        var post    = new GraphPost    { Id = 1, Title = "A Post", GraphAuthorId = 1 };
        var comment = new GraphComment { Id = 1, Body  = "A Comment", GraphPostId = 1 };

        // Remove root-first (worst case for FK ordering)
        ctx.Remove(author);
        ctx.Remove(post);
        ctx.Remove(comment);

        var affected = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(3, affected);
    }
}
