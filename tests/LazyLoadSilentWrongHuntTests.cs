using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial hunt for SILENT-WRONG lazy-loading results on SQLite: a lazy navigation returning
/// wrong / stale / empty data, loading the wrong rows, leaking filtered rows, or cross-contaminating
/// between parents. Uses the PROXY access path (ICollection&lt;T&gt; + LazyNavigationReference&lt;T&gt;),
/// not the explicit Entry().Collection().Load() API, so it exercises the on-first-access load.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LazyLoadSilentWrongHuntTests
{
    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    [Table("LzhBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public decimal Rating { get; set; }
        public bool IsDeleted { get; set; }
        // No initializer => stays null at materialization => lazy proxy installed.
        public ICollection<Post> Posts { get; set; } = null!;
    }

    [Table("LzhPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int? BlogId { get; set; }
        public string Title { get; set; } = "";
        public bool IsDeleted { get; set; }
        public LazyNavigationReference<Blog>? Blog { get; set; }
    }

    private static SqliteConnection Seed(string extraBlogCols = "", string blogRows = "", string postRows = "")
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE LzhBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Rating TEXT NOT NULL, IsDeleted INTEGER NOT NULL" + extraBlogCols + ");" +
            "CREATE TABLE LzhPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NULL, Title TEXT NOT NULL, IsDeleted INTEGER NOT NULL);" +
            blogRows + postRows;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContextOptions BaseOpts()
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Blog>().HasKey(b => b.Id)
                .HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId!, b => b.Id)
        };
        return opts;
    }

    // ---------------------------------------------------------------------------------------------
    // 1. Lazy collection PROXY: correct children per parent, empty (not null) when none, and no
    //    cross-contamination between different parents (N+1 correctness).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Lazy_collection_proxy_loads_correct_children_no_cross_contamination()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0),(2,'b2','3.0',0),(3,'b3','1.0',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0),(2,1,'b',0),(3,2,'c',0);");
        using var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);

        var blogs = ctx.Query<Blog>().OrderBy(b => b.Id).ToList();
        Assert.Equal(3, blogs.Count);

        // Blog 1 -> posts {1,2}
        Assert.Equal(new[] { 1, 2 }, blogs[0].Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
        // Blog 2 -> post {3} only (NOT blog 1's children)
        Assert.Equal(new[] { 3 }, blogs[1].Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
        // Blog 3 -> EMPTY collection (not null, not another parent's rows)
        Assert.NotNull(blogs[2].Posts);
        Assert.Empty(blogs[2].Posts);
    }

    // ---------------------------------------------------------------------------------------------
    // 2. Lazy collection PROXY + soft-delete global filter: soft-deleted children must be EXCLUDED
    //    (a leak here is severe).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Lazy_collection_proxy_applies_soft_delete_filter()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0),(2,1,'b',1),(3,1,'c',0);"); // post 2 deleted
        var opts = BaseOpts();
        opts.AddGlobalFilter<Post>(p => !p.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var blog = ctx.Query<Blog>().First();
        Assert.Equal(new[] { 1, 3 }, blog.Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // 3. Lazy reference PROXY dependent->principal + soft-deleted principal: a filtered-out parent
    //    must read as null (missing), matching a direct filtered query — not the wrong/soft-deleted row.
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public async Task Lazy_reference_dependent_to_principal_respects_principal_soft_delete()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0),(2,'b2','3.0',1);", // blog 2 deleted
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0),(2,2,'b',0);");
        var opts = BaseOpts();
        opts.AddGlobalFilter<Blog>(b => !b.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var post1 = ctx.Query<Post>().First(p => p.Id == 1);
        var b1 = await post1.Blog!.GetValueAsync();
        Assert.NotNull(b1);
        Assert.Equal(1, b1!.Id);

        var post2 = ctx.Query<Post>().First(p => p.Id == 2);
        var b2 = await post2.Blog!.GetValueAsync();
        // blog 2 is soft-deleted -> must read as missing parent.
        Assert.Null(b2);
    }

    // ---------------------------------------------------------------------------------------------
    // 4. Lazy reference with NULL FK: no parent, must be null (not throw, not wrong parent).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public async Task Lazy_reference_null_fk_returns_null()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,NULL,'orphan',0);");
        using var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);

        var post = ctx.Query<Post>().First();
        var blog = await post.Blog!.GetValueAsync();
        Assert.Null(blog);
    }

    // ---------------------------------------------------------------------------------------------
    // 5. Lazy reference materializes decimal correctly (converter/decimal column on principal).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public async Task Lazy_reference_materializes_decimal_principal_correctly()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','12.75',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0);");
        using var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);

        var post = ctx.Query<Post>().First();
        var blog = await post.Blog!.GetValueAsync();
        Assert.NotNull(blog);
        Assert.Equal(12.75m, blog!.Rating);
    }

    // ---------------------------------------------------------------------------------------------
    // 6. Lazy reference dependent->principal identity map: an already-tracked principal carrying a
    //    PENDING change must be returned WITH the change (not overwritten to the DB value).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public async Task Lazy_reference_returns_tracked_principal_preserving_pending_change()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'original','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0);");
        using var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);

        var trackedBlog = ctx.Query<Blog>().First(b => b.Id == 1);
        trackedBlog.Name = "changed";   // pending, not saved

        var post = ctx.Query<Post>().First();
        var lazyBlog = await post.Blog!.GetValueAsync();
        Assert.NotNull(lazyBlog);
        Assert.Same(trackedBlog, lazyBlog);           // identity resolution
        Assert.Equal("changed", lazyBlog!.Name);      // pending change preserved
    }

    // ---------------------------------------------------------------------------------------------
    // 7. Lazy load caching: second access returns SAME data without re-query, and returns the same
    //    materialized child instances (identity stable).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Lazy_collection_second_access_is_cached_and_stable()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0),(2,1,'b',0);");
        using var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);

        var blog = ctx.Query<Blog>().First();
        var first = blog.Posts.OrderBy(p => p.Id).ToList();
        var second = blog.Posts.OrderBy(p => p.Id).ToList();
        Assert.Equal(2, first.Count);
        Assert.Same(first[0], second[0]);
        Assert.Same(first[1], second[1]);
    }

    // ---------------------------------------------------------------------------------------------
    // 8. Lazy load after DbContext disposal must throw a CLEAR exception, not silently return
    //    empty/null (silent-wrong).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Lazy_collection_after_dispose_must_not_silently_return_empty()
    {
        SqliteConnection cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0),(2,1,'b',0);");
        using var _cn = cn; // connection kept OPEN (ownsConnection:false) so the DB is reachable.
        Blog blog;
        var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);
        blog = ctx.Query<Blog>().First();
        ctx.Dispose();

        // After disposal, accessing an unloaded lazy collection must EITHER throw a clear exception
        // OR (if it still runs) return the CORRECT 2 rows. Silently returning an empty collection
        // while blog 1 has 2 posts is silent-wrong (data-loss).
        int count;
        try { count = blog.Posts.Count; }
        catch (Exception ex) { Assert.True(true, "threw: " + ex.GetType().Name); return; }
        Assert.Equal(2, count); // if it did not throw, it MUST be correct, not silently 0
    }

    [Fact]
    public async Task Lazy_reference_after_dispose_must_not_silently_return_null()
    {
        SqliteConnection cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0);");
        using var _cn = cn; // connection kept OPEN so the parent row is reachable.
        var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);
        var post = ctx.Query<Post>().First();
        ctx.Dispose();

        // After disposal, the lazy reference must EITHER throw OR return the CORRECT parent (Id 1).
        // Silently returning null while the parent row exists is silent-wrong.
        Blog? blog;
        try { blog = await post.Blog!.GetValueAsync(); }
        catch (Exception ex) { Assert.True(true, "threw: " + ex.GetType().Name); return; }
        Assert.NotNull(blog);
        Assert.Equal(1, blog!.Id);
    }

    // ---------------------------------------------------------------------------------------------
    // 9. Lazy collection + tenant boundary via PROXY: another tenant's child sharing the FK must
    //    not leak (severe).
    // ---------------------------------------------------------------------------------------------
    [Table("LzhTBlog")]
    public class TBlog
    {
        [Key] public int Id { get; set; }
        public string TenantKey { get; set; } = "";
        public ICollection<TPost> Posts { get; set; } = null!;
    }

    [Table("LzhTPost")]
    public class TPost
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string TenantKey { get; set; } = "";
    }

    [Fact]
    public void Lazy_collection_proxy_keeps_tenant_boundary()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE LzhTBlog (Id INTEGER PRIMARY KEY, TenantKey TEXT NOT NULL);" +
                "CREATE TABLE LzhTPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, TenantKey TEXT NOT NULL);" +
                "INSERT INTO LzhTBlog VALUES (1,'T1');" +
                "INSERT INTO LzhTPost VALUES (1,1,'T1'),(2,1,'T1'),(3,1,'T2');"; // post 3 = other tenant
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1"), TenantColumnName = "TenantKey" };
        opts.OnModelCreating = mb => mb.Entity<TBlog>().HasKey(b => b.Id)
            .HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var blog = ctx.Query<TBlog>().First();
        Assert.Equal(new[] { 1, 2 }, blog.Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // 10. Lazy-loaded collection write-back: adding a child to a lazy-loaded collection then
    //     SaveChanges must persist the new child (participates in change tracking).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public async Task Lazy_collection_add_then_save_persists_new_child()
    {
        using var cn = Seed(
            blogRows: "INSERT INTO LzhBlog VALUES (1,'b1','4.5',0);",
            postRows: "INSERT INTO LzhPost VALUES (1,1,'a',0);");
        using var ctx = new DbContext(cn, new SqliteProvider(), BaseOpts(), ownsConnection: false);

        var blog = ctx.Query<Blog>().First();
        _ = blog.Posts.Count; // force lazy load
        blog.Posts.Add(new Post { Id = 99, BlogId = 1, Title = "new" });
        await ctx.SaveChangesAsync();

        // Re-read the row count directly from the DB.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM LzhPost WHERE BlogId = 1;";
        var count = Convert.ToInt32(cmd.ExecuteScalar());
        Assert.Equal(2, count); // original + new
    }

    // ---------------------------------------------------------------------------------------------
    // 11. Lazy REFERENCE proxy principal->dependent (FK on the dependent) via convention: routes through
    //     the batched loader's FirstOrDefault + row-filter clause. A revoked (soft-deleted) dependent must
    //     read as null, a valid one must load correctly. Distinct path from dependent->principal.
    // ---------------------------------------------------------------------------------------------
    [Table("LzhPerson")]
    public class Person
    {
        [Key] public int Id { get; set; }
        public LazyNavigationReference<Passport>? Passport { get; set; }
    }

    [Table("LzhPassport")]
    public class Passport
    {
        [Key] public int Id { get; set; }
        public int PersonId { get; set; }
        public bool IsRevoked { get; set; }
    }

    [Fact]
    public async Task Lazy_reference_principal_to_dependent_respects_dependent_soft_delete()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE LzhPerson (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE LzhPassport (Id INTEGER PRIMARY KEY, PersonId INTEGER NOT NULL, IsRevoked INTEGER NOT NULL);" +
                "INSERT INTO LzhPerson VALUES (1),(2);" +
                "INSERT INTO LzhPassport VALUES (10,1,0),(20,2,1);"; // person 2's passport revoked
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Person>().HasKey(p => p.Id) };
        opts.AddGlobalFilter<Passport>(pp => !pp.IsRevoked);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var p1 = ctx.Query<Person>().First(p => p.Id == 1);
        var pass1 = await p1.Passport!.GetValueAsync();
        Assert.NotNull(pass1);
        Assert.Equal(10, pass1!.Id);

        var p2 = ctx.Query<Person>().First(p => p.Id == 2);
        var pass2 = await p2.Passport!.GetValueAsync();
        Assert.Null(pass2); // revoked dependent must not leak
    }

    // ---------------------------------------------------------------------------------------------
    // 12. Type-mismatched keys: principal PK is `long`, dependent FK is `int`. The batched loader groups
    //     children by their FK model value and looks them up by the parent's PK model value. If the box
    //     types differ (long vs int) the dictionary lookup silently misses -> EMPTY collection while the
    //     DB has children. Must EITHER coerce/match (correct rows) OR fail loud -- never silently empty.
    // ---------------------------------------------------------------------------------------------
    [Table("LzhBigBlog")]
    public class BigBlog
    {
        [Key] public long Id { get; set; }
        public ICollection<SmallPost> Posts { get; set; } = null!;
    }

    [Table("LzhSmallPost")]
    public class SmallPost
    {
        [Key] public int Id { get; set; }
        public int BigBlogId { get; set; } // int FK vs long PK
    }

    [Fact]
    public void Lazy_collection_mismatched_key_widths_must_not_silently_return_empty()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE LzhBigBlog (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE LzhSmallPost (Id INTEGER PRIMARY KEY, BigBlogId INTEGER NOT NULL);" +
                "INSERT INTO LzhBigBlog VALUES (1);" +
                "INSERT INTO LzhSmallPost VALUES (1,1),(2,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BigBlog>().HasKey(b => b.Id)
                .HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BigBlogId, b => b.Id)
        };
        DbContext ctx;
        try { ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false); }
        catch (Exception) { return; } // config-time fail-loud is acceptable

        using (ctx)
        {
            // CONTROL: the data and relationship resolve fine via a direct query -> 2 children exist.
            var direct = ctx.Query<SmallPost>().Where(p => p.BigBlogId == 1).Count();
            Assert.Equal(2, direct);

            var blog = ctx.Query<BigBlog>().First();
            int count;
            try { count = blog.Posts.Count; }
            catch (Exception) { return; } // load-time fail-loud is acceptable
            Assert.Equal(2, count); // if it returned, it MUST be correct, not silently 0
        }
    }

    // Matching-width CONTROL: identical model but principal PK is `int` (same as FK). Proves the empty
    // result above is caused specifically by the long-vs-int key box-type mismatch, not the model shape.
    [Table("LzhBigBlog2")]
    public class BigBlog2
    {
        [Key] public int Id { get; set; }
        public ICollection<SmallPost2> Posts { get; set; } = null!;
    }

    [Table("LzhSmallPost2")]
    public class SmallPost2
    {
        [Key] public int Id { get; set; }
        public int BigBlog2Id { get; set; }
    }

    [Fact]
    public void Lazy_collection_matching_key_widths_control_returns_children()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE LzhBigBlog2 (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE LzhSmallPost2 (Id INTEGER PRIMARY KEY, BigBlog2Id INTEGER NOT NULL);" +
                "INSERT INTO LzhBigBlog2 VALUES (1);" +
                "INSERT INTO LzhSmallPost2 VALUES (1,1),(2,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BigBlog2>().HasKey(b => b.Id)
                .HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BigBlog2Id, b => b.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        var blog = ctx.Query<BigBlog2>().First();
        Assert.Equal(2, blog.Posts.Count); // matching int/int -> loads correctly
    }

    // Same long-PK / int-FK mismatch, but through the EAGER Include path (IncludeProcessor) rather than
    // lazy loading. childGroups is keyed by the boxed int FK and looked up by the boxed long PK, so the
    // exact-match dictionary lookup misses -> the Include silently materializes an EMPTY collection though
    // the DB rows exist. Must EITHER coerce/match (correct rows) OR fail loud -- never silently empty.
    [Fact]
    public void Include_collection_mismatched_key_widths_must_not_silently_return_empty()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE LzhBigBlog (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE LzhSmallPost (Id INTEGER PRIMARY KEY, BigBlogId INTEGER NOT NULL);" +
                "INSERT INTO LzhBigBlog VALUES (1);" +
                "INSERT INTO LzhSmallPost VALUES (1,1),(2,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BigBlog>().HasKey(b => b.Id)
                .HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BigBlogId, b => b.Id)
        };
        DbContext ctx;
        try { ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false); }
        catch (Exception) { return; } // config-time fail-loud is acceptable

        using (ctx)
        {
            List<BigBlog> blogs;
            try { blogs = ctx.Query<BigBlog>().Include(b => b.Posts).ToList(); }
            catch (Exception) { return; } // load-time fail-loud is acceptable
            var blog = Assert.Single(blogs);
            Assert.Equal(2, blog.Posts.Count); // if it returned, it MUST be correct, not silently 0
        }
    }
}
