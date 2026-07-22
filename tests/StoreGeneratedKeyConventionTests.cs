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
/// EF Core parity: a single-column integer primary key with no explicit value-generation config is
/// store-generated when its value is default (0) and honored when non-default — on SQLite, where a
/// single-column INTEGER primary key is an alias for the rowid. Covers both the tracked (Add +
/// SaveChanges) path and the direct InsertAsync path, plus the opt-outs (explicit [DatabaseGenerated],
/// composite keys) that must stay client-set. The tables are created by nORM's own EnsureCreated so the
/// generated DDL is exercised end to end. Guards the honor-non-zero convention against regression.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StoreGeneratedKeyConventionTests
{
    [Table("SgkWidget")]
    public sealed class Widget
    {
        [Key] public int Id { get; set; }   // convention key: no [DatabaseGenerated]
        public string Name { get; set; } = "";
    }

    [Table("SgkBlog")]
    public sealed class Blog
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("SgkPost")]
    public sealed class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Text { get; set; } = "";
        public Blog Blog { get; set; } = default!;
    }

    [Table("SgkManual")]
    public sealed class Manual
    {
        // Explicit opt-out: value generation is configured to None, so the convention must NOT apply.
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("SgkPair")]
    public sealed class Pair
    {
        [Key, Column(Order = 0)] public int Left { get; set; }
        [Key, Column(Order = 1)] public int Right { get; set; }
        public string Data { get; set; } = "";
    }

    private static async Task<DbContext> NewCtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(x => x.Id);
                mb.Entity<Blog>().HasKey(x => x.Id);
                mb.Entity<Post>().HasKey(x => x.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne(p => p.Blog).HasForeignKey(p => p.BlogId, b => b.Id);
                mb.Entity<Manual>().HasKey(x => x.Id);
                mb.Entity<Pair>().HasKey(e => new { e.Left, e.Right });
            }
        });
        await ctx.Database.EnsureCreatedAsync();
        return ctx;
    }

    [Fact]
    public async Task TwoDefaultKeys_SaveChanges_GetDistinctGeneratedIds()
    {
        await using var ctx = await NewCtxAsync();
        var a = new Widget { Name = "a" };
        var b = new Widget { Name = "b" };
        ctx.Add(a);
        ctx.Add(b);
        await ctx.SaveChangesAsync();

        Assert.True(a.Id > 0);
        Assert.True(b.Id > 0);
        Assert.NotEqual(a.Id, b.Id);
        Assert.Equal(2, await ctx.Query<Widget>().CountAsync());
    }

    [Fact]
    public async Task ExplicitNonZeroKey_SaveChanges_IsHonored()
    {
        await using var ctx = await NewCtxAsync();
        var w = new Widget { Id = 5000, Name = "x" };
        ctx.Add(w);
        await ctx.SaveChangesAsync();

        Assert.Equal(5000, w.Id);
        Assert.Equal("x", (await ctx.Query<Widget>().SingleAsync(r => r.Id == 5000)).Name);
    }

    [Fact]
    public async Task MixedBatch_DefaultGeneratedExplicitHonored()
    {
        await using var ctx = await NewCtxAsync();
        var def = new Widget { Name = "def" };
        var exp = new Widget { Id = 100, Name = "exp" };
        var def2 = new Widget { Name = "def2" };
        ctx.Add(def);
        ctx.Add(exp);
        ctx.Add(def2);
        await ctx.SaveChangesAsync();

        Assert.Equal(100, exp.Id);
        Assert.True(def.Id > 0 && def.Id != 100);
        Assert.True(def2.Id > 0 && def2.Id != 100 && def2.Id != def.Id);
        Assert.Equal(3, await ctx.Query<Widget>().CountAsync());
    }

    [Fact]
    public async Task ParentChildGraph_ChildForeignKeysUseGeneratedParentKey()
    {
        await using var ctx = await NewCtxAsync();
        var blog = new Blog { Title = "b" };
        blog.Posts.Add(new Post { Text = "p1" });
        blog.Posts.Add(new Post { Text = "p2" });
        ctx.Add(blog);
        await ctx.SaveChangesAsync();

        Assert.True(blog.Id > 0);
        Assert.All(blog.Posts, p => Assert.Equal(blog.Id, p.BlogId));
        Assert.All(blog.Posts, p => Assert.True(p.Id > 0));
        var posts = await ctx.Query<Post>().ToListAsync();
        Assert.Equal(2, posts.Count);
        Assert.All(posts, p => Assert.Equal(blog.Id, p.BlogId));
    }

    [Fact]
    public async Task SecondSaveInSameContext_DoesNotReinsert()
    {
        await using var ctx = await NewCtxAsync();
        var a = new Widget { Name = "first" };
        ctx.Add(a);
        await ctx.SaveChangesAsync();
        var firstId = a.Id;

        var b = new Widget { Name = "second" };
        ctx.Add(b);
        await ctx.SaveChangesAsync();

        Assert.True(b.Id > 0 && b.Id != firstId);
        Assert.Equal(2, await ctx.Query<Widget>().CountAsync());
    }

    [Fact]
    public async Task DirectInsert_TwoDefaultKeys_GetDistinctGeneratedIds()
    {
        await using var ctx = await NewCtxAsync();
        var a = new Widget { Name = "a" };
        var b = new Widget { Name = "b" };
        await ctx.InsertAsync(a);
        await ctx.InsertAsync(b);

        Assert.True(a.Id > 0 && b.Id > 0 && a.Id != b.Id);
        Assert.Equal(2, await ctx.Query<Widget>().CountAsync());
    }

    [Fact]
    public async Task DirectInsert_ExplicitNonZeroKey_IsHonored()
    {
        await using var ctx = await NewCtxAsync();
        var w = new Widget { Id = 777, Name = "e" };
        await ctx.InsertAsync(w);

        Assert.Equal(777, w.Id);
        Assert.Equal("e", (await ctx.Query<Widget>().SingleAsync(r => r.Id == 777)).Name);
    }

    [Fact]
    public async Task DatabaseGeneratedNone_OptsOutOfConvention_ClientKeyKept()
    {
        await using var ctx = await NewCtxAsync();
        // An explicit opt-out is client-set: the value is written as-is, not store-generated.
        var m = new Manual { Id = 42, Name = "m" };
        ctx.Add(m);
        await ctx.SaveChangesAsync();
        Assert.Equal(42, m.Id);

        // A default (0) value is likewise kept as-is (a store-generated key would have become positive).
        var zero = new Manual { Id = 0, Name = "z" };
        ctx.Add(zero);
        await ctx.SaveChangesAsync();
        Assert.Equal(0, zero.Id);
        Assert.Equal(2, await ctx.Query<Manual>().CountAsync());
    }

    [Fact]
    public async Task CompositeKey_NotConvention_ExplicitValuesHonored()
    {
        await using var ctx = await NewCtxAsync();
        var p1 = new Pair { Left = 1, Right = 2, Data = "a" };
        var p2 = new Pair { Left = 1, Right = 3, Data = "b" };
        ctx.Add(p1);
        ctx.Add(p2);
        await ctx.SaveChangesAsync();

        Assert.Equal(1, p1.Left);
        Assert.Equal(2, p1.Right);
        Assert.Equal(1, p2.Left);
        Assert.Equal(3, p2.Right);
        var rows = await ctx.Query<Pair>().OrderBy(r => r.Right).ToListAsync();
        Assert.Equal(new[] { "a", "b" }, rows.Select(r => r.Data).ToArray());
    }
}
