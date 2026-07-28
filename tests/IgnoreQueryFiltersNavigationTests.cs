using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// IgnoreQueryFilters() must suppress the user's global filters for the WHOLE query — not just the root tree.
/// Before the fix an eager Include / split-query child load and a translator-built correlated subquery each
/// re-emitted the user global filter unconditionally, so IgnoreQueryFilters() silently OMITTED rows the caller
/// explicitly asked to see. The filter here is defined only on the CHILD type (Post), so the root Blog tree is
/// byte-identical with and without IgnoreQueryFilters — this also exercises the plan-cache fingerprint, which
/// must separate the two or one shape's plan is replayed for the other.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IgnoreQueryFiltersNavigationTests
{
    [Table("IqfnBlog_Test")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public List<Post> Posts { get; set; } = new();
    }

    [Table("IqfnPost_Test")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:iqfn_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IqfnBlog_Test (Id INTEGER PRIMARY KEY);
                CREATE TABLE IqfnPost_Test (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO IqfnBlog_Test VALUES (1);
                -- Blog 1 has a live post (10) and a soft-deleted one (11). The global filter hides 11.
                INSERT INTO IqfnPost_Test VALUES (10, 1, 0), (11, 1, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions();
        // Filter lives ONLY on the child — the root Blog query is identical with/without IgnoreQueryFilters.
        opts.AddGlobalFilter<Post>(p => !p.IsDeleted);
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Include_without_ignore_applies_child_filter()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var blog = (await ((INormQueryable<Blog>)ctx.Query<Blog>())
                .Include(b => b.Posts)
                .ToListAsync())
            .Single();

        // Control: the soft-delete filter is in force, so only the live post loads.
        Assert.Equal(new[] { 10 }, blog.Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task Include_with_ignore_loads_all_child_rows()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var blog = (await ((INormQueryable<Blog>)ctx.Query<Blog>().IgnoreQueryFilters())
                .Include(b => b.Posts)
                .ToListAsync())
            .Single();

        // IgnoreQueryFilters must reach the eager child load: BOTH posts, including the soft-deleted one.
        Assert.Equal(new[] { 10, 11 }, blog.Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task Split_query_include_with_ignore_loads_all_child_rows()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var blog = (await ((INormQueryable<Blog>)ctx.Query<Blog>().IgnoreQueryFilters())
                .Include(b => b.Posts)
                .AsSplitQuery()
                .ToListAsync())
            .Single();

        // Split-query child fetch runs through the dependent-query loader — it too must drop the user filter.
        Assert.Equal(new[] { 10, 11 }, blog.Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task Correlated_subquery_count_without_ignore_applies_child_filter()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Control: the filtered count is 1 (only the live post), so the ==2 predicate matches nothing.
        var ids = (await ctx.Query<Blog>()
                .Where(b => b.Posts.Count() == 2)
                .ToListAsync())
            .Select(b => b.Id).ToList();

        Assert.Empty(ids);
    }

    [Fact]
    public async Task Correlated_subquery_count_with_ignore_counts_all_children()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // With filters ignored the subquery counts BOTH posts, so the ==2 predicate matches blog 1.
        var ids = (await ctx.Query<Blog>().IgnoreQueryFilters()
                .Where(b => b.Posts.Count() == 2)
                .ToListAsync())
            .Select(b => b.Id).ToList();

        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Explicit_subquery_without_ignore_applies_child_filter()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Control: the explicit correlated subquery root is wrapped with the user filter → count 1 → no match.
        var ids = (await ctx.Query<Blog>()
                .Where(b => ctx.Query<Post>().Count(p => p.BlogId == b.Id) == 2)
                .ToListAsync())
            .Select(b => b.Id).ToList();

        Assert.Empty(ids);
    }

    [Fact]
    public async Task Explicit_subquery_with_ignore_counts_all_children()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // The explicit-subquery-root filter wrapping (WrapSubqueryRoot) must honor IgnoreQueryFilters too:
        // both posts count, so the ==2 predicate matches blog 1.
        var ids = (await ctx.Query<Blog>().IgnoreQueryFilters()
                .Where(b => ctx.Query<Post>().Count(p => p.BlogId == b.Id) == 2)
                .ToListAsync())
            .Select(b => b.Id).ToList();

        Assert.Equal(new[] { 1 }, ids);
    }

    // ── Tenant boundary must survive IgnoreQueryFilters (security invariant) ──

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    [Table("IqftBlog_Test")]
    public class TBlog
    {
        [Key] public int Id { get; set; }
        public string TenantKey { get; set; } = "";
        public List<TPost> Posts { get; set; } = new();
    }

    [Table("IqftPost_Test")]
    public class TPost
    {
        [Key] public int Id { get; set; }
        public int TBlogId { get; set; }
        public string TenantKey { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateTenantDb()
    {
        var cs = $"Data Source=file:iqft_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IqftBlog_Test (Id INTEGER PRIMARY KEY, TenantKey TEXT NOT NULL);
                CREATE TABLE IqftPost_Test (Id INTEGER PRIMARY KEY, TBlogId INTEGER NOT NULL, TenantKey TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO IqftBlog_Test VALUES (1, 'T1');
                -- Post 10: T1 live; 11: T1 soft-deleted; 12: T2 live sharing the FK — the cross-tenant leak detector.
                INSERT INTO IqftPost_Test VALUES (10, 1, 'T1', 0), (11, 1, 'T1', 1), (12, 1, 'T2', 0);
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1"), TenantColumnName = "TenantKey" };
        opts.AddGlobalFilter<TPost>(p => !p.IsDeleted);
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Include_with_ignore_keeps_tenant_boundary()
    {
        var (keeper, ctx) = CreateTenantDb();
        using var _ = keeper;
        await using var __ = ctx;

        var blog = (await ((INormQueryable<TBlog>)ctx.Query<TBlog>().IgnoreQueryFilters())
                .Include(b => b.Posts)
                .ToListAsync())
            .Single();

        // IgnoreQueryFilters drops the soft-delete filter (11 appears) but NEVER the tenant boundary:
        // the other tenant's post (12) must not leak, even though it shares the FK.
        Assert.Equal(new[] { 10, 11 }, blog.Posts.Select(p => p.Id).OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task Explicit_subquery_with_ignore_keeps_tenant_boundary()
    {
        var (keeper, ctx) = CreateTenantDb();
        using var _ = keeper;
        await using var __ = ctx;

        // The subquery counts T1's posts only (10 + 11 = 2), never T2's — tenant survives IgnoreQueryFilters.
        var ids = (await ctx.Query<TBlog>().IgnoreQueryFilters()
                .Where(b => ctx.Query<TPost>().Count(p => p.TBlogId == b.Id) == 2)
                .ToListAsync())
            .Select(b => b.Id).ToList();

        Assert.Equal(new[] { 1 }, ids);
    }
}
