using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract: the tenant filter holds on every read shape (tenant matrix cell). Two tenants share
/// one table with IDENTICAL values, so a shape that loses the tenant predicate produces visibly
/// wrong counts/pairs instead of hiding behind distinctive data. Covers scalar aggregates,
/// GroupBy, self-joins (BOTH join sources must be scoped), set operations (both branches),
/// ordered paging tails, the First/Single family, correlated Any subqueries, Contains, and the
/// fast-path shapes (simple Where / Take): tenant-scoped contexts must bypass the fast-path
/// executor - its cached SQL carries no tenant predicate - and take the fully filtered pipeline.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantQueryShapeSweepContractTests
{
    [Table("TenSweep_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenant : ITenantProvider
    {
        private readonly string _id;
        public FixedTenant(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateTenantContext(string tenant)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // T1 and T2 hold IDENTICAL V values on distinct keys.
            cmd.CommandText =
                "CREATE TABLE TenSweep_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, TenantId TEXT NOT NULL);" +
                "INSERT INTO TenSweep_Row VALUES (1, 10, 'T1'), (2, 20, 'T1'), (3, 10, 'T2'), (4, 20, 'T2');";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            TenantProvider = new FixedTenant(tenant)
        });
        return (cn, ctx);
    }

    [Fact]
    public async Task Scalar_aggregates_only_see_the_callers_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        Assert.Equal(2, await ctx.Query<Row>().CountAsync());
        Assert.Equal(30, await ctx.Query<Row>().SumAsync(r => r.V));
        Assert.Equal(10, await ctx.Query<Row>().MinAsync(r => r.V));
        Assert.Equal(20, await ctx.Query<Row>().MaxAsync(r => r.V));
        Assert.Equal(15.0, await ctx.Query<Row>().AverageAsync(r => r.V));
    }

    [Fact]
    public async Task GroupBy_groups_only_within_the_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // Unscoped grouping would count 2 per V (one row from each tenant).
        var groups = await ctx.Query<Row>()
            .GroupBy(r => r.V)
            .Select(g => new { g.Key, C = g.Count() })
            .ToListAsync();
        Assert.Equal(2, groups.Count);
        Assert.All(groups, g => Assert.Equal(1, g.C));
    }

    [Fact]
    public async Task Both_sides_of_a_self_join_are_tenant_scoped()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // An unscoped inner source pairs T1's V=10 with T2's Id=3.
        var pairs = await ctx.Query<Row>()
            .Join(ctx.Query<Row>(), r => r.V, o => o.V, (r, o) => new { r.Id, OtherId = o.Id })
            .ToListAsync();
        Assert.Equal(2, pairs.Count);
        Assert.All(pairs, p =>
        {
            Assert.InRange(p.Id, 1, 2);
            Assert.InRange(p.OtherId, 1, 2);
        });
    }

    [Fact]
    public async Task Both_branches_of_set_operations_are_tenant_scoped()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        var union = await ctx.Query<Row>().Where(r => r.V == 10)
            .Union(ctx.Query<Row>().Where(r => r.V == 20))
            .ToListAsync();
        Assert.Equal(2, union.Count);
        Assert.All(union, r => Assert.Equal("T1", r.TenantId));

        // An unscoped second branch would leave rows behind (T2's V=20 is not T1's).
        var except = await ctx.Query<Row>()
            .Except(ctx.Query<Row>().Where(r => r.V == 20))
            .ToListAsync();
        var only = Assert.Single(except);
        Assert.Equal(1, only.Id);
    }

    [Fact]
    public async Task Ordered_paging_tail_stays_within_the_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // Unscoped ordering would page T2's Id=3 into the tail.
        var tail = await ctx.Query<Row>().OrderBy(r => r.Id).Skip(1).Take(2).ToListAsync();
        var only = Assert.Single(tail);
        Assert.Equal(2, only.Id);
    }

    [Fact]
    public async Task First_family_never_returns_a_foreign_tenant_row()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        Assert.Equal(1, (await ctx.Query<Row>().FirstAsync(r => r.V == 10)).Id);
        Assert.Equal(1, (await ctx.Query<Row>().SingleAsync(r => r.Id == 1)).Id);
        // The other tenant's key is invisible, not an exception-free leak.
        Assert.Null(await ctx.Query<Row>().FirstOrDefaultAsync(r => r.Id == 3));
    }

    [Fact]
    public async Task Correlated_any_subquery_is_tenant_scoped()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // The inner Any targets the OTHER tenant's key: a scoped subquery finds nothing.
        var viaForeignKey = await ctx.Query<Row>()
            .Where(r => ctx.Query<Row>().Any(o => o.Id == 3 && o.V == r.V))
            .ToListAsync();
        Assert.Empty(viaForeignKey);

        // No own-row loss: the same shape against the caller's key matches.
        var viaOwnKey = await ctx.Query<Row>()
            .Where(r => ctx.Query<Row>().Any(o => o.Id == 1 && o.V == r.V))
            .ToListAsync();
        Assert.Single(viaOwnKey);
        Assert.Equal(1, viaOwnKey[0].Id);
    }

    [Fact]
    public async Task Contains_over_a_mixed_id_list_returns_only_own_rows()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        var ids = new[] { 1, 3 };
        var rows = await ctx.Query<Row>().Where(r => ids.Contains(r.Id)).ToListAsync();
        var only = Assert.Single(rows);
        Assert.Equal(1, only.Id);
    }

    [Fact]
    public async Task Fast_path_shapes_take_the_filtered_pipeline_under_a_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // Simple property-equality Where and plain Take are exactly the shapes the fast-path
        // executor serves; its cached SQL has NO tenant predicate, so tenant-scoped contexts
        // must bypass it. A foreign key comes back empty and a Take stays scoped.
        Assert.Empty(await ctx.Query<Row>().Where(r => r.Id == 3).ToListAsync());
        var own = await ctx.Query<Row>().Where(r => r.Id == 1).ToListAsync();
        Assert.Single(own);
        Assert.Equal("T1", own[0].TenantId);

        var take = await ctx.Query<Row>().Take(10).ToListAsync();
        Assert.Equal(2, take.Count);
        Assert.All(take, r => Assert.Equal("T1", r.TenantId));

        // Sync path too.
        var sync = ctx.Query<Row>().Where(r => r.Id == 4).ToList();
        Assert.Empty(sync);
    }
}
