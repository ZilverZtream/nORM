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
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Third adversarial leakage batch: SelectMany flattening a navigation collection to
/// entities (no result selector, with result selector, filtered navigation), where the
/// child filter must gate the flattened rows AND — under multi-tenancy — a cross-tenant
/// child (or a child whose parent is cross-tenant) must never leak into the flatten.
/// Also probes projecting a navigation collection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterLeakHuntSelectManyTests
{
    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    [Table("SmPa")]
    private class SmPa
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string TenantId { get; set; } = "";
        public List<SmKid> Kids { get; set; } = new();
    }

    [Table("SmKid")]
    private class SmKid
    {
        [Key] public int Id { get; set; }
        public int PaId { get; set; }
        public string Label { get; set; } = "";
        public bool IsDeleted { get; set; }
        public string TenantId { get; set; } = "";
    }

    // ── Soft-delete flavor ────────────────────────────────────────────────────
    private static DbContext SoftDeleteCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SmPa  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
                CREATE TABLE SmKid (Id INTEGER PRIMARY KEY, PaId INTEGER NOT NULL, Label TEXT NOT NULL, IsDeleted INTEGER NOT NULL, TenantId TEXT NOT NULL);
                INSERT INTO SmPa  VALUES (1,'p1','X');
                INSERT INTO SmKid VALUES (10,1,'k-live',0,'X'),(11,1,'k-deleted',1,'X');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmPa>().HasKey(p => p.Id);
                mb.Entity<SmKid>().HasKey(k => k.Id);
                mb.Entity<SmPa>().HasMany(p => p.Kids).WithOne().HasForeignKey(k => k.PaId, p => p.Id);
            }
        };
        opts.AddGlobalFilter<SmKid>(k => !k.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task SelectMany_to_entities_no_result_selector_excludes_softdeleted()
    {
        using var ctx = SoftDeleteCtx(out var cn);
        using var _cn = cn;

        var kids = (await ctx.Query<SmPa>().SelectMany(p => p.Kids).ToListAsync())
            .OrderBy(k => k.Id).ToList();

        Assert.Equal(new[] { 10 }, kids.Select(k => k.Id).ToArray());
        Assert.DoesNotContain(kids, k => k.IsDeleted);
    }

    [Fact]
    public async Task SelectMany_with_result_selector_excludes_softdeleted()
    {
        using var ctx = SoftDeleteCtx(out var cn);
        using var _cn = cn;

        var rows = (await ctx.Query<SmPa>()
            .SelectMany(p => p.Kids, (p, k) => new { p.Name, k.Label })
            .ToListAsync()).OrderBy(r => r.Label).ToList();

        Assert.Equal(new[] { "k-live" }, rows.Select(r => r.Label).ToArray());
        Assert.DoesNotContain(rows, r => r.Label == "k-deleted");
    }

    [Fact]
    public async Task SelectMany_filtered_navigation_applies_both_user_and_global_filter()
    {
        using var ctx = SoftDeleteCtx(out var cn);
        using var _cn = cn;

        // User filter selects Label starting with 'k'; the global soft-delete filter must ALSO
        // apply, so the deleted kid (11) is excluded even though it matches the user filter.
        var kids = (await ctx.Query<SmPa>()
            .SelectMany(p => p.Kids.Where(k => k.Label != "nope"))
            .ToListAsync()).OrderBy(k => k.Id).ToList();

        Assert.Equal(new[] { 10 }, kids.Select(k => k.Id).ToArray());
        Assert.DoesNotContain(kids, k => k.IsDeleted);
    }

    // ── Tenant flavor: cross-tenant poison on BOTH parent and child ───────────
    private static DbContext TenantCtx(SqliteConnection cn, string tenant)
        => new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenant),
            TenantColumnName = "TenantId",
            OnModelCreating = mb =>
            {
                mb.Entity<SmPa>().HasKey(p => p.Id);
                mb.Entity<SmKid>().HasKey(k => k.Id);
                mb.Entity<SmPa>().HasMany(p => p.Kids).WithOne().HasForeignKey(k => k.PaId, p => p.Id);
            }
        }, ownsConnection: false);

    private static SqliteConnection TenantDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmPa  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE SmKid (Id INTEGER PRIMARY KEY, PaId INTEGER NOT NULL, Label TEXT NOT NULL, IsDeleted INTEGER NOT NULL, TenantId TEXT NOT NULL);
            -- Parent 1 is tenant T1; parent 2 is tenant T2.
            INSERT INTO SmPa  VALUES (1,'p1-T1','T1'),(2,'p2-T2','T2');
            -- Kid 10: T1 kid under T1 parent -> the ONLY row T1 should see.
            INSERT INTO SmKid VALUES (10,1,'k-own',0,'T1');
            -- Kid 20: T1-labelled kid under the T2 parent -> must NOT leak (parent is cross-tenant).
            INSERT INTO SmKid VALUES (20,2,'k-poison-parent',0,'T1');
            -- Kid 30: T2 kid under the T1 parent -> must NOT leak (kid is cross-tenant).
            INSERT INTO SmKid VALUES (30,1,'k-poison-child',0,'T2');
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Tenant_SelectMany_to_entities_excludes_cross_tenant_parent_and_child()
    {
        using var cn = TenantDb();
        using var ctx = TenantCtx(cn, "T1");

        var kids = (await ctx.Query<SmPa>().SelectMany(p => p.Kids).ToListAsync())
            .OrderBy(k => k.Id).ToList();

        // Only kid 10 (T1 kid under T1 parent). Kid 20 (T1 kid under T2 parent) and kid 30
        // (T2 kid under T1 parent) must both be excluded.
        Assert.Equal(new[] { 10 }, kids.Select(k => k.Id).ToArray());
        Assert.All(kids, k => Assert.Equal("T1", k.TenantId));
    }

    [Fact]
    public async Task Tenant_SelectMany_with_result_selector_excludes_cross_tenant()
    {
        using var cn = TenantDb();
        using var ctx = TenantCtx(cn, "T1");

        var rows = await ctx.Query<SmPa>()
            .SelectMany(p => p.Kids, (p, k) => new { PName = p.Name, k.Label, KTenant = k.TenantId })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal("k-own", row.Label);
        Assert.Equal("T1", row.KTenant);
        Assert.Equal("p1-T1", row.PName);
    }

    // ── Projecting a navigation collection ────────────────────────────────────
    [Fact]
    public async Task Projected_navigation_collection_excludes_softdeleted_or_fails_loud()
    {
        using var ctx = SoftDeleteCtx(out var cn);
        using var _cn = cn;

        try
        {
            // Project the principal key (Id) so the shaped-collection projection is well-formed.
            var rows = await ctx.Query<SmPa>()
                .Select(p => new { p.Id, p.Name, Kids = p.Kids.ToList() })
                .ToListAsync();

            var row = Assert.Single(rows);
            // If nORM materializes the projected collection, the soft-deleted kid must NOT leak.
            Assert.Equal(new[] { 10 }, row.Kids.Select(k => k.Id).OrderBy(i => i).ToArray());
            Assert.DoesNotContain(row.Kids, k => k.IsDeleted);
        }
        catch (nORM.Core.NormUnsupportedFeatureException)
        {
            // Fail-loud is acceptable — never silent-wrong. The point is only that a
            // soft-deleted child never silently appears.
        }
    }
}
