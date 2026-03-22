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
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Table("MtpcParent")]
file class MtpcParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
    public ICollection<MtpcChild> Children { get; set; } = new List<MtpcChild>();
}

[Table("MtpcChild")]
file class MtpcChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public string Value { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
}

[Table("MtpcRow")]
file class MtpcRow
{
    [Key]
    public int Id { get; set; }
    public string TenantId { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
}

file sealed class FixedTenantProvider(string id) : ITenantProvider
{
    public object GetCurrentTenantId() => id;
}

/// <summary>
/// Tests that the static query plan cache — shared across all <c>DbContext</c> instances —
/// correctly isolates results per tenant when multiple tenants use the same query shape.
/// Covers compiled queries, eager-loaded Includes, high-contention parallel contexts,
/// adversarial SQL-injection tenant IDs, and sequential reuse of a shared plan cache.
/// </summary>
public class MultiTenantPlanCacheTests
{
    private static DbContextOptions TenantOpts(string tenantId, Action<ModelBuilder>? model = null) => new()
    {
        TenantProvider = new FixedTenantProvider(tenantId),
        OnModelCreating = model
    };

    [Fact]
    public async Task CompiledQuery_TwoTenants_SharePlanCache_NoCrossLeak()
    {
        // Two tenants execute the same compiled query against a shared in-memory DB.
        // Neither tenant should see the other's rows even though the plan cache is static.
        var dbName = $"Mtpc_CQ_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using (var setup = keeper.CreateCommand())
        {
            setup.CommandText = @"
CREATE TABLE MtpcRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Data TEXT NOT NULL);
INSERT INTO MtpcRow VALUES (1, 'A', 'data-A1');
INSERT INTO MtpcRow VALUES (2, 'A', 'data-A2');
INSERT INTO MtpcRow VALUES (3, 'B', 'data-B1');";
            setup.ExecuteNonQuery();
        }

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<MtpcRow>().Where(r => r.Id >= minId));

        using var cnA = new SqliteConnection(connStr);
        using var cnB = new SqliteConnection(connStr);
        cnA.Open(); cnB.Open();

        using var ctxA = new DbContext(cnA, new SqliteProvider(), TenantOpts("A"));
        using var ctxB = new DbContext(cnB, new SqliteProvider(), TenantOpts("B"));

        var rowsA = await compiled(ctxA, 1);
        Assert.Equal(2, rowsA.Count);
        Assert.All(rowsA, r => Assert.Equal("A", r.TenantId));

        var rowsB = await compiled(ctxB, 1);
        Assert.Single(rowsB);
        Assert.All(rowsB, r => Assert.Equal("B", r.TenantId));
    }

    [Fact]
    public async Task Include_SharedPlanCache_TenantFilterAppliedAtBothLevels()
    {
        // Include eager-loading must apply the tenant predicate to both the parent and
        // child queries even when the plan cache is shared across tenants.
        // Adversarial: both tenants share ParentId=1 as the FK value.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE MtpcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
CREATE TABLE MtpcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Value TEXT NOT NULL, TenantId TEXT NOT NULL);
INSERT INTO MtpcParent VALUES (1, 'pA', 'A');
INSERT INTO MtpcParent VALUES (2, 'pB', 'B');
INSERT INTO MtpcChild  VALUES (10, 1, 'child-A', 'A');
INSERT INTO MtpcChild  VALUES (11, 1, 'cross-tenant-poison', 'B');
INSERT INTO MtpcChild  VALUES (12, 2, 'child-B', 'B');";
        setup.ExecuteNonQuery();

        var opts = TenantOpts("A", mb =>
            mb.Entity<MtpcParent>()
              .HasMany(p => p.Children)
              .WithOne()
              .HasForeignKey(c => c.ParentId, p => p.Id));

        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var parents = await ((INormQueryable<MtpcParent>)ctx.Query<MtpcParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(parents);
        Assert.Equal("pA", parents[0].Name);
        Assert.Single(parents[0].Children);
        Assert.Equal("child-A", parents[0].Children.First().Value);
        Assert.DoesNotContain(parents[0].Children, c => c.TenantId != "A");
    }

    [Fact]
    public async Task HighContention_50ConcurrentTenantContexts_NoDataLeakage()
    {
        // 50 concurrent tasks each with an independent tenant context querying their
        // own data must produce correctly isolated results with no cross-tenant leakage.
        var dbName = $"Mtpc_HC_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        const int Degree = 50;
        using (var setup = keeper.CreateCommand())
        {
            var inserts = string.Join(";", Enumerable.Range(1, Degree)
                .Select(i => $"INSERT INTO MtpcRow VALUES ({i}, 'T{i}', 'data-{i}')"));
            setup.CommandText = "CREATE TABLE MtpcRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Data TEXT NOT NULL);" + inserts;
            setup.ExecuteNonQuery();
        }

        var tasks = Enumerable.Range(1, Degree).Select(async i =>
        {
            using var cn = new SqliteConnection(connStr);
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider(), TenantOpts($"T{i}"));
            var rows = await ctx.Query<MtpcRow>().ToListAsync();
            Assert.Single(rows);
            Assert.Equal($"T{i}", rows[0].TenantId);
            Assert.Equal($"data-{i}", rows[0].Data);
        });

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task AdversarialTenantId_SqlMetaCharacters_NoInjectionInInclude()
    {
        // SQL meta-characters injected as a tenant ID must be parameterized safely and
        // must not cause SQL injection in tenant-scoped Include queries.
        const string AdversarialTenantId = "'; DROP TABLE MtpcChild; --";

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE MtpcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
CREATE TABLE MtpcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Value TEXT NOT NULL, TenantId TEXT NOT NULL);
INSERT INTO MtpcParent VALUES (1, 'parent', 'safe-tenant');
INSERT INTO MtpcChild  VALUES (10, 1, 'child', 'safe-tenant');";
        setup.ExecuteNonQuery();

        var opts = TenantOpts(AdversarialTenantId, mb =>
            mb.Entity<MtpcParent>()
              .HasMany(p => p.Children)
              .WithOne()
              .HasForeignKey(c => c.ParentId, p => p.Id));

        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var parents = await ((INormQueryable<MtpcParent>)ctx.Query<MtpcParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        // Adversarial tenant sees no rows (all rows are 'safe-tenant').
        Assert.Empty(parents);

        // The table must still exist — no injection succeeded.
        using var probe = cn.CreateCommand();
        probe.CommandText = "SELECT COUNT(*) FROM MtpcChild";
        Assert.Equal(1L, Convert.ToInt64(probe.ExecuteScalar()));
    }

    [Fact]
    public async Task RepeatedPlanCacheReuse_DifferentTenants_AlwaysIsolated()
    {
        // The same query shape executed 10 times alternating between two tenants
        // must always return only that tenant's rows, with no stale plan cache leakage.
        var dbName = $"Mtpc_SP_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using (var setup = keeper.CreateCommand())
        {
            setup.CommandText = @"
CREATE TABLE MtpcRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Data TEXT NOT NULL);
INSERT INTO MtpcRow VALUES (1, 'X', 'x-data');
INSERT INTO MtpcRow VALUES (2, 'Y', 'y-data');
INSERT INTO MtpcRow VALUES (3, 'X', 'x-data2');";
            setup.ExecuteNonQuery();
        }

        for (int pass = 0; pass < 10; pass++)
        {
            foreach (var (tenant, expectedCount) in new[] { ("X", 2), ("Y", 1) })
            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = new DbContext(cn, new SqliteProvider(), TenantOpts(tenant));
                var rows = await ctx.Query<MtpcRow>().ToListAsync();
                Assert.Equal(expectedCount, rows.Count);
                Assert.All(rows, r => Assert.Equal(tenant, r.TenantId));
            }
        }
    }
}
