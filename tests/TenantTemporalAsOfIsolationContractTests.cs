using System;
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
/// Contract: temporal AsOf reads stay tenant-isolated (tenant matrix cell). The AsOf translation
/// swaps the FROM clause for a subquery over <c>&lt;Table&gt;_History</c>; the tenant filter is an
/// expression rewrite on the outer query, so this pins that the combination holds END-TO-END:
/// each tenant's AsOf reconstruction sees ONLY its own rows at every checkpoint (no cross-tenant
/// leak), sees ALL of its own rows (no loss), and a point read of another tenant's key comes back
/// empty. Current (non-AsOf) reads on the same temporal+tenant table stay scoped too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantTemporalAsOfIsolationContractTests
{
    [Table("TenAsOf_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenant : nORM.Enterprise.ITenantProvider
    {
        private readonly string _id;
        public FixedTenant(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private static DbContext CreateTenantContext(SqliteConnection cn, string tenant)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            TenantProvider = new FixedTenant(tenant)
        };
        opts.EnableTemporalVersioning();
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task AsOf_reconstruction_is_tenant_scoped_at_every_checkpoint()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TenAsOf_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, TenantId TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        await using var ctxA = CreateTenantContext(cn, "T1");
        await using var ctxB = CreateTenantContext(cn, "T2");

        // Interleave versions across tenants with IDENTICAL values so a lost tenant predicate
        // cannot hide behind distinctive data.
        var a = new Row { Id = 1, V = 10, TenantId = "T1" };
        ctxA.Add(a);
        await ctxA.SaveChangesAsync();
        var b = new Row { Id = 2, V = 10, TenantId = "T2" };
        ctxB.Add(b);
        await ctxB.SaveChangesAsync();

        await Task.Delay(150);
        var betweenV1V2 = DateTime.UtcNow;
        await Task.Delay(150);

        a.V = 20;
        await ctxA.SaveChangesAsync();
        b.V = 20;
        await ctxB.SaveChangesAsync();

        // Checkpoint 1: between the versions, each tenant reconstructs exactly its own v1 row.
        var oldA = await ctxA.Query<Row>().AsOf(betweenV1V2).ToListAsync();
        var oldB = await ctxB.Query<Row>().AsOf(betweenV1V2).ToListAsync();
        var onlyA = Assert.Single(oldA);
        Assert.Equal(1, onlyA.Id);
        Assert.Equal(10, onlyA.V);
        Assert.Equal("T1", onlyA.TenantId);
        var onlyB = Assert.Single(oldB);
        Assert.Equal(2, onlyB.Id);
        Assert.Equal("T2", onlyB.TenantId);

        // Checkpoint 2: after the last version, current-state AsOf stays scoped.
        var nowA = await ctxA.Query<Row>().AsOf(DateTime.UtcNow.AddSeconds(1)).ToListAsync();
        Assert.Single(nowA);
        Assert.Equal(20, nowA[0].V);
        Assert.Equal("T1", nowA[0].TenantId);

        // Point read of the OTHER tenant's key through AsOf: empty, not a leak.
        var foreignKey = await ctxA.Query<Row>().AsOf(betweenV1V2).Where(r => r.Id == 2).ToListAsync();
        Assert.Empty(foreignKey);

        // Non-AsOf current reads on the temporal+tenant table stay scoped as well.
        var currentA = await ctxA.Query<Row>().ToListAsync();
        Assert.Single(currentA);
        Assert.Equal("T1", currentA[0].TenantId);
    }

    [Fact]
    public async Task AsOf_aggregate_over_history_is_tenant_scoped()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TenAsOf_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, TenantId TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        await using var ctxA = CreateTenantContext(cn, "T1");
        await using var ctxB = CreateTenantContext(cn, "T2");

        ctxA.Add(new Row { Id = 1, V = 100, TenantId = "T1" });
        await ctxA.SaveChangesAsync();
        ctxB.Add(new Row { Id = 2, V = 900, TenantId = "T2" });
        await ctxB.SaveChangesAsync();

        await Task.Delay(150);
        var afterSeed = DateTime.UtcNow.AddSeconds(1);

        // A scalar aggregate through the AsOf history subquery must not absorb the other
        // tenant's rows.
        var countA = await ctxA.Query<Row>().AsOf(afterSeed).CountAsync();
        Assert.Equal(1, countA);
        var sumA = await ctxA.Query<Row>().AsOf(afterSeed).SumAsync(r => r.V);
        Assert.Equal(100, sumA);
    }
}
