using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A shaped collection projection composes over a composable FromSqlRaw root:
/// <c>FromSqlRaw&lt;Order&gt;(sql).Select(o =&gt; new { o.Id, Lines = o.Lines.Where(p).Select(..).ToList() })</c>.
/// Unlike <c>Include</c> (which rebuilds the root from the mapped table and so fails loud over a raw root),
/// the shaped-collection Select keeps the raw SQL as the root FROM and keys the split-query children off the
/// raw query's primary keys — so the raw filter is honoured and the child load still applies tenant and
/// per-element filters.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FromSqlRawShapedCollectionTests
{
    [Table("Rs2Order")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("Rs2Line")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int TenantId { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    private sealed class FixedTenant(int t) : ITenantProvider { public object GetCurrentTenantId() => t; }

    private static void Seed(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Rs2Order (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL);
            CREATE TABLE Rs2Line (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, TenantId INTEGER NOT NULL, Sku TEXT NOT NULL, Qty INTEGER NOT NULL);
            INSERT INTO Rs2Order VALUES (1,1),(2,1),(3,2);
            -- Order 1 (tenant 1) also has a FORGED tenant-2 line pointing at it.
            INSERT INTO Rs2Line VALUES (1,1,1,'a',3),(2,1,1,'b',8),(3,1,2,'FORGED',9),(4,2,1,'c',5);
            """;
        cmd.ExecuteNonQuery();
    }

    private static DbContext Ctx(SqliteConnection cn, int? tenant = null)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        if (tenant is { } t) { opts.TenantColumnName = "TenantId"; opts.TenantProvider = new FixedTenant(t); }
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void shaped_collection_over_raw_root_respects_the_raw_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); Seed(cn);
        using var ctx = Ctx(cn);   // no tenant
        var rows = ((INormQueryable<Order>)ctx.FromSqlRaw<Order>("SELECT * FROM Rs2Order WHERE Id <= 2"))
            .Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToList() })
            .ToList().OrderBy(r => r.Id).ToList();
        // The raw filter keeps orders 1,2 (not 3), and each order's Lines load via the split query.
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { "FORGED", "a", "b" }, rows[0].Skus.OrderBy(s => s, System.StringComparer.Ordinal).ToArray());   // no tenant → all 3 lines of order 1
        Assert.Equal(new[] { "c" }, rows[1].Skus);
    }

    [Fact]
    public void shaped_collection_over_raw_root_applies_element_filter_and_tenant()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); Seed(cn);
        using var ctx = Ctx(cn, tenant: 1);
        var rows = ((INormQueryable<Order>)ctx.FromSqlRaw<Order>("SELECT * FROM Rs2Order"))
            .Select(o => new { o.Id, Big = o.Lines.Where(l => l.Qty > 4).Select(l => l.Sku).ToList() })
            .ToList().OrderBy(r => r.Id).ToList();
        // Tenant 1 sees orders 1,2 (order 3 is tenant 2). Order 1 Big (qty>4): 'b'(8) only — the forged
        // tenant-2 line (qty 9) is excluded by the child tenant filter, so the raw root does not leak it.
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { "b" }, rows[0].Big);
        Assert.Equal(new[] { "c" }, rows[1].Big);
    }
}
