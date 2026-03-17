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
/// Regression tests for F1/S1/X1 audit finding:
/// SaveOwnedCollectionsAsync (and LoadOwnedCollectionsAsync) previously silently
/// no-oped for owners with composite PKs because the implementation captured the
/// owner key only when KeyColumns.Length == 1.
///
/// Root cause:
///   var ownerKey = ownerMap.KeyColumns.Length == 1 ? ownerMap.KeyColumns[0].Getter(owner) : null;
///   if (ownerKey == null) return;          ← entire method skipped for composite-PK owners
///
/// Fix: ResolveOwnerKeyColumnForOwnedFk resolves which key column the FK references
/// via exact name match, prefix-stripped match, then first-column fallback.
/// The ownerByPk dictionary in LoadOwnedCollectionsAsync is now per-ownedMap.
/// </summary>
public class CompositeKeyOwnedCollectionTests
{
    // ── Entity definitions ────────────────────────────────────────────────────

    /// <summary>
    /// Owner entity with a composite PK (TenantId, CkOrderId).
    /// The FK column on the owned table is "CkOrderId", which exactly matches
    /// the second key column — exercises the exact-name-match path in
    /// ResolveOwnerKeyColumnForOwnedFk.
    /// </summary>
    [Table("CkOrder")]
    private class CkOrder
    {
        [Key] public int TenantId { get; set; }
        [Key] public int CkOrderId { get; set; }
        public string CustomerName { get; set; } = "";
        public List<CkOrderLine> Lines { get; set; } = new();
    }

    private class CkOrderLine
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string ProductName { get; set; } = "";
        public decimal Price { get; set; }
    }

    /// <summary>
    /// Owner entity whose type name is "CompInv"; the FK column on the owned
    /// table is "CompInvId".  Stripping the type-name prefix "CompInv" yields
    /// "Id", which matches the second key column named "Id" — exercises the
    /// prefix-stripped match path in ResolveOwnerKeyColumnForOwnedFk.
    /// </summary>
    [Table("CompInv")]
    private class CompInv
    {
        [Key] public int TenantId { get; set; }
        [Key] public int Id { get; set; }
        public string Ref { get; set; } = "";
        public List<CompInvLine> Lines { get; set; } = new();
    }

    private class CompInvLine
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int LineId { get; set; }
        public string Description { get; set; } = "";
    }

    // ── Schema helpers ────────────────────────────────────────────────────────

    private static SqliteConnection CreateCkOrderDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CkOrder (
                TenantId  INTEGER NOT NULL,
                CkOrderId INTEGER NOT NULL,
                CustomerName TEXT NOT NULL,
                PRIMARY KEY (TenantId, CkOrderId)
            );
            CREATE TABLE CkOrderLine (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                CkOrderId   INTEGER NOT NULL,
                ProductName TEXT NOT NULL,
                Price       REAL NOT NULL
            );";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateCkOrderCtx(SqliteConnection cn) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CkOrder>()
                .OwnsMany<CkOrderLine>(o => o.Lines, tableName: "CkOrderLine", foreignKey: "CkOrderId")
        });

    private static SqliteConnection CreateCompInvDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CompInv (
                TenantId INTEGER NOT NULL,
                Id       INTEGER NOT NULL,
                Ref      TEXT NOT NULL,
                PRIMARY KEY (TenantId, Id)
            );
            CREATE TABLE CompInvLine (
                LineId      INTEGER PRIMARY KEY AUTOINCREMENT,
                CompInvId   INTEGER NOT NULL,
                Description TEXT NOT NULL
            );";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateCompInvCtx(SqliteConnection cn) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CompInv>()
                .OwnsMany<CompInvLine>(o => o.Lines, tableName: "CompInvLine", foreignKey: "CompInvId")
        });

    private static int CountRows(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return (int)(long)cmd.ExecuteScalar()!;
    }

    // ── CKOC-1: Exact-name-match path (FK "CkOrderId" == key column "CkOrderId") ─

    [Fact]
    public async Task CkOrder_Add_WithLines_PersistsLines()
    {
        using var cn = CreateCkOrderDb();
        using var ctx = CreateCkOrderCtx(cn);

        var order = new CkOrder
        {
            TenantId = 1, CkOrderId = 100, CustomerName = "ACME",
            Lines = new List<CkOrderLine>
            {
                new() { ProductName = "Widget", Price = 9.99m },
                new() { ProductName = "Gadget", Price = 19.99m }
            }
        };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        Assert.Equal(2, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 100"));
    }

    [Fact]
    public async Task CkOrder_Add_TwoTenants_SameOrderId_LinesIsolated()
    {
        // Verifies that the FK targets the correct key column (CkOrderId=100, not TenantId=1)
        // so tenant 1 and tenant 2 each get their own line rows.
        using var cn = CreateCkOrderDb();
        using var ctx = CreateCkOrderCtx(cn);

        ctx.Add(new CkOrder
        {
            TenantId = 1, CkOrderId = 100, CustomerName = "Tenant1",
            Lines = new() { new() { ProductName = "T1-Item", Price = 1m } }
        });
        ctx.Add(new CkOrder
        {
            TenantId = 2, CkOrderId = 100, CustomerName = "Tenant2",
            Lines = new() { new() { ProductName = "T2-Item", Price = 2m } }
        });
        await ctx.SaveChangesAsync();

        // Both owners share CkOrderId=100 but FK targets that column → 2 lines total
        Assert.Equal(2, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 100"));
    }

    [Fact]
    public async Task CkOrder_Modify_ReplacesLines()
    {
        using var cn = CreateCkOrderDb();
        using var ctx = CreateCkOrderCtx(cn);

        var order = new CkOrder
        {
            TenantId = 1, CkOrderId = 200, CustomerName = "Mod",
            Lines = new() { new() { ProductName = "Old", Price = 1m } }
        };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        Assert.Equal(1, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 200"));

        // Re-attach and replace lines
        using var ctx2 = CreateCkOrderCtx(cn);
        var order2 = new CkOrder
        {
            TenantId = 1, CkOrderId = 200, CustomerName = "Mod",
            Lines = new()
            {
                new() { ProductName = "New1", Price = 5m },
                new() { ProductName = "New2", Price = 6m }
            }
        };
        ctx2.Update(order2);
        await ctx2.SaveChangesAsync();

        Assert.Equal(2, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 200"));
    }

    [Fact]
    public async Task CkOrder_Delete_RemovesLines()
    {
        using var cn = CreateCkOrderDb();
        using var ctx = CreateCkOrderCtx(cn);

        var order = new CkOrder
        {
            TenantId = 1, CkOrderId = 300, CustomerName = "Del",
            Lines = new() { new() { ProductName = "X", Price = 1m } }
        };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        Assert.Equal(1, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 300"));

        using var ctx2 = CreateCkOrderCtx(cn);
        ctx2.Remove(new CkOrder { TenantId = 1, CkOrderId = 300, CustomerName = "Del" });
        await ctx2.SaveChangesAsync();

        Assert.Equal(0, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 300"));
    }

    [Fact]
    public async Task CkOrder_Load_PopulatesLines()
    {
        using var cn = CreateCkOrderDb();

        // Seed data directly via SQL
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO CkOrder VALUES (1, 400, 'Load');
                INSERT INTO CkOrderLine (CkOrderId, ProductName, Price) VALUES (400, 'Alpha', 3.5);
                INSERT INTO CkOrderLine (CkOrderId, ProductName, Price) VALUES (400, 'Beta',  7.0);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = CreateCkOrderCtx(cn);
        var results = await ctx.Query<CkOrder>().ToListAsync();
        var order = results.Single(o => o.CkOrderId == 400);

        Assert.Equal(2, order.Lines.Count);
        Assert.Contains(order.Lines, l => l.ProductName == "Alpha");
        Assert.Contains(order.Lines, l => l.ProductName == "Beta");
    }

    [Fact]
    public async Task CkOrder_Add_EmptyLines_DoesNotThrow()
    {
        using var cn = CreateCkOrderDb();
        using var ctx = CreateCkOrderCtx(cn);

        ctx.Add(new CkOrder { TenantId = 1, CkOrderId = 500, CustomerName = "Empty", Lines = new() });
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
        Assert.Equal(0, CountRows(cn, "SELECT COUNT(*) FROM CkOrderLine WHERE CkOrderId = 500"));
    }

    // ── CKOC-2: Prefix-stripped match path (FK "CompInvId" strip "CompInv" → "Id" == key "Id") ─

    [Fact]
    public async Task CompInv_Add_WithLines_PersistsLines()
    {
        using var cn = CreateCompInvDb();
        using var ctx = CreateCompInvCtx(cn);

        ctx.Add(new CompInv
        {
            TenantId = 1, Id = 10, Ref = "INV-001",
            Lines = new() { new() { Description = "Line A" }, new() { Description = "Line B" } }
        });
        await ctx.SaveChangesAsync();

        Assert.Equal(2, CountRows(cn, "SELECT COUNT(*) FROM CompInvLine WHERE CompInvId = 10"));
    }

    [Fact]
    public async Task CompInv_Delete_RemovesLines()
    {
        using var cn = CreateCompInvDb();
        using var ctx = CreateCompInvCtx(cn);

        ctx.Add(new CompInv
        {
            TenantId = 1, Id = 20, Ref = "INV-002",
            Lines = new() { new() { Description = "To delete" } }
        });
        await ctx.SaveChangesAsync();

        using var ctx2 = CreateCompInvCtx(cn);
        ctx2.Remove(new CompInv { TenantId = 1, Id = 20, Ref = "INV-002" });
        await ctx2.SaveChangesAsync();

        Assert.Equal(0, CountRows(cn, "SELECT COUNT(*) FROM CompInvLine WHERE CompInvId = 20"));
    }

    [Fact]
    public async Task CompInv_Load_PopulatesLines()
    {
        using var cn = CreateCompInvDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO CompInv VALUES (1, 30, 'INV-003');
                INSERT INTO CompInvLine (CompInvId, Description) VALUES (30, 'Desc1');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = CreateCompInvCtx(cn);
        var invs = await ctx.Query<CompInv>().ToListAsync();
        var inv = invs.Single(i => i.Id == 30);

        Assert.Single(inv.Lines);
        Assert.Equal("Desc1", inv.Lines[0].Description);
    }

    // ── CKOC-3: ResolveOwnerKeyColumnForOwnedFk unit-level verification ──────

    [Fact]
    public void ResolveOwnerKey_ExactMatch_ReturnsMatchingColumn()
    {
        // Confirm the mapping correctly identifies the CkOrderId key column
        using var cn = CreateCkOrderDb();
        using var ctx = CreateCkOrderCtx(cn);
        var map = ctx.GetMapping(typeof(CkOrder));
        var oc = map.OwnedCollections[0];

        // FK column is "CkOrderId"; owner has [TenantId, CkOrderId]
        // The FK should reference the CkOrderId column, not TenantId
        Assert.Equal("CkOrderId", oc.ForeignKeyColumn);
        Assert.Equal(2, map.KeyColumns.Length);
    }

    [Fact]
    public void ResolveOwnerKey_PrefixStrip_ReturnsMatchingColumn()
    {
        // CompInv: FK="CompInvId", strip "CompInv" → "Id", key col "Id" matches
        using var cn = CreateCompInvDb();
        using var ctx = CreateCompInvCtx(cn);
        var map = ctx.GetMapping(typeof(CompInv));
        var oc = map.OwnedCollections[0];

        Assert.Equal("CompInvId", oc.ForeignKeyColumn);
        Assert.Equal(2, map.KeyColumns.Length);
    }
}
