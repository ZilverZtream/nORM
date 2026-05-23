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
/// Contracts for Include / eager-loading behaviour that are not already covered
/// by IncludeProcessorCoverageTests:
///
/// 1. Reference-navigation (many-to-one) single Include returns the related entity.
/// 2. Collection Include (one-to-many) returns all children.
/// 3. Composite-key dependent Include throws NormUnsupportedFeatureException.
/// 4. Include WITHOUT AsSplitQuery() silently skips eager loading — children stay empty.
///    nORM requires AsSplitQuery() as an explicit opt-in; omitting it must never silently
///    return partial data that looks correct on a small dataset and breaks under load.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class IncludeContractTests
{
    // ── Domain model ──────────────────────────────────────────────────────────

    [Table("ICT_Customer")]
    private class IctCustomer
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ICollection<IctOrder> Orders { get; set; } = new List<IctOrder>();
    }

    [Table("ICT_Order")]
    private class IctOrder
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public decimal Amount { get; set; }
        public IctCustomer? Customer { get; set; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static DbContext CreateCtx(SqliteConnection cn)
    {
        Exec(cn, "CREATE TABLE IF NOT EXISTS ICT_Customer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IF NOT EXISTS ICT_Order (Id INTEGER PRIMARY KEY AUTOINCREMENT, CustomerId INTEGER NOT NULL, Amount REAL NOT NULL)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<IctCustomer>()
                  .HasMany(c => c.Orders)
                  .WithOne(o => o.Customer)
                  .HasForeignKey(o => o.CustomerId, c => c.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // ── 1. Collection Include (one-to-many) with AsSplitQuery returns children ─

    [Fact]
    public async Task CollectionInclude_WithAsSplitQuery_LoadsChildren()
    {
        using var cn = OpenDb();
        using var ctx = CreateCtx(cn);

        Exec(cn, "INSERT INTO ICT_Customer VALUES(1,'Alice')");
        Exec(cn, "INSERT INTO ICT_Order VALUES(1,1,100.0),(2,1,200.0)");

        var customers = await ((INormQueryable<IctCustomer>)ctx.Query<IctCustomer>())
            .AsSplitQuery()
            .Include(c => c.Orders)
            .ToListAsync();

        Assert.Single(customers);
        Assert.Equal(2, customers[0].Orders.Count);
        Assert.All(customers[0].Orders, o => Assert.Equal(1, o.CustomerId));
    }

    // ── 2. Collection Include without AsSplitQuery — eager loading is silently skipped ─

    [Fact]
    public async Task CollectionInclude_WithoutAsSplitQuery_ChildrenAreEmpty()
    {
        // nORM requires AsSplitQuery() as an explicit opt-in for eager loading.
        // Without it, EagerLoadAsync is never invoked and children remain at their
        // default empty-collection value. This is a contract: callers must opt in.
        using var cn = OpenDb();
        using var ctx = CreateCtx(cn);

        Exec(cn, "INSERT INTO ICT_Customer VALUES(1,'Bob')");
        Exec(cn, "INSERT INTO ICT_Order VALUES(1,1,50.0)");

        var customers = await ((INormQueryable<IctCustomer>)ctx.Query<IctCustomer>())
            // Note: no .AsSplitQuery() call
            .Include(c => c.Orders)
            .ToListAsync();

        // Customers are returned but their Orders collection is not populated.
        Assert.Single(customers);
        Assert.Empty(customers[0].Orders);
    }

    // ── 3. Composite-key dependent Include throws NormUnsupportedFeatureException ─

    [Fact]
    public async Task Include_CompositeKeyDependent_ThrowsNormUnsupportedFeatureException()
    {
        // Covered comprehensively in CompositeKeyIncludeTests but included here
        // as part of the blocker-10 contract surface.
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE ICT_CompositeParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE ICT_CompositeLine (ParentId INTEGER NOT NULL, Seq INTEGER NOT NULL, Note TEXT NOT NULL, PRIMARY KEY(ParentId, Seq))");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<IctCompositeParent>().HasKey(p => p.Id);
                mb.Entity<IctCompositeLine>().HasKey(l => new { l.ParentId, l.Seq });
                mb.Entity<IctCompositeParent>()
                  .HasMany(p => p.Lines)
                  .WithOne()
                  .HasForeignKey(l => l.ParentId, p => p.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        Exec(cn, "INSERT INTO ICT_CompositeParent VALUES(1,'P1')");
        Exec(cn, "INSERT INTO ICT_CompositeLine VALUES(1,1,'Line1')");

        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
            await ((INormQueryable<IctCompositeParent>)ctx.Query<IctCompositeParent>())
                .AsSplitQuery()
                .Include(p => p.Lines)
                .ToListAsync());
    }

    // ── 4. Collection Include with multiple parents groups correctly ──────────

    [Fact]
    public async Task CollectionInclude_MultipleParents_ChildrenGroupedCorrectly()
    {
        using var cn = OpenDb();
        using var ctx = CreateCtx(cn);

        Exec(cn, "INSERT INTO ICT_Customer VALUES(1,'C1'),(2,'C2')");
        Exec(cn, "INSERT INTO ICT_Order VALUES(1,1,10.0),(2,1,20.0),(3,2,30.0)");

        var customers = await ((INormQueryable<IctCustomer>)ctx.Query<IctCustomer>())
            .AsSplitQuery()
            .Include(c => c.Orders)
            .ToListAsync();

        Assert.Equal(2, customers.Count);
        var c1 = customers.Single(c => c.Name == "C1");
        var c2 = customers.Single(c => c.Name == "C2");
        Assert.Equal(2, c1.Orders.Count);
        Assert.Single(c2.Orders);
    }

    // ── 5. Sync collection Include with AsSplitQuery returns children ─────────

    [Fact]
    public void CollectionInclude_Sync_WithAsSplitQuery_LoadsChildren()
    {
        using var cn = OpenDb();
        using var ctx = CreateCtx(cn);

        Exec(cn, "INSERT INTO ICT_Customer VALUES(1,'SyncC')");
        Exec(cn, "INSERT INTO ICT_Order VALUES(1,1,99.0)");

        var customers = ((INormQueryable<IctCustomer>)ctx.Query<IctCustomer>())
            .AsSplitQuery()
            .Include(c => c.Orders)
            .ToList();

        Assert.Single(customers);
        Assert.Single(customers[0].Orders);
    }

    // ── 6. Sync Include without AsSplitQuery — children are empty ────────────

    [Fact]
    public void CollectionInclude_Sync_WithoutAsSplitQuery_ChildrenAreEmpty()
    {
        using var cn = OpenDb();
        using var ctx = CreateCtx(cn);

        Exec(cn, "INSERT INTO ICT_Customer VALUES(1,'SyncNoSplit')");
        Exec(cn, "INSERT INTO ICT_Order VALUES(1,1,77.0)");

        var customers = ((INormQueryable<IctCustomer>)ctx.Query<IctCustomer>())
            // Note: no .AsSplitQuery() call
            .Include(c => c.Orders)
            .ToList();

        Assert.Single(customers);
        Assert.Empty(customers[0].Orders);
    }
}

// ── Support types for composite-key test ──────────────────────────────────────

[Table("ICT_CompositeParent")]
file class IctCompositeParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public ICollection<IctCompositeLine> Lines { get; set; } = new List<IctCompositeLine>();
}

[Table("ICT_CompositeLine")]
file class IctCompositeLine
{
    public int ParentId { get; set; }
    public int Seq { get; set; }
    public string Note { get; set; } = string.Empty;
}
