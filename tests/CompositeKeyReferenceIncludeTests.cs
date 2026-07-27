using System;
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
/// Include of a REFERENCE navigation whose PRINCIPAL has a composite key
/// (dependent -> principal, e.g. Line.Order where Order's key is {TenantId, OrderId}).
/// Composite-key dependents (the collection direction) are already supported; the reference
/// direction must load the principal by matching all FK columns — not silently return null.
/// </summary>
[Trait("Category", "Fast")]
public class CompositeKeyReferenceIncludeTests
{
    [Table("CkRefOrder")]
    private sealed class Ord
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Customer { get; set; } = "";
        public ICollection<Line> Lines { get; set; } = new List<Line>();
    }

    [Table("CkRefLine")]
    private sealed class Line
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Description { get; set; } = "";
        public Ord? Order { get; set; }   // reference nav to the composite-key principal
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Ord>().HasKey(x => new { x.TenantId, x.OrderId });
            mb.Entity<Line>().HasKey(x => x.Id);
            mb.Entity<Ord>()
                .HasMany(o => o.Lines)
                .WithOne(l => l.Order)
                .HasForeignKey(l => new { l.TenantId, l.OrderId }, o => new { o.TenantId, o.OrderId });
        }
    };

    private static DbContext Seed(SqliteConnection cn)
    {
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE CkRefOrder (TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Customer TEXT NOT NULL, PRIMARY KEY(TenantId, OrderId));" +
                "CREATE TABLE CkRefLine (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Description TEXT NOT NULL);" +
                "INSERT INTO CkRefOrder VALUES (1,100,'Alice'),(2,100,'Bob'),(1,101,'Cara');" +
                "INSERT INTO CkRefLine VALUES (1,1,100,'a'),(2,2,100,'b'),(3,1,101,'c');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);
    }

    [Fact]
    public void Include_reference_nav_to_composite_key_principal_loads_the_principal()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = Seed(cn);

        var lines = ((INormQueryable<Line>)ctx.Query<Line>())
            .Include(l => l.Order)
            .AsNoTracking()
            .OrderBy(l => l.Id)
            .ToList();

        Assert.Equal(3, lines.Count);
        // Each line's composite-key principal must be loaded and matched on BOTH key columns.
        Assert.Equal("Alice", lines.Single(l => l.Id == 1).Order?.Customer);  // (1,100)
        Assert.Equal("Bob", lines.Single(l => l.Id == 2).Order?.Customer);    // (2,100) — different tenant, same OrderId
        Assert.Equal("Cara", lines.Single(l => l.Id == 3).Order?.Customer);   // (1,101)
    }
}
