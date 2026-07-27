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
/// Clearing a loaded reference navigation to an OPTIONAL composite-key principal (dependent.Principal = null)
/// must disassociate by nulling every FK component, the same way the single-key path does. Reference-nav
/// fixup bailed entirely for composite-key principals on the null (disassociation) case, so setting the nav
/// to null was silently ignored and the row kept its old link. A direct FK-scalar null still worked, but the
/// nav-based disassociation diverged from EF.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompositeKeyReferenceNavDisassociationTests
{
    [Table("CkdOrder")]
    private sealed class Ord
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Customer { get; set; } = "";
        public ICollection<Line> Lines { get; set; } = new List<Line>();
    }

    [Table("CkdLine")]
    private sealed class Line
    {
        [Key] public int Id { get; set; }
        public int? TenantId { get; set; }  // optional (nullable) composite FK
        public int? OrderId { get; set; }
        public string Description { get; set; } = "";
        public Ord? Order { get; set; }
    }

    private static DbContext Seed(SqliteConnection cn)
    {
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE CkdOrder (TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Customer TEXT NOT NULL, PRIMARY KEY(TenantId, OrderId));" +
                "CREATE TABLE CkdLine (Id INTEGER PRIMARY KEY, TenantId INTEGER NULL, OrderId INTEGER NULL, Description TEXT NOT NULL);" +
                "INSERT INTO CkdOrder VALUES (2,100,'Bob');" +
                "INSERT INTO CkdLine VALUES (10, 2, 100, 'x');";
            c.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
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
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static (long? TenantId, long? OrderId) FkOf(SqliteConnection cn, int lineId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT TenantId, OrderId FROM CkdLine WHERE Id = {lineId}";
        using var r = cmd.ExecuteReader();
        r.Read();
        long? t = r.IsDBNull(0) ? null : r.GetInt64(0);
        long? o = r.IsDBNull(1) ? null : r.GetInt64(1);
        return (t, o);
    }

    [Fact]
    public async Task Clearing_reference_nav_to_optional_composite_principal_nulls_the_fk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = Seed(cn);

        var line = await ((INormQueryable<Line>)ctx.Query<Line>()).Include(l => l.Order).FirstAsync(l => l.Id == 10);
        Assert.NotNull(line.Order);          // loaded non-null

        line.Order = null;                   // disassociate via the navigation
        await ctx.SaveChangesAsync();

        Assert.Equal((null, null), FkOf(cn, 10));   // BUG: (2, 100) — composite disassociation was a no-op
    }
}
