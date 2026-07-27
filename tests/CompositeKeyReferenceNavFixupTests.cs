using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Assigning a reference navigation to a composite-key principal (dependent.Principal = principal, without
/// setting the FK columns) must propagate every FK component — the same way the collection direction does.
/// Reference-nav fixup bailed entirely for composite-key principals, so the composite FK was never set and
/// the dependent was persisted with default (0/null) FK components: an orphan on SQLite, an FK violation
/// elsewhere.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompositeKeyReferenceNavFixupTests
{
    [Table("CkfxOrder")]
    private sealed class Ord
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Customer { get; set; } = "";
        public ICollection<Line> Lines { get; set; } = new List<Line>();
    }

    [Table("CkfxLine")]
    private sealed class Line
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Description { get; set; } = "";
        public Ord? Order { get; set; }
    }

    private static DbContext Seed(SqliteConnection cn)
    {
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE CkfxOrder (TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Customer TEXT NOT NULL, PRIMARY KEY(TenantId, OrderId));" +
                "CREATE TABLE CkfxLine (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Description TEXT NOT NULL);" +
                "INSERT INTO CkfxOrder VALUES (1,100,'Alice'),(2,100,'Bob');";
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

    private static (int TenantId, int OrderId) FkOf(SqliteConnection cn, int lineId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT TenantId, OrderId FROM CkfxLine WHERE Id = {lineId}";
        using var r = cmd.ExecuteReader();
        r.Read();
        return (r.GetInt32(0), r.GetInt32(1));
    }

    [Fact]
    public async Task Setting_reference_nav_to_composite_principal_propagates_the_composite_fk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = Seed(cn);

        var order = await ctx.Query<Ord>().FirstAsync(o => o.TenantId == 2 && o.OrderId == 100);
        var line = new Line { Id = 10, Description = "new" };   // FK components unset
        line.Order = order;                                     // navigation only
        ctx.Add(line);
        await ctx.SaveChangesAsync();

        Assert.Equal((2, 100), FkOf(cn, 10));   // BUG: (0, 0) — composite fixup bailed
    }
}
