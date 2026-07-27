using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ThenInclude that CHAINS OFF a composite-key principal reference nav
/// (Line.Order[composite key] -> Order.Region) must load the second level, not silently drop it — the
/// sibling of the composite-key reference-Include case.
/// </summary>
[Trait("Category", "Fast")]
public class CompositeKeyThenIncludeTests
{
    [Table("CkTiRegion")]
    private sealed class Region { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("CkTiOrder")]
    private sealed class Ord
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public int RegionId { get; set; }
        public Region? Region { get; set; }
        public ICollection<Line> Lines { get; set; } = new List<Line>();
    }

    [Table("CkTiLine")]
    private sealed class Line
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public Ord? Order { get; set; }
    }

    private static DbContext Seed(SqliteConnection cn)
    {
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE CkTiRegion (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE CkTiOrder (TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, RegionId INTEGER NOT NULL, PRIMARY KEY(TenantId, OrderId));" +
                "CREATE TABLE CkTiLine (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL);" +
                "INSERT INTO CkTiRegion VALUES (7,'West'),(8,'East');" +
                "INSERT INTO CkTiOrder VALUES (1,100,7),(2,100,8);" +
                "INSERT INTO CkTiLine VALUES (1,1,100),(2,2,100);";
            c.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Ord>().HasKey(x => new { x.TenantId, x.OrderId });
                mb.Entity<Line>().HasKey(x => x.Id);
                mb.Entity<Region>().HasKey(x => x.Id);
                mb.Entity<Ord>()
                    .HasMany(o => o.Lines)
                    .WithOne(l => l.Order)
                    .HasForeignKey(l => new { l.TenantId, l.OrderId }, o => new { o.TenantId, o.OrderId });
            }
        };
        return new DbContext(cn, new SqliteProvider(), options, ownsConnection: false);
    }

    [Fact]
    public void ThenInclude_off_a_composite_key_principal_loads_the_second_level()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = Seed(cn);

        var lines = ((INormQueryable<Line>)ctx.Query<Line>())
            .Include(l => l.Order)
            .ThenInclude(o => o!.Region)
            .AsNoTracking()
            .OrderBy(l => l.Id)
            .ToList();

        Assert.Equal("West", lines.Single(l => l.Id == 1).Order?.Region?.Name);  // Order(1,100)->Region 7
        Assert.Equal("East", lines.Single(l => l.Id == 2).Order?.Region?.Name);  // Order(2,100)->Region 8
    }
}
