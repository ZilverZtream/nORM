using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A lazy reference navigation on the DEPENDENT side of a many-to-one (the entity holds the FK and points at
/// its principal — `child.Parent`, the most common direction) must load the correct principal. The lazy and
/// explicit load paths only handled the principal→dependent direction, so a `LazyNavigationReference&lt;TPrincipal&gt;`
/// on the FK-holder silently loaded nothing, marked the nav loaded, and returned null even though the
/// principal row exists.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LazyReferenceDependentToPrincipalTests
{
    [Table("LrpParent")]
    public class LrpParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public ICollection<LrpChild>? Children { get; set; }
    }

    [Table("LrpChild")]
    public class LrpChild
    {
        [Key] public int Id { get; set; }
        public int LrpParentId { get; set; }
        public string Sku { get; set; } = "";
        public LazyNavigationReference<LrpParent>? Parent { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE LrpParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "CREATE TABLE LrpChild (Id INTEGER PRIMARY KEY, LrpParentId INTEGER NOT NULL, Sku TEXT NOT NULL);" +
                              "INSERT INTO LrpParent VALUES (1,'p1'),(2,'p2');" +
                              "INSERT INTO LrpChild VALUES (1,1,'a'),(2,1,'b'),(3,2,'c');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<LrpParent>().HasKey(p => p.Id)
                .HasMany(p => p.Children!).WithOne().HasForeignKey(c => c.LrpParentId, p => p.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Lazy_reference_dependent_to_principal_loads_correct_parent()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var child = ctx.Query<LrpChild>().First(c => c.Id == 1);
        Assert.NotNull(child.Parent);
        var parent = await child.Parent!.GetValueAsync();
        Assert.NotNull(parent);            // ZZParent row 1 exists
        Assert.Equal(1, parent!.Id);
        Assert.Equal("p1", parent.Name);
    }

    [Fact]
    public async Task Lazy_reference_dependent_to_principal_distinct_parents()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var c3 = ctx.Query<LrpChild>().First(c => c.Id == 3);
        var p3 = await c3.Parent!.GetValueAsync();
        Assert.NotNull(p3);
        Assert.Equal(2, p3!.Id);
        Assert.Equal("p2", p3.Name);
    }
}
