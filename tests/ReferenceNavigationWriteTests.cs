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
/// Writing a reference navigation must sync the underlying FK column. Setting
/// emp.Dept = otherDept without touching emp.DeptId, or clearing emp.Dept = null,
/// must update / null the FK on SaveChanges. A dropped fixup silently persists
/// the stale FK.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReferenceNavigationWriteTests
{
    [Table("RnwDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("RnwEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:rnw_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE RnwDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE RnwEmp (Id INTEGER PRIMARY KEY, DeptId INTEGER NULL, Name TEXT NOT NULL);
                INSERT INTO RnwDept VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO RnwEmp VALUES (1, 1, 'ann');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions { OnModelCreating = mb => { mb.Entity<Emp>(); mb.Entity<Dept>().HasKey(d => d.Id); } };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static int? ReadDeptId(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT DeptId FROM RnwEmp WHERE Id = 1";
        var v = cmd.ExecuteScalar();
        return v is DBNull or null ? null : Convert.ToInt32(v);
    }

    [Fact]
    public async Task Changing_reference_nav_updates_fk()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var emp = ctx.Query<Emp>().First(e => e.Id == 1);
        var ops = ctx.Query<Dept>().First(d => d.Id == 2);
        emp.Dept = ops; // does NOT touch emp.DeptId
        await ctx.SaveChangesAsync();

        Assert.Equal(2, ReadDeptId(keeper));
    }

    [Fact]
    public async Task Clearing_loaded_reference_nav_nulls_fk()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        // Load WITH the nav so emp.Dept is the Eng principal, then clear it. This
        // is the unambiguous "disassociate" case (distinct from a never-loaded
        // null nav, where keeping the valid FK is correct).
        var emp = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!).First(e => e.Id == 1);
        Assert.NotNull(emp.Dept);
        emp.Dept = null;
        await ctx.SaveChangesAsync();

        Assert.Null(ReadDeptId(keeper));
    }

    [Fact]
    public async Task Unloaded_null_reference_nav_keeps_valid_fk()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        // Loaded WITHOUT the nav: emp.Dept is null because it was never loaded,
        // not cleared. An unrelated edit must NOT null the valid FK.
        var emp = ctx.Query<Emp>().First(e => e.Id == 1);
        Assert.Null(emp.Dept);
        emp.Name = "renamed";
        await ctx.SaveChangesAsync();

        Assert.Equal(1, ReadDeptId(keeper)); // FK preserved
    }

    [Fact]
    public async Task Setting_fk_scalar_directly_still_persists()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var emp = ctx.Query<Emp>().First(e => e.Id == 1);
        emp.DeptId = 2; // the scalar path
        await ctx.SaveChangesAsync();

        Assert.Equal(2, ReadDeptId(keeper));
    }
}
