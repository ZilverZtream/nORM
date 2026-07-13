using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SaveChanges honours reference-navigation assignments (dependent.Principal = entity):
/// the FK scalar is aligned with the principal's key, an untracked principal is
/// discovered and inserted first (principal-before-dependent order), and a principal
/// with a DB-generated key links up after its INSERT hydrates the key. A deliberately
/// edited FK scalar outranks a stale navigation reference, and a merely-loaded
/// navigation never marks a row modified. A null navigation is a no-op: plain queries
/// leave navigations null while the FK holds a value, so severing is done by clearing
/// the FK scalar.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReferenceNavigationSaveChangesTests
{
    [Table("RefSave_Region")]
    private class Region
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Zone { get; set; } = "";
    }

    [Table("RefSave_Dept")]
    private class Dept
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public int? RegionId { get; set; }
        [ForeignKey(nameof(RegionId))] public Region? Region { get; set; }
    }

    [Table("RefSave_Emp")]
    private class Emp
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public int? BackupDeptId { get; set; }
        [ForeignKey(nameof(BackupDeptId))] public Dept? BackupDept { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE RefSave_Region (Id INTEGER PRIMARY KEY AUTOINCREMENT, Zone TEXT NOT NULL);
                CREATE TABLE RefSave_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL, RegionId INTEGER NULL);
                CREATE TABLE RefSave_Emp (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, DeptId INTEGER NULL, BackupDeptId INTEGER NULL);
                INSERT INTO RefSave_Dept (Id, Title) VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO RefSave_Emp (Id, Name, DeptId) VALUES (1, 'ann', 1);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Region>().HasKey(r => r.Id);
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
            }
        });
    }

    private static object? Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    [Fact]
    public async Task Reassigning_nav_to_tracked_principal_updates_fk()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ctx.Query<Emp>().Single(e => e.Id == 1);
        var ops = ctx.Query<Dept>().Single(d => d.Id == 2);
        emp.Dept = ops;
        await ctx.SaveChangesAsync();
        Assert.Equal(2L, Scalar(cn, "SELECT DeptId FROM RefSave_Emp WHERE Id = 1"));
    }

    [Fact]
    public async Task Existing_dependent_assigned_new_dbgen_principal_links_after_insert()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ctx.Query<Emp>().Single(e => e.Id == 1);
        emp.Dept = new Dept { Title = "R&D" };
        await ctx.SaveChangesAsync();
        Assert.Equal("R&D", Scalar(cn, "SELECT d.Title FROM RefSave_Emp e JOIN RefSave_Dept d ON d.Id = e.DeptId WHERE e.Id = 1"));
    }

    [Fact]
    public async Task Graph_add_discovers_principal_chain_and_links_generated_keys()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = new Emp { Name = "eve", Dept = new Dept { Title = "Lab", Region = new Region { Zone = "EU" } } };
        ctx.Add(emp);
        await ctx.SaveChangesAsync();
        Assert.Equal("Lab:EU", Scalar(cn,
            "SELECT d.Title || ':' || r.Zone FROM RefSave_Emp e JOIN RefSave_Dept d ON d.Id = e.DeptId JOIN RefSave_Region r ON r.Id = d.RegionId WHERE e.Name = 'eve'"));
    }

    [Fact]
    public async Task Deliberate_fk_edit_outranks_stale_navigation()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!).AsSplitQuery().ToList().Single(e => e.Id == 1);
        Assert.NotNull(emp.Dept); // navigation still points at Dept 1
        emp.DeptId = 2;           // the scalar edit is the deliberate signal
        await ctx.SaveChangesAsync();
        Assert.Equal(2L, Scalar(cn, "SELECT DeptId FROM RefSave_Emp WHERE Id = 1"));
    }

    [Fact]
    public async Task Two_navs_to_same_principal_type_each_write_their_own_fk()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ctx.Query<Emp>().Single(e => e.Id == 1);
        var ops = ctx.Query<Dept>().Single(d => d.Id == 2);
        emp.BackupDept = ops;
        await ctx.SaveChangesAsync();
        Assert.Equal("1:2", Scalar(cn, "SELECT DeptId || ':' || BackupDeptId FROM RefSave_Emp WHERE Id = 1"));
    }

    [Fact]
    public async Task Loaded_navigation_alone_does_not_mark_row_modified()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!).AsSplitQuery().ToList().Single(e => e.Id == 1);
        Assert.NotNull(emp.Dept);
        Assert.Equal(0, await ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Null_navigation_with_populated_fk_is_not_a_sever()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ctx.Query<Emp>().Single(e => e.Id == 1); // plain query: Dept is null, DeptId = 1
        Assert.Null(emp.Dept);
        emp.Name = "ann2";
        await ctx.SaveChangesAsync();
        Assert.Equal(1L, Scalar(cn, "SELECT DeptId FROM RefSave_Emp WHERE Id = 1"));
    }
}
