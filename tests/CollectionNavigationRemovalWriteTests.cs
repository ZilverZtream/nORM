using System;
using System.Collections.Generic;
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
/// Removing a child from a LOADED one-to-many collection navigation must sever the
/// child's optional foreign key (set it null) on SaveChanges — the collection-side
/// mirror of clearing a reference navigation. A dropped fixup silently leaves the
/// removed child still pointing at its old principal.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CollectionNavigationRemovalWriteTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CnrDept")]
    public class Dept
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Emp> Employees { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CnrEmp")]
    public class Emp
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int? DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:cnr_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CnrDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CnrEmp (Id INTEGER PRIMARY KEY, DeptId INTEGER NULL, Name TEXT NOT NULL);
                INSERT INTO CnrDept VALUES (1, 'Eng');
                INSERT INTO CnrEmp VALUES (1, 1, 'ann'), (2, 1, 'bob');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Dept>().HasKey(d => d.Id);
                    mb.Entity<Emp>().HasKey(e => e.Id);
                    mb.Entity<Dept>().HasMany(d => d.Employees).WithOne()
                                     .HasForeignKey(e => e.DeptId!, d => d.Id);
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static int? ReadDeptId(SqliteConnection k, int empId)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = $"SELECT DeptId FROM CnrEmp WHERE Id = {empId}";
        var v = cmd.ExecuteScalar();
        return v is DBNull or null ? null : Convert.ToInt32(v);
    }

    [Fact]
    public async Task Removing_child_from_loaded_collection_nulls_fk()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var dept = ((INormQueryable<Dept>)ctx.Query<Dept>()).Include(d => d.Employees).First(d => d.Id == 1);
        Assert.Equal(2, dept.Employees.Count);

        var ann = dept.Employees.First(e => e.Id == 1);
        dept.Employees.Remove(ann);
        await ctx.SaveChangesAsync();

        Assert.Null(ReadDeptId(keeper, 1));   // ann severed
        Assert.Equal(1, ReadDeptId(keeper, 2)); // bob untouched
    }

    [Fact]
    public async Task Unloaded_collection_does_not_sever_existing_children()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        // Loaded WITHOUT Include: Employees is empty because it was never loaded,
        // not emptied. An unrelated edit must NOT sever the existing children.
        var dept = ctx.Query<Dept>().First(d => d.Id == 1);
        Assert.Empty(dept.Employees);
        dept.Name = "Engineering";
        await ctx.SaveChangesAsync();

        Assert.Equal(1, ReadDeptId(keeper, 1));
        Assert.Equal(1, ReadDeptId(keeper, 2));
    }

    [Fact]
    public async Task Clearing_whole_loaded_collection_severs_all()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var dept = ((INormQueryable<Dept>)ctx.Query<Dept>()).Include(d => d.Employees).First(d => d.Id == 1);
        Assert.Equal(2, dept.Employees.Count);
        dept.Employees.Clear();
        await ctx.SaveChangesAsync();

        Assert.Null(ReadDeptId(keeper, 1));
        Assert.Null(ReadDeptId(keeper, 2));
    }

    [Fact]
    public async Task Second_save_after_removal_does_not_resever_or_touch_others()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var dept = ((INormQueryable<Dept>)ctx.Query<Dept>()).Include(d => d.Employees).First(d => d.Id == 1);
        var ann = dept.Employees.First(e => e.Id == 1);
        dept.Employees.Remove(ann);
        await ctx.SaveChangesAsync();
        Assert.Null(ReadDeptId(keeper, 1));

        // Re-link ann OUT of band, then an unrelated edit + save must NOT re-sever her:
        // the baseline was refreshed on the first save, so bob (still present) is untouched.
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "UPDATE CnrEmp SET DeptId = 1 WHERE Id = 1";
            cmd.ExecuteNonQuery();
        }
        dept.Name = "Engineering";
        await ctx.SaveChangesAsync();

        Assert.Equal(1, ReadDeptId(keeper, 1)); // ann NOT re-severed by the stale snapshot
        Assert.Equal(1, ReadDeptId(keeper, 2)); // bob untouched throughout
    }

    // ---- Required (non-nullable FK) relationship: removal deletes the orphan ----

    [System.ComponentModel.DataAnnotations.Schema.Table("CnrReqOrder")]
    public class Order
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CnrReqLine")]
    public class Line
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int OrderId { get; set; } // required (non-nullable)
        public string Sku { get; set; } = "";
    }

    [Fact]
    public async Task Removing_child_from_required_collection_deletes_orphan()
    {
        var keeper = new SqliteConnection($"Data Source=file:cnrreq_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using var _ = keeper;
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CnrReqOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE CnrReqLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO CnrReqOrder VALUES (1, 'o1');
                INSERT INTO CnrReqLine VALUES (1, 1, 'a'), (2, 1, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Order>().HasKey(o => o.Id);
                    mb.Entity<Line>().HasKey(l => l.Id);
                    mb.Entity<Order>().HasMany(o => o.Lines).WithOne()
                                      .HasForeignKey(l => l.OrderId, o => o.Id);
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }

        await using var ctx = Make();
        var order = ((INormQueryable<Order>)ctx.Query<Order>()).Include(o => o.Lines).First(o => o.Id == 1);
        Assert.Equal(2, order.Lines.Count);
        var a = order.Lines.First(l => l.Id == 1);
        order.Lines.Remove(a);
        await ctx.SaveChangesAsync();

        int CountLine(int id)
        {
            using var cmd = keeper.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM CnrReqLine WHERE Id = {id}";
            return Convert.ToInt32(cmd.ExecuteScalar());
        }
        Assert.Equal(0, CountLine(1)); // orphan of a required relationship is deleted
        Assert.Equal(1, CountLine(2)); // sibling untouched
    }
}
