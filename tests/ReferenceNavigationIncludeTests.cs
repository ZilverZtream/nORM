using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Include() on a REFERENCE navigation (dependent → principal, e.g. Order.Customer)
/// eager-loads the principal. Principals are fetched by primary key from the parents'
/// FK values: a NULL FK and a dangling FK both leave the navigation null, and parents
/// sharing a principal share the same tracked instance. Chains work in both
/// directions: reference→reference and collection→reference.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReferenceNavigationIncludeTests
{
    [Table("RefInc_Region")]
    private class Region
    {
        [Key] public int Id { get; set; }
        public string Zone { get; set; } = "";
    }

    [Table("RefInc_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public int? RegionId { get; set; }
        [ForeignKey(nameof(RegionId))] public Region? Region { get; set; }
    }

    [Table("RefInc_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public int? BackupDeptId { get; set; }
        [ForeignKey(nameof(BackupDeptId))] public Dept? BackupDept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("RefInc_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE RefInc_Region (Id INTEGER PRIMARY KEY, Zone TEXT NOT NULL);
                CREATE TABLE RefInc_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, RegionId INTEGER NULL);
                CREATE TABLE RefInc_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL, BackupDeptId INTEGER NULL);
                CREATE TABLE RefInc_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO RefInc_Region VALUES (10, 'EU');
                INSERT INTO RefInc_Dept VALUES (1, 'Eng', 10), (2, 'Ops', NULL);
                INSERT INTO RefInc_Emp VALUES (1, 'ann', 1, 2), (2, 'bob', NULL, NULL), (3, 'cid', 1, NULL), (4, 'dan', 99, NULL);
                INSERT INTO RefInc_Chore VALUES (1, 1, 'code', 2), (2, 1, 'ship', NULL), (3, 3, 'ops', 1);
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
                mb.Entity<Chore>().HasKey(c => c.Id);
                mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
            }
        });
    }

    [Fact]
    public void Include_reference_nav_loads_principal_shares_instance_and_nulls_orphans()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!).AsSplitQuery().ToList()
            .OrderBy(e => e.Id).ToList();
        Assert.Equal("Eng", emps[0].Dept?.Title);
        Assert.Null(emps[1].Dept);                        // NULL FK
        Assert.Same(emps[0].Dept, emps[2].Dept);          // shared tracked principal
        Assert.Null(emps[3].Dept);                        // FK 99 has no principal row
    }

    [Fact]
    public void Include_reference_then_reference_chain_loads_grandparent()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!).ThenInclude(d => d.Region!)
            .AsSplitQuery().ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal("EU", emps[0].Dept?.Region?.Zone);
        Assert.Null(emps[1].Dept);
    }

    [Fact]
    public void Include_collection_then_reference_chain_loads_child_principals()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Chores).ThenInclude(c => c.Dept!)
            .AsSplitQuery().ToList().OrderBy(e => e.Id).ToList();
        var annChores = emps[0].Chores.OrderBy(c => c.Id).ToList();
        Assert.Equal(2, annChores.Count);
        Assert.Equal("Ops", annChores[0].Dept?.Title);
        Assert.Null(annChores[1].Dept);
        Assert.Equal("Eng", emps[2].Chores.Single().Dept?.Title);
    }

    [Fact]
    public void Include_two_reference_navs_to_same_principal_type_use_their_own_fk()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)((INormQueryable<Emp>)ctx.Query<Emp>())
            .Include(e => e.Dept!))
            .Include(e => e.BackupDept!).AsSplitQuery().ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal("Eng", emps[0].Dept?.Title);
        Assert.Equal("Ops", emps[0].BackupDept?.Title);
        Assert.Null(emps[2].BackupDept);
    }
}
