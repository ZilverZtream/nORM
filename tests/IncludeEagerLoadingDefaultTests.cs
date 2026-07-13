using System;
using System.Collections.Generic;
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
/// Include() by itself eager-loads the requested navigations. Split-query loading is
/// the engine's loading strategy, not an opt-in: requiring AsSplitQuery() left every
/// navigation silently null for callers who didn't know the extra call was needed.
/// AsSplitQuery() remains supported as an explicit no-op.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IncludeEagerLoadingDefaultTests
{
    [Table("IncDef_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("IncDef_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("IncDef_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IncDef_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE IncDef_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE IncDef_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL);
                INSERT INTO IncDef_Dept VALUES (1, 'Eng');
                INSERT INTO IncDef_Emp VALUES (1, 'ann', 1), (2, 'bob', NULL);
                INSERT INTO IncDef_Chore VALUES (1, 1, 'code'), (2, 1, 'ship');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Chore>().HasKey(c => c.Id);
                mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
            }
        });
    }

    [Fact]
    public void Include_collection_without_split_query_loads_children()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Chores)
            .ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal(2, emps[0].Chores.Count);
        Assert.Empty(emps[1].Chores);
    }

    [Fact]
    public void Include_reference_without_split_query_loads_principal()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!)
            .ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal("Eng", emps[0].Dept?.Title);
        Assert.Null(emps[1].Dept);
    }

    [Fact]
    public async Task Include_without_split_query_loads_on_async_and_single_result_paths()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = await ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Chores).ToListAsync();
        Assert.Equal(2, emps.Single(e => e.Id == 1).Chores.Count);

        var first = ((INormQueryable<Emp>)ctx.Query<Emp>().Where(e => e.Id == 1)).Include(e => e.Dept!)
            .ToList().Single();
        Assert.Equal("Eng", first.Dept?.Title);
    }

    [Fact]
    public void AsSplitQuery_remains_a_supported_explicit_call()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Chores).AsSplitQuery()
            .ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal(2, emps[0].Chores.Count);
    }
}
