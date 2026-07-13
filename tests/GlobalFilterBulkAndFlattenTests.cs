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
/// Global filters gate bulk writes and collection flattening: ExecuteDelete and
/// ExecuteUpdate touch only visible rows, and SelectMany over a collection
/// navigation excludes filtered-out children in BOTH overloads — the result-selector
/// arm projects an unmapped shape the provider-level filter rewrite cannot wrap, so
/// the child's filter lives in the flatten join's ON clause.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterBulkAndFlattenTests
{
    [Table("GfBulk_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool Archived { get; set; }
        public int? Salary { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("GfBulk_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public bool Done { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GfBulk_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Archived INTEGER NOT NULL, Salary INTEGER NULL);
                CREATE TABLE GfBulk_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL, Done INTEGER NOT NULL);
                INSERT INTO GfBulk_Emp VALUES (1, 'ann', 0, 10), (2, 'bob', 1, 20);
                INSERT INTO GfBulk_Chore VALUES (1, 1, 'code', 0), (2, 1, 'ship', 1), (3, 2, 'ops', 0);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Emp>(e => !e.Archived);
        opts.AddGlobalFilter<Chore>(c => !c.Done);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Emp>().HasKey(e => e.Id);
            mb.Entity<Chore>().HasKey(c => c.Id);
            mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static object? Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    [Fact]
    public async Task execute_delete_respects_target_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Only visible (non-archived) rows may be deleted: bob must survive.
        var n = await ((INormQueryable<Emp>)ctx.Query<Emp>()).ExecuteDeleteAsync();
        Assert.Equal(1, n);
        Assert.Equal(1L, Scalar(cn, "SELECT COUNT(*) FROM GfBulk_Emp"));
        Assert.Equal("bob", Scalar(cn, "SELECT Name FROM GfBulk_Emp"));
    }

    [Fact]
    public async Task execute_update_respects_target_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var n = await ((INormQueryable<Emp>)ctx.Query<Emp>())
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Salary, e => 0));
        Assert.Equal(1, n);
        Assert.Equal(0L, Scalar(cn, "SELECT Salary FROM GfBulk_Emp WHERE Id = 1"));
        Assert.Equal(20L, Scalar(cn, "SELECT Salary FROM GfBulk_Emp WHERE Id = 2")); // archived: untouched
    }

    [Fact]
    public void selectmany_flatten_respects_child_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // ann is visible; her 'ship' chore is Done (filtered). bob is archived entirely.
        var whats = ctx.Query<Emp>().SelectMany(e => e.Chores).Select(c => c.What)
            .ToList().OrderBy(w => w).ToList();
        Assert.Equal(new[] { "code" }, whats);
    }

    [Fact]
    public void selectmany_result_selector_respects_child_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>().SelectMany(e => e.Chores, (e, c) => new { e.Name, c.What })
            .ToList().OrderBy(r => r.What).ToList();
        Assert.Single(rows);
        Assert.Equal("code", rows[0].What);
    }
}
