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
/// ExecuteDelete/ExecuteUpdate honour reference-navigation predicates
/// (Where(e => e.Dept.Title == ...)): the correlated scalar subquery flows into the
/// bulk statement's WHERE and touches exactly the matching rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationBulkPredicateTests
{
    [Table("NavBulk_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("NavBulk_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? Salary { get; set; }
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
                CREATE TABLE NavBulk_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE NavBulk_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Salary INTEGER NULL, DeptId INTEGER NULL);
                INSERT INTO NavBulk_Dept VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO NavBulk_Emp VALUES (1, 'ann', 10, 1), (2, 'bob', 20, 2), (3, 'cid', 30, NULL), (4, 'dan', 40, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Dept>().HasKey(d => d.Id); mb.Entity<Emp>().HasKey(e => e.Id); }
        });
    }

    private static object? Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    [Fact]
    public async Task ExecuteDelete_with_nav_predicate_removes_exactly_matching_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var n = await ((INormQueryable<Emp>)ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng")).ExecuteDeleteAsync();
        Assert.Equal(2, n);
        Assert.Equal("2:3", Scalar(cn, "SELECT GROUP_CONCAT(Id, ':') FROM NavBulk_Emp ORDER BY Id"));
    }

    [Fact]
    public async Task ExecuteUpdate_with_nav_predicate_updates_exactly_matching_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var n = await ((INormQueryable<Emp>)ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng"))
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Salary, e => e.Salary + 1));
        Assert.Equal(2, n);
        Assert.Equal(11L, Scalar(cn, "SELECT Salary FROM NavBulk_Emp WHERE Id = 1"));
        Assert.Equal(20L, Scalar(cn, "SELECT Salary FROM NavBulk_Emp WHERE Id = 2"));
        Assert.Equal(30L, Scalar(cn, "SELECT Salary FROM NavBulk_Emp WHERE Id = 3"));
    }
}
