using System.Collections.Generic;
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
/// A `== null` / `!= null` test inside an ExecuteUpdate SetProperty (a ternary value, or a nav-aggregate
/// Count predicate) must lower to `IS NULL` / `IS NOT NULL`. The BulkCud predicate renderers mapped Equal to
/// `=` and NotEqual to `&lt;&gt;` unconditionally, so `col = NULL` (3VL-unknown) made every conditional take the
/// ELSE branch and every filtered count return 0 — a silent wrong value written to every matched row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ExecuteUpdateNullPredicateTests
{
    [Table("EunRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int? N { get; set; }
        public int Result { get; set; }
    }

    private static (SqliteConnection, DbContext) Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EunRow (Id INTEGER PRIMARY KEY, N INTEGER NULL, Result INTEGER NOT NULL);" +
                              "INSERT INTO EunRow VALUES (1,NULL,0),(2,5,0),(3,NULL,0),(4,7,0),(5,NULL,0);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static List<(int, int)> Read(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Result FROM EunRow ORDER BY Id";
        using var rdr = cmd.ExecuteReader();
        var list = new List<(int, int)>();
        while (rdr.Read()) list.Add((rdr.GetInt32(0), rdr.GetInt32(1)));
        return list;
    }

    [Fact]
    public async Task SetProperty_conditional_is_null_sets_correct_branch()
    {
        var (cn, ctx) = Ctx();
        using var _cn = cn; using var _ctx = ctx;
        await ctx.Query<Row>().ExecuteUpdateAsync(s => s.SetProperty(r => r.Result, r => r.N == null ? 1 : 0));
        Assert.Equal(new List<(int, int)> { (1, 1), (2, 0), (3, 1), (4, 0), (5, 1) }, Read(cn));
    }

    [Fact]
    public async Task SetProperty_conditional_is_not_null_sets_correct_branch()
    {
        var (cn, ctx) = Ctx();
        using var _cn = cn; using var _ctx = ctx;
        await ctx.Query<Row>().ExecuteUpdateAsync(s => s.SetProperty(r => r.Result, r => r.N != null ? 1 : 0));
        Assert.Equal(new List<(int, int)> { (1, 0), (2, 1), (3, 0), (4, 1), (5, 0) }, Read(cn));
    }

    [Table("EunParent")]
    public class Parent { [Key] public int Id { get; set; } public int Cnt { get; set; } public List<Item> Items { get; set; } = new(); }
    [Table("EunItem")]
    public class Item { [Key] public int Id { get; set; } public int ParentId { get; set; } public int? AssigneeId { get; set; } public Parent Parent { get; set; } = null!; }

    [Fact]
    public async Task SetProperty_navAggregate_count_null_column_is_correct()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EunParent (Id INTEGER PRIMARY KEY, Cnt INTEGER NOT NULL DEFAULT 0);" +
                              "CREATE TABLE EunItem (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, AssigneeId INTEGER NULL);" +
                              "INSERT INTO EunParent (Id) VALUES (1);" +
                              "INSERT INTO EunItem (Id, ParentId, AssigneeId) VALUES (1,1,NULL),(2,1,7),(3,1,NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Parent>().HasKey(p => p.Id)
                .HasMany(p => p.Items).WithOne(i => i.Parent).HasForeignKey(i => i.ParentId, p => p.Id)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await ctx.Query<Parent>().ExecuteUpdateAsync(s => s.SetProperty(p => p.Cnt, p => p.Items.Count(i => i.AssigneeId == null)));
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Cnt FROM EunParent WHERE Id = 1";
        Assert.Equal(2L, (long)check.ExecuteScalar()!);
    }
}
