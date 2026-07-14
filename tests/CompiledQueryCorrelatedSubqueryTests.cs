using System;
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

[Trait("Category", "Fast")]
public class CompiledQueryCorrelatedSubqueryTests
{
    [Table("CqcParent")]
    public class Parent { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("CqcChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public int Val { get; set; } }

    private static readonly (int Id, int Pid, int Val)[] ChildRows =
    {
        (1, 1, 10), (2, 1, 30), (3, 2, 5), (4, 2, 50), (5, 3, 7), (6, 3, 12),
    };

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:cqc_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CqcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CqcChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO CqcParent VALUES (1,'a'),(2,'b'),(3,'c');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, val) in ChildRows)
            {
                using var ins = keeper.CreateCommand();
                ins.CommandText = $"INSERT INTO CqcChild VALUES ({id},{pid},{val})";
                ins.ExecuteNonQuery();
            }
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => { mb.Entity<Parent>().HasKey(p => p.Id); mb.Entity<Child>().HasKey(c => c.Id); }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static IEnumerable<Child> Oracle() =>
        ChildRows.Select(r => new Child { Id = r.Id, ParentId = r.Pid, Val = r.Val });

    [Fact]
    public async Task Compiled_query_with_correlated_count_rebinds_param()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;

        var compiled = Norm.CompileQuery((DbContext ctx, int minChildren) =>
            ctx.Query<Parent>()
               .Where(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id) >= minChildren)
               .Select(p => new { p.Id }));

        int[] OracleRun(int minC) => new[] { 1, 2, 3 }
            .Where(pid => Oracle().Count(c => c.ParentId == pid) >= minC)
            .OrderBy(x => x).ToArray();

        foreach (var minC in new[] { 1, 2, 3, 0 })
        {
            await using var ctx = make();
            var got = (await compiled(ctx, minC)).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(OracleRun(minC), got);
        }
    }

    [Fact]
    public async Task Compiled_query_with_correlated_first_rebinds_param()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;

        var compiled = Norm.CompileQuery((DbContext ctx, int hi) =>
            ctx.Query<Parent>()
               .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                              .OrderByDescending(c => c.Val).Select(c => c.Val).First() > hi)
               .Select(p => new { p.Id }));

        int[] OracleRun(int hi) => new[] { 1, 2, 3 }
            .Where(pid => Oracle().Where(c => c.ParentId == pid)
                              .OrderByDescending(c => c.Val).Select(c => c.Val).First() > hi)
            .OrderBy(x => x).ToArray();

        foreach (var hi in new[] { 20, 40, 5, 100 })
        {
            await using var ctx = make();
            var got = (await compiled(ctx, hi)).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(OracleRun(hi), got);
        }
    }
}
