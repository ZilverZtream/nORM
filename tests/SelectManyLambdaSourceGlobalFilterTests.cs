using System;
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
/// Query-shaped sources inside SelectMany LAMBDAS (correlated and cross-join arms)
/// respect the inner entity's global filters: the provider-level filter rewrite
/// never descends into lambda arguments, so the flatten join carries the filter in
/// its ON clause when a result selector projects an unmapped shape.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SelectManyLambdaSourceGlobalFilterTests
{
    [Table("GfLam_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("GfLam_Chore")]
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
                CREATE TABLE GfLam_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE GfLam_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL, Done INTEGER NOT NULL);
                INSERT INTO GfLam_Emp VALUES (1, 'ann');
                INSERT INTO GfLam_Chore VALUES (1, 1, 'code', 0), (2, 1, 'ship', 1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Chore>(c => !c.Done);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Emp>().HasKey(e => e.Id);
            mb.Entity<Chore>().HasKey(c => c.Id);
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void correlated_query_source_respects_inner_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .SelectMany(e => ctx.Query<Chore>().Where(c => c.EmpId == e.Id), (e, c) => new { e.Name, c.What })
            .ToList().OrderBy(r => r.What).ToList();
        Assert.Single(rows); // 'ship' is Done → filtered
        Assert.Equal("code", rows[0].What);
    }

    [Fact]
    public void cross_join_query_source_respects_inner_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .SelectMany(e => ctx.Query<Chore>(), (e, c) => new { e.Name, c.What })
            .ToList().OrderBy(r => r.What).ToList();
        Assert.Single(rows);
        Assert.Equal("code", rows[0].What);
    }
}
