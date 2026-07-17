using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// Pins temporal consistency of composite and subquery-carrying shapes under AsOf:
/// set operations, GroupBy aggregates, grouped-First correlated subqueries, and
/// Contains over another mapped query all read through the SAME history window as
/// the root (the nested sub-translations inherit the ambient temporal scope), so
/// the whole statement reflects one point in time. Sibling of
/// JoinAsOfConsistencyContractTests, which pins the JOIN-based shapes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SubqueryAsOfConsistencyContractTests
{
    [Table("SqaDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("SqaEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int DeptId { get; set; }
        public Dept? Dept { get; set; }
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> BootstrapAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SqaDept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE SqaEmp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Dept>().HasMany(d => d.Emps).WithOne(e => e.Dept!).HasForeignKey(e => e.DeptId, d => d.Id);
            }
        };
        opts.EnableTemporalVersioning();
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        await Task.CompletedTask;
        return (cn, ctx);
    }

    private static async Task<DateTime> ServerNowAsync(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    /// <summary>
    /// Era 1: dept "d1" with one emp "e1". After t1: dept renamed "d1x", emp renamed
    /// "e1x", second emp "e2" added to the same dept.
    /// </summary>
    private static async Task<DateTime> SeedTwoErasAsync(DbContext ctx, SqliteConnection cn)
    {
        var dept = new Dept { Id = 1, Title = "d1" };
        var e1 = new Emp { Id = 1, Name = "e1", DeptId = 1 };
        ctx.Add(dept);
        ctx.Add(e1);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync(cn);
        await Task.Delay(60);

        dept.Title = "d1x";
        e1.Name = "e1x";
        ctx.Add(new Emp { Id = 2, Name = "e2", DeptId = 1 });
        await ctx.SaveChangesAsync();
        return t1;
    }

    [Fact]
    public async Task Set_operations_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // Both sides of a set operation read the era: at t1 only "e1" exists, so a
        // union with a second mapped source contributes nothing extra — the post-t1
        // second emp does not exist in the window.
        var union = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Name.StartsWith("e"))
            .Union(ctx.Query<Emp>().Where(e => e.Id == 2))
            .ToListAsync();
        Assert.Equal(new[] { "e1" }, union.Select(e => e.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());

        // Concat of two era-filtered sides sees era rows only.
        var concat = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Id == 1)
            .Concat(ctx.Query<Emp>().Where(e => e.Id == 2))
            .ToListAsync();
        Assert.Equal(new[] { "e1" }, concat.Select(e => e.Name).ToArray());

        // Live equivalents see the live rows.
        var liveUnion = await ctx.Query<Emp>().Where(e => e.Name.StartsWith("e"))
            .Union(ctx.Query<Emp>().Where(e => e.Id == 2))
            .ToListAsync();
        Assert.Equal(new[] { "e1x", "e2" }, liveUnion.Select(e => e.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task Group_by_aggregate_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // The grouped source is the era row set: one emp in dept 1 at t1, two live.
        var historic = await ctx.Query<Emp>().AsOf(t1)
            .GroupBy(e => e.DeptId)
            .Select(g => new { g.Key, N = g.Count() })
            .ToListAsync();
        Assert.Equal(1, historic.Single().N);

        var live = await ctx.Query<Emp>()
            .GroupBy(e => e.DeptId)
            .Select(g => new { g.Key, N = g.Count() })
            .ToListAsync();
        Assert.Equal(2, live.Single().N);
    }

    [Fact]
    public async Task Grouped_scalar_aggregate_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // The grouped scalar aggregate reads the era rows: the era group contains only
        // the one pre-rename name, while the live group's maximum is the second emp.
        var historic = await ctx.Query<Emp>().AsOf(t1)
            .GroupBy(e => e.DeptId)
            .Select(g => new { g.Key, Max = g.Max(e => e.Name) })
            .ToListAsync();
        Assert.Equal("e1", historic.Single().Max);

        var live = await ctx.Query<Emp>()
            .GroupBy(e => e.DeptId)
            .Select(g => new { g.Key, Max = g.Max(e => e.Name) })
            .ToListAsync();
        Assert.Equal("e2", live.Single().Max);
    }

    [Fact]
    public async Task Contains_subquery_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // The inner mapped query windows too: the era title "d1" identifies the dept
        // at t1, while the live title "d1x" matches nothing inside the window.
        var eraMatch = await ctx.Query<Emp>().AsOf(t1)
            .Where(e => ctx.Query<Dept>().Where(d => d.Title == "d1").Select(d => d.Id).Contains(e.DeptId))
            .ToListAsync();
        Assert.Equal(new[] { "e1" }, eraMatch.Select(e => e.Name).ToArray());

        var liveTitleAtEra = await ctx.Query<Emp>().AsOf(t1)
            .Where(e => ctx.Query<Dept>().Where(d => d.Title == "d1x").Select(d => d.Id).Contains(e.DeptId))
            .ToListAsync();
        Assert.Empty(liveTitleAtEra);

        var live = await ctx.Query<Emp>()
            .Where(e => ctx.Query<Dept>().Where(d => d.Title == "d1x").Select(d => d.Id).Contains(e.DeptId))
            .ToListAsync();
        Assert.Equal(new[] { "e1x", "e2" }, live.OrderBy(e => e.Name).Select(e => e.Name).ToArray());
    }
}
