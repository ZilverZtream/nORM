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
/// Pins temporal consistency of JOIN-based query shapes: navigation-scalar projections
/// (e.Dept.Title), navigation predicates in WHERE, SelectMany navigation flattening, and
/// correlated navigation aggregates (d.Emps.Count()) must all read through the SAME history
/// window as the AsOf root — era values and era membership, with no live-table leaks. These
/// are the join-path siblings of the eager-load consistency contract
/// (IncludeAsOfConsistencyContractTests): the root FROM substitution alone is not enough,
/// because these shapes reference additional tables inside the main statement.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class JoinAsOfConsistencyContractTests
{
    [Table("JaocDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("JaocEmp")]
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
                CREATE TABLE JaocDept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE JaocEmp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
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
    /// Era 1: dept "d1" with one emp "e1". After t1: dept renamed "d1x", emp renamed "e1x",
    /// second emp "e2" added. Every probe below asks for t1 and must see ONLY era-1 state.
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
    public async Task Nav_scalar_projection_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // e.Dept.Title joins the principal table inside the main statement: under AsOf
        // both sides must be the t1 era — one emp, era names on both entities.
        var historic = await ctx.Query<Emp>().AsOf(t1)
            .Select(e => new { e.Name, DeptTitle = e.Dept!.Title })
            .ToListAsync();
        Assert.Equal(new[] { "e1|d1" },
            historic.OrderBy(r => r.Name).Select(r => $"{r.Name}|{r.DeptTitle}").ToArray());

        // Without AsOf the live values come back (two emps, renamed titles).
        var live = await ctx.Query<Emp>()
            .Select(e => new { e.Name, DeptTitle = e.Dept!.Title })
            .ToListAsync();
        Assert.Equal(new[] { "e1x|d1x", "e2|d1x" },
            live.OrderBy(r => r.Name).Select(r => $"{r.Name}|{r.DeptTitle}").ToArray());
    }

    [Fact]
    public async Task Nav_scalar_predicate_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // The WHERE navigation must evaluate against the era title: at t1 the dept was
        // "d1", so filtering on the LIVE title "d1x" must match nothing and the era
        // title must match the one era emp.
        var eraMatch = await ctx.Query<Emp>().AsOf(t1)
            .Where(e => e.Dept!.Title == "d1").ToListAsync();
        Assert.Equal(new[] { "e1" }, eraMatch.Select(e => e.Name).ToArray());

        var liveTitleAtEra = await ctx.Query<Emp>().AsOf(t1)
            .Where(e => e.Dept!.Title == "d1x").ToListAsync();
        Assert.Empty(liveTitleAtEra);

        var live = await ctx.Query<Emp>()
            .Where(e => e.Dept!.Title == "d1x").ToListAsync();
        Assert.Equal(new[] { "e1x", "e2" }, live.OrderBy(e => e.Name).Select(e => e.Name).ToArray());
    }

    [Fact]
    public async Task Select_many_nav_flatten_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // SelectMany builds the JOIN inside the main statement: at t1 the dept had ONE
        // emp named "e1" — the post-t1 rename and the post-t1 second emp must not leak.
        var historic = await ctx.Query<Dept>().AsOf(t1)
            .SelectMany(d => d.Emps).ToListAsync();
        Assert.Equal(new[] { "e1" }, historic.Select(e => e.Name).ToArray());

        var live = await ctx.Query<Dept>()
            .SelectMany(d => d.Emps).ToListAsync();
        Assert.Equal(new[] { "e1x", "e2" }, live.OrderBy(e => e.Name).Select(e => e.Name).ToArray());
    }

    [Fact]
    public async Task Correlated_nav_aggregate_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // The correlated subquery must count era membership: one emp at t1, two live.
        var historic = await ctx.Query<Dept>().AsOf(t1)
            .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync();
        Assert.Equal(1, historic.Single().N);

        var live = await ctx.Query<Dept>()
            .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync();
        Assert.Equal(2, live.Single().N);
    }

    [Fact]
    public async Task Root_aggregates_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // Root-level aggregates must count/test era membership, not the live rows.
        Assert.Equal(1, await ctx.Query<Emp>().AsOf(t1).CountAsync());
        Assert.Equal(2, await ctx.Query<Emp>().CountAsync());
        Assert.True(await ctx.Query<Emp>().AsOf(t1).AnyAsync(e => e.Name == "e1"));
        Assert.False(await ctx.Query<Emp>().AsOf(t1).AnyAsync(e => e.Name == "e1x"));
        Assert.False(await ctx.Query<Emp>().AsOf(t1).AnyAsync(e => e.Name == "e2"));
    }

    [Fact]
    public async Task Explicit_join_under_as_of_probe()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // An explicit Join under a root AsOf reads BOTH sides through the same history
        // window: the whole statement reflects one point in time.
        var historic = await ctx.Query<Dept>().AsOf(t1)
            .Join(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, e) => new { d.Title, e.Name })
            .ToListAsync();
        Assert.Equal(new[] { "d1|e1" },
            historic.OrderBy(r => r.Name).Select(r => $"{r.Title}|{r.Name}").ToArray());

        var live = await ctx.Query<Dept>()
            .Join(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, e) => new { d.Title, e.Name })
            .ToListAsync();
        Assert.Equal(new[] { "d1x|e1x", "d1x|e2" },
            live.OrderBy(r => r.Name).Select(r => $"{r.Title}|{r.Name}").ToArray());
    }

    [Fact]
    public async Task Execute_update_and_delete_reject_as_of_sources()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;
        var t1 = await SeedTwoErasAsync(ctx, cn);

        // Set-based writes target the live table; selecting the target rows by
        // historical state must fail loud instead of silently mutating current rows.
        var updateEx = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Emp>().AsOf(t1).Where(e => e.Name == "e1")
                .ExecuteUpdateAsync(s => s.SetProperty(e => e.Name, "mutated")));
        Assert.Contains("AsOf", updateEx.Message, StringComparison.Ordinal);

        var deleteEx = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Emp>().AsOf(t1).Where(e => e.Name == "e1").ExecuteDeleteAsync());
        Assert.Contains("AsOf", deleteEx.Message, StringComparison.Ordinal);

        // Nothing was written: both eras remain intact.
        Assert.Equal(2, await ctx.Query<Emp>().CountAsync());
        Assert.Equal(1, await ctx.Query<Emp>().AsOf(t1).CountAsync());
    }
}
