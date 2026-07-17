using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// AsOf composes with navigation predicates era-consistently: the navigation's
/// correlated principal read goes through the SAME history window as the root,
/// so the whole statement reflects one point in time (see also
/// JoinAsOfConsistencyContractTests for the full join-shape matrix). A principal
/// written OUTSIDE temporal tracking (raw SQL before bootstrap) has no history
/// rows and is therefore not visible through an AsOf navigation — the same rule
/// that already governs root rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalAsOfNavigationTests
{
    [Table("AsOfNav_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("AsOfNav_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    private static async Task<DateTime> ServerNowAsync(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    [Fact]
    public async Task asof_nav_predicate_reads_the_era_principal()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AsOfNav_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE AsOfNav_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => { mb.Entity<Emp>(); mb.Entity<Dept>().HasKey(d => d.Id); } };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var dept = new Dept { Id = 1, Title = "Eng" };
        ctx.Add(dept);
        ctx.Add(new Emp { Id = 1, Name = "ann", DeptId = 1 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync(cn);   // dept titled "Eng"
        await Task.Delay(60);

        dept.Title = "Platform";             // principal renamed after t1
        await ctx.SaveChangesAsync();

        // The navigation predicate evaluates against the PRINCIPAL'S ERA STATE:
        // at t1 the department was titled "Eng", so the era title matches and the
        // post-t1 live title does not.
        var eraMatch = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Dept!.Title == "Eng")
            .Select(e => e.Name).ToListAsync();
        Assert.Equal(new[] { "ann" }, eraMatch);

        var liveTitleAtEra = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Dept!.Title == "Platform")
            .Select(e => e.Name).ToListAsync();
        Assert.Empty(liveTitleAtEra);

        // Without AsOf the live principal governs.
        var live = await ctx.Query<Emp>().Where(e => e.Dept!.Title == "Platform")
            .Select(e => e.Name).ToListAsync();
        Assert.Equal(new[] { "ann" }, live);
    }

    [Fact]
    public async Task principal_without_history_is_not_visible_through_asof_nav()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            // The principal row pre-dates temporal bootstrap (raw SQL insert), so
            // its history table never records it.
            cmd.CommandText = """
                CREATE TABLE AsOfNav_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE AsOfNav_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO AsOfNav_Dept VALUES (1, 'Eng');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => { mb.Entity<Emp>(); mb.Entity<Dept>().HasKey(d => d.Id); } };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        ctx.Add(new Emp { Id = 1, Name = "ann", DeptId = 1 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync(cn);

        // The root reconstructs (tracked write -> history row), but the untracked
        // principal has no history: at t1 it reads as MISSING through the navigation,
        // exactly as an untracked root row would be missing from an AsOf query.
        var names = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Dept!.Title == "Eng")
            .Select(e => e.Name).ToListAsync();
        Assert.Empty(names);

        // The live query still sees it.
        var live = await ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng")
            .Select(e => e.Name).ToListAsync();
        Assert.Equal(new[] { "ann" }, live);
    }
}
