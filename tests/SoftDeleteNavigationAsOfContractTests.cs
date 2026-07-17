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
/// Pins soft-delete era visibility THROUGH NAVIGATIONS under AsOf: the principal's
/// global filter renders against the windowed rows, so a principal soft-deleted
/// AFTER the timestamp is still visible at that era through navigation reads and
/// aggregates, and hidden from live reads — the same rule the root already obeys.
/// The dependent side mirrors it: a child soft-deleted after the timestamp still
/// counts at the era and stops counting live.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SoftDeleteNavigationAsOfContractTests
{
    [Table("SdnDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public bool IsDeleted { get; set; }
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("SdnEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
        public int DeptId { get; set; }
        public Dept? Dept { get; set; }
    }

    private static async Task<DateTime> ServerNowAsync(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    [Fact]
    public async Task Soft_delete_visibility_is_era_consistent_through_navigations()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SdnDept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                CREATE TABLE SdnEmp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL, DeptId INTEGER NOT NULL);
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
        opts.AddGlobalFilter<Dept>(d => !d.IsDeleted);
        opts.AddGlobalFilter<Emp>(e => !e.IsDeleted);
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Era 1: live dept "da" with two live emps.
        var dept = new Dept { Id = 1, Title = "da" };
        var e1 = new Emp { Id = 1, Name = "ea", DeptId = 1 };
        var e2 = new Emp { Id = 2, Name = "eb", DeptId = 1 };
        ctx.Add(dept);
        ctx.Add(e1);
        ctx.Add(e2);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync(cn);
        await Task.Delay(60);

        // Era 2: the principal AND one dependent are soft-deleted after t1.
        dept.IsDeleted = true;
        e2.IsDeleted = true;
        await ctx.SaveChangesAsync();

        // At t1 the principal was live: its title reads through the navigation and
        // the navigation predicate matches. Live, the principal is filtered out —
        // it reads as MISSING (NULL member) and the predicate matches nothing.
        var eraProj = await ctx.Query<Emp>().AsOf(t1)
            .Select(e => new { e.Name, DeptTitle = (string?)e.Dept!.Title }).ToListAsync();
        Assert.Equal(new[] { "ea|da", "eb|da" },
            eraProj.OrderBy(r => r.Name, StringComparer.Ordinal).Select(r => $"{r.Name}|{r.DeptTitle}").ToArray());

        var eraPred = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Dept!.Title == "da").ToListAsync();
        Assert.Equal(2, eraPred.Count);

        var liveProj = await ctx.Query<Emp>()
            .Select(e => new { e.Name, DeptTitle = (string?)e.Dept!.Title }).ToListAsync();
        var liveRow = Assert.Single(liveProj);   // e2 is itself soft-deleted live
        Assert.Equal("ea", liveRow.Name);
        Assert.Null(liveRow.DeptTitle);          // principal soft-deleted -> missing

        Assert.Empty(await ctx.Query<Emp>().Where(e => e.Dept!.Title == "da").ToListAsync());

        // Dependent-side aggregates: both emps count at t1; live, the dept itself is
        // filtered out at the root and the surviving emp's aggregate view of history
        // is reachable only through the era query.
        var eraCounts = await ctx.Query<Dept>().AsOf(t1)
            .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync();
        Assert.Equal(2, eraCounts.Single().N);

        Assert.Empty(await ctx.Query<Dept>().ToListAsync());
    }
}
