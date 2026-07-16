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
/// Pins temporal consistency of eager loading: Include under AsOf reconstructs the related
/// rows through the SAME history window as the root — the child collection at the requested
/// timestamp holds that era's rows with that era's values, and neither later updates nor
/// later-added children leak in. Before the fix the root reconstructed history while every
/// eager load read the LIVE tables, silently mixing eras.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IncludeAsOfConsistencyContractTests
{
    [Table("IaocParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("IaocChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task One_to_many_include_under_as_of_probe()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IaocParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE IaocChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        async Task<DateTime> ServerNowAsync()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        }

        var parent = new Parent { Id = 1, Name = "p" };
        var c1 = new Child { Id = 1, ParentId = 1, Val = 10 };
        ctx.Add(parent);
        ctx.Add(c1);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();   // parent has ONE child, Val=10
        await Task.Delay(60);

        c1.Val = 99;                        // child updated after t1
        var c2 = new Child { Id = 2, ParentId = 1, Val = 20 };
        ctx.Add(c2);                        // second child added after t1
        await ctx.SaveChangesAsync();

        // The eager load must reconstruct the children at the SAME timestamp as the root:
        // at t1 the parent had ONE child with Val=10. Live children (Val=99, plus the
        // post-t1 second child) must not leak into the historical result.
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children).AsOf(t1).ToListAsync();
        var kids = historic.Single().Children.OrderBy(c => c.Id).Select(c => $"{c.Id}:{c.Val}").ToList();
        Assert.Equal(new[] { "1:10" }, kids);

        // Include WITHOUT AsOf still reads the live rows.
        var live = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children).ToListAsync();
        Assert.Equal(new[] { "1:99", "2:20" },
            live.Single().Children.OrderBy(c => c.Id).Select(c => $"{c.Id}:{c.Val}").ToArray());
    }
}
