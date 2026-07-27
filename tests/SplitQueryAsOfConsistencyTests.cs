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
/// A shaped navigation-collection projection under AsOf reconstructs its children through the dependent
/// (split) child load, which must bind the AsOf timestamp the way the provider stores its validity
/// windows. That child load bound the raw DateTime instead of the provider-formatted value; on SQLite
/// the windows are TEXT compared lexically with fixed three-decimal milliseconds, so a boundary instant
/// whose milliseconds end in zero bound as a differently formatted (trailing-zero-trimmed) string that
/// sorts BEFORE the stored window edge — selecting the previous era's child rows. The main-query and
/// single-query Include AsOf paths already bind through the provider hook; the split child load must match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SplitQueryAsOfConsistencyTests
{
    [Table("SqaocParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("SqaocChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    private static DbContext BuildTemporal(SqliteConnection cn)
    {
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
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Shaped_collection_projection_at_a_trailing_zero_ms_boundary_reads_the_correct_era()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        Exec(cn, "CREATE TABLE SqaocParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);"
               + "CREATE TABLE SqaocChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);");

        await using var ctx = BuildTemporal(cn);

        // Two versions of the child (Val 10 -> 99) so the history table holds a real transition; the
        // triggers create the history rows and windows.
        ctx.Add(new Parent { Id = 1, Name = "p" });
        ctx.Add(new Child { Id = 1, ParentId = 1, Val = 10 });
        await ctx.SaveChangesAsync();
        var c = await ctx.Query<Child>().FirstAsync(x => x.Id == 1);
        c.Val = 99;
        await ctx.SaveChangesAsync();

        // Rewrite the validity windows to fully controlled instants so the era boundary is a fixed
        // trailing-zero-millisecond value ('...:00.500'). At exactly that instant the NEW version (Val=99)
        // is valid: windows are [ValidFrom, ValidTo). Provider-formatted binding compares '...:00.500'
        // against the stored '...:00.500' and selects Val=99; the raw DateTime bind trims the trailing
        // zero to '...:00.5', sorts before the boundary, and selects the OLD Val=10.
        const string boundary = "2020-06-01 00:00:00.500";
        Exec(cn, "UPDATE SqaocParent_History SET __ValidFrom='2000-01-01 00:00:00.000', __ValidTo='9999-12-31 23:59:59.999';");
        Exec(cn, $"UPDATE SqaocChild_History SET __ValidFrom='2000-01-01 00:00:00.000', __ValidTo='{boundary}' WHERE Val=10;");
        Exec(cn, $"UPDATE SqaocChild_History SET __ValidFrom='{boundary}', __ValidTo='9999-12-31 23:59:59.999' WHERE Val=99;");

        var asOf = DateTime.SpecifyKind(
            DateTime.ParseExact(boundary, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
            DateTimeKind.Utc);

        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Select(p => new { p.Id, Vals = p.Children.Select(x => x.Val).ToList() })
            .AsOf(asOf).ToListAsync();

        Assert.Equal(new[] { 99 }, historic.Single().Vals.ToArray());   // BUG: {10} — raw bind selected the previous era
    }

    [Fact]
    public async Task Shaped_collection_projection_under_as_of_reconstructs_the_same_era_as_the_root()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        Exec(cn, "CREATE TABLE SqaocParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);"
               + "CREATE TABLE SqaocChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);");

        await using var ctx = BuildTemporal(cn);

        async Task<DateTime> ServerNowAsync()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        }

        ctx.Add(new Parent { Id = 1, Name = "p" });
        ctx.Add(new Child { Id = 1, ParentId = 1, Val = 10 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();
        await Task.Delay(60);

        var c = await ctx.Query<Child>().FirstAsync(x => x.Id == 1);
        c.Val = 99;
        ctx.Add(new Child { Id = 2, ParentId = 1, Val = 20 });
        await ctx.SaveChangesAsync();

        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Select(p => new { p.Id, Vals = p.Children.Select(x => x.Val).ToList() })
            .AsOf(t1).ToListAsync();
        Assert.Equal(new[] { 10 }, historic.Single().Vals.OrderBy(v => v).ToArray());

        var live = ctx.Query<Parent>()
            .Select(p => new { p.Id, Vals = p.Children.Select(x => x.Val).ToList() })
            .ToList();
        Assert.Equal(new[] { 20, 99 }, live.Single().Vals.OrderBy(v => v).ToArray());
    }
}
