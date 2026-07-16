using System;
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
/// Pins the soft-delete x temporal interaction, probed differentially: a global filter
/// (`AddGlobalFilter(r => !r.IsDeleted)`) composes with AsOf over the RECONSTRUCTED era
/// state — the row is visible at timestamps where it was live (with that era's values),
/// hidden at timestamps after the soft delete, hidden in the current view, and the soft
/// delete itself is an ordinary versioned update so the history chain stays complete.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SoftDeleteTemporalInteractionContractTests
{
    [Table("SdtRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
        public bool IsDeleted { get; set; }
    }

    [Fact]
    public async Task Soft_delete_filter_composes_with_as_of_over_reconstructed_state()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SdtRow_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL, IsDeleted INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.AddGlobalFilter<Row>(r => !r.IsDeleted);
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        async Task<DateTime> ServerNowAsync()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(
                DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None),
                DateTimeKind.Utc);
        }

        var row = new Row { Id = 1, Val = 10 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();
        await Task.Delay(60);

        row.Val = 20;
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t2 = await ServerNowAsync();
        await Task.Delay(60);

        row.IsDeleted = true;   // soft delete = plain update through the temporal triggers
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t3 = await ServerNowAsync();

        // Current view: hidden by the filter.
        var current = await ctx.Query<Row>().ToListAsync();
        Console.WriteLine($"current: {current.Count} row(s)");

        // AsOf while live: reconstructed state was not deleted -> visible with era values.
        var at1 = await ctx.Query<Row>().AsOf(t1).ToListAsync();
        Console.WriteLine($"AsOf(t1): {at1.Count} row(s), Val={(at1.Count > 0 ? at1[0].Val : -1)}");
        var at2 = await ctx.Query<Row>().AsOf(t2).ToListAsync();
        Console.WriteLine($"AsOf(t2): {at2.Count} row(s), Val={(at2.Count > 0 ? at2[0].Val : -1)}");

        // AsOf after the soft delete: the reconstructed row IS deleted -> the filter must hide it.
        var at3 = await ctx.Query<Row>().AsOf(t3).ToListAsync();
        Console.WriteLine($"AsOf(t3): {at3.Count} row(s)");

        // History integrity: the soft delete is an ordinary versioned update (3 versions).
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM SdtRow_Test_History";
            Console.WriteLine($"history rows: {cmd.ExecuteScalar()}");
        }

        Assert.Empty(current);
        Assert.True(at1.Count == 1 && at1[0].Val == 10, $"AsOf(t1) expected the live v1 row, got {at1.Count} row(s)");
        Assert.True(at2.Count == 1 && at2[0].Val == 20, $"AsOf(t2) expected the live v2 row, got {at2.Count} row(s)");
        Assert.Empty(at3);
    }
}
