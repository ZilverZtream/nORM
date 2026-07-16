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
/// Contract for AsOf semantics at EXACT version-transition instants (temporal matrix cell).
///
/// AsOf intervals are HALF-OPEN: <c>ts &gt;= __ValidFrom AND ts &lt; __ValidTo</c>. A timestamp
/// exactly equal to a transition instant therefore belongs to the NEW version (inclusive lower
/// bound, exclusive upper bound) - the same contract as SQL Server's FOR SYSTEM_TIME AS OF. This
/// holds against the ACTUAL STORED trigger timestamps (parsed back from the history table), so the
/// bound-parameter text form and the trigger's millisecond text compare correctly at the boundary.
/// Before the first version AsOf is empty; after the last it returns the current state; the exact
/// first ValidFrom instant already sees version 1. Probes keep >precision gaps between versions per
/// the temporal campaign's probe lesson.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalAsOfBoundaryContractTests
{
    [Table("AsOfBoundary_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Fact]
    public async Task AsOf_at_a_transition_instant_returns_the_new_version()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AsOfBoundary_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var row = new Row { Id = 1, V = 1 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();
        await Task.Delay(150);   // > trigger precision (ms), per the temporal probe lesson
        row.V = 2;
        await ctx.SaveChangesAsync();
        await Task.Delay(150);
        row.V = 3;
        await ctx.SaveChangesAsync();

        // Read the ACTUAL stored transition instants from the history table.
        string v1From, v1To;
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT __ValidFrom, __ValidTo FROM AsOfBoundary_Row_History WHERE V = 1;";
            using var r = c.ExecuteReader();
            Assert.True(r.Read());
            v1From = r.GetString(0);
            v1To = r.GetString(1);
        }
        var firstFrom = DateTime.Parse(v1From, CultureInfo.InvariantCulture);
        var boundary = DateTime.Parse(v1To, CultureInfo.InvariantCulture);   // v1 -> v2 transition

        async Task<string> AsOfV(DateTime ts)
        {
            var got = await ctx.Query<Row>().AsOf(ts).Where(r => r.Id == 1).ToListAsync();
            return got.Count == 0 ? "EMPTY" : $"V{got[0].V}";
        }

        Assert.Equal("V2", await AsOfV(boundary));                          // exact instant -> NEW version
        Assert.Equal("V1", await AsOfV(boundary.AddMilliseconds(-1)));      // just before -> old version
        Assert.Equal("V2", await AsOfV(boundary.AddMilliseconds(1)));       // just after -> new version
        Assert.Equal("V1", await AsOfV(firstFrom));                         // inclusive lower bound
        Assert.Equal("EMPTY", await AsOfV(firstFrom.AddMilliseconds(-50))); // before first version
        Assert.Equal("V3", await AsOfV(DateTime.UtcNow.AddSeconds(1)));     // after last -> current
    }

    [Table("AsOfZeroMs_Row")]
    private class ZeroMsRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    /// <summary>
    /// Deterministic regression for the trailing-zero-millisecond boundary: the triggers store
    /// fixed three-decimal text ("...53.590"), but the driver's default DateTime text trims
    /// trailing fractional zeros ("...53.59"), which sorts BEFORE the stored form in the lexical
    /// window comparison - an AsOf at such a transition instant silently returned the OLD
    /// version (and matched BOTH windows). Seeds the history with zero-ending boundaries
    /// directly so the case does not depend on the wall clock.
    /// </summary>
    [Fact]
    public async Task AsOf_at_a_trailing_zero_millisecond_boundary_returns_the_new_version()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE AsOfZeroMs_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);" +
                "INSERT INTO AsOfZeroMs_Row VALUES (1, 2);" +
                "CREATE TABLE AsOfZeroMs_Row_History (__VersionId INTEGER PRIMARY KEY AUTOINCREMENT, __ValidFrom TEXT NOT NULL, __ValidTo TEXT NOT NULL, __Operation TEXT NOT NULL, Id INTEGER NOT NULL, V INTEGER NOT NULL);" +
                "INSERT INTO AsOfZeroMs_Row_History (__ValidFrom, __ValidTo, __Operation, Id, V) VALUES " +
                "('2026-01-01 10:00:00.500', '2026-01-01 10:00:01.590', 'I', 1, 1)," +
                "('2026-01-01 10:00:01.590', '9999-12-31', 'U', 1, 2);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<ZeroMsRow>() };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var boundary = new DateTime(2026, 1, 1, 10, 0, 1, 590, DateTimeKind.Unspecified);
        var got = await ctx.Query<ZeroMsRow>().AsOf(boundary).Where(r => r.Id == 1).ToListAsync();
        var only = Assert.Single(got);   // matching BOTH windows was part of the failure mode
        Assert.Equal(2, only.V);         // exact instant -> NEW version

        var justBefore = await ctx.Query<ZeroMsRow>().AsOf(boundary.AddMilliseconds(-1)).Where(r => r.Id == 1).ToListAsync();
        Assert.Equal(1, Assert.Single(justBefore).V);
    }
}
