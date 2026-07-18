using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that bulk ExecuteUpdate/ExecuteDelete apply a column's value converter — the WHERE predicate and the
/// SetProperty value both use the provider representation, so `Where(w => w.Score == 5)` targets the stored
/// value and `SetProperty(w => w.Score, 20)` stores the converted value. A plausible silent-wrong site;
/// validated correct (these go through the full translator).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkCudConverterColumnContractTests
{
    [Table("BccWidget")]
    public class Widget { [Key] public int Id { get; set; } public int Score { get; set; } }

    // Model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE BccWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
                INSERT INTO BccWidget VALUES (1,1005),(2,1007),(3,1009);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Widget>().HasKey(w => w.Id);
            mb.Entity<Widget>().Property<int>(w => w.Score).HasConversion(new OffsetConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static int[] RawScores(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Score FROM BccWidget ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var v = new List<int>(); while (r.Read()) v.Add(r.GetInt32(0)); return v.ToArray();
    }

    [Fact]
    public async Task ExecuteDelete_applies_the_converter_in_the_where()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        var n = await ctx.Query<Widget>().Where(w => w.Score == 5).ExecuteDeleteAsync();

        Assert.Equal(1, n);   // model 5 → stored 1005 (row 1)
        Assert.Equal(new[] { 2, 3 }, ctx.Query<Widget>().ToList().OrderBy(w => w.Id).Select(w => w.Id).ToArray());
    }

    [Fact]
    public async Task ExecuteUpdate_stores_the_converted_set_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        var n = await ctx.Query<Widget>().Where(w => w.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(w => w.Score, 20));

        Assert.Equal(1, n);
        Assert.Equal(new[] { 1020, 1007, 1009 }, RawScores(cn));   // 20 stored as 1020
    }

    [Fact]
    public async Task ExecuteUpdate_where_and_set_both_convert()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        // WHERE model Score == 7 (stored 1007, row 2) → SET Score = 30 (stored 1030).
        var n = await ctx.Query<Widget>().Where(w => w.Score == 7).ExecuteUpdateAsync(s => s.SetProperty(w => w.Score, 30));

        Assert.Equal(1, n);
        Assert.Equal(new[] { 1005, 1030, 1009 }, RawScores(cn));
    }
}
