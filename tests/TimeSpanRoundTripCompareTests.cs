using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Multi-day TimeSpans must round-trip exactly and compare/order numerically, not by the lexical
/// order of their canonical text ('10.00:00:00' &lt; '9.23:59:59' lexically, but 10 days &gt; 9d23h).
/// A naive TEXT storage + string comparison silently returns the wrong rows.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TimeSpanRoundTripCompareTests
{
    [Table("TsItem")]
    private class TsItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public TimeSpan Duration { get; set; }
    }

    private static DbContext Create(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TsItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Duration TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Theory]
    [InlineData("10.05:30:15.1234567")]   // multi-day, high precision
    [InlineData("00:00:00")]
    [InlineData("9.23:59:59.9999999")]
    [InlineData("-3.12:00:00")]            // negative multi-day
    [InlineData("365.00:00:00")]           // a full year
    public async Task Multiday_timespan_round_trips_exactly(string literal)
    {
        var value = TimeSpan.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);

        var item = new TsItem { Name = "x", Duration = value };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<TsItem>().Where(t => t.Id == item.Id).FirstAsync();
        Assert.Equal(value, loaded.Duration);
    }

    [Fact]
    public async Task Multiday_timespan_comparison_orders_numerically()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);

        ctx.Add(new TsItem { Name = "8d",    Duration = TimeSpan.FromDays(8) });
        ctx.Add(new TsItem { Name = "9d23h", Duration = new TimeSpan(9, 23, 0, 0) });   // < 10 days
        ctx.Add(new TsItem { Name = "10d",   Duration = TimeSpan.FromDays(10) });
        await ctx.SaveChangesAsync();

        var threshold = TimeSpan.FromDays(9);
        var overNine = (await ctx.Query<TsItem>().Where(t => t.Duration > threshold).ToListAsync())
            .Select(t => t.Name).OrderBy(n => n).ToArray();
        // 9d23h and 10d are > 9d; 8d is not. Lexical text order would wrongly place '10.*' below '9.*'.
        Assert.Equal(new[] { "10d", "9d23h" }, overNine);
    }

    [Fact]
    public async Task Multiday_timespan_orderby_is_numeric()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);

        ctx.Add(new TsItem { Name = "10d", Duration = TimeSpan.FromDays(10) });
        ctx.Add(new TsItem { Name = "9d",  Duration = TimeSpan.FromDays(9) });
        ctx.Add(new TsItem { Name = "100d", Duration = TimeSpan.FromDays(100) });
        await ctx.SaveChangesAsync();

        var ordered = (await ctx.Query<TsItem>().OrderBy(t => t.Duration).ToListAsync())
            .Select(t => t.Name).ToArray();
        Assert.Equal(new[] { "9d", "10d", "100d" }, ordered);
    }
}
