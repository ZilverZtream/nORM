using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// A top-level OrderBy/OrderByDescending applied AFTER a Take/Skip window re-sorts the
/// windowed rows through a derived-table wrap (QueryTranslator.OrderByTranslator
/// TranslateAfterTakeSkipWindow). SQLite stores decimal / TimeSpan / DateTimeOffset as TEXT
/// which lex-compares wrong, so the FORWARD OrderBy path coerces such keys
/// (CoerceOrderKey -> OrderByDecimalKeySql / NormalizeTimeSpanForCompare / ...). The
/// windowed-resort path emits the bare key without that coercion, so it sorts lexically —
/// a silent-wrong row order. These tests compare against a LINQ-to-Objects oracle over the
/// same values; the CONTROL (no window) confirms the forward path coerces correctly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderByAfterWindowKeyCoercionTests
{
    [Table("ObwDec")]
    public class DecRow { [Key] public int Id { get; set; } public decimal Price { get; set; } }

    [Table("ObwTs")]
    public class TsRow { [Key] public int Id { get; set; } public TimeSpan Duration { get; set; } }

    // Decimals whose canonical-TEXT (BINARY) order differs sharply from numeric order.
    private static readonly (int Id, decimal Price)[] DecRows =
    {
        (1, 100m), (2, 2m), (3, 10.5m), (4, 9m), (5, 10m),
    };

    // Multi-day durations: "10.00:00:00" < "9.23:00:00" lexically, but 10d > 9d23h numerically.
    private static readonly (int Id, TimeSpan Duration)[] TsRows =
    {
        (1, TimeSpan.FromDays(10)),
        (2, new TimeSpan(9, 23, 0, 0)),
        (3, TimeSpan.FromDays(2)),
        (4, TimeSpan.FromDays(1)),
    };

    private static async Task<DbContext> SeedDecAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand()) { c.CommandText = "CREATE TABLE ObwDec (Id INTEGER PRIMARY KEY, Price TEXT NOT NULL);"; c.ExecuteNonQuery(); }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, price) in DecRows) await ctx.InsertAsync(new DecRow { Id = id, Price = price });
        return ctx;
    }

    private static async Task<DbContext> SeedTsAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand()) { c.CommandText = "CREATE TABLE ObwTs (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);"; c.ExecuteNonQuery(); }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, d) in TsRows) await ctx.InsertAsync(new TsRow { Id = id, Duration = d });
        return ctx;
    }

    // CONTROL: forward OrderBy on a decimal column sorts numerically (proves setup + forward coercion).
    [Fact]
    public async Task Control_direct_OrderBy_decimal_sorts_numerically()
    {
        using var ctx = await SeedDecAsync();
        var norm = ctx.Query<DecRow>().OrderBy(x => x.Price).ToList().Select(x => x.Id).ToList();
        var oracle = DecRows.OrderBy(x => x.Price).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // BUG: OrderBy after a Take window must still order the decimal key NUMERICALLY.
    [Fact]
    public async Task OrderBy_after_Take_window_decimal_sorts_numerically()
    {
        using var ctx = await SeedDecAsync();
        var norm = ctx.Query<DecRow>().OrderBy(x => x.Id).Take(10).OrderBy(x => x.Price)
            .ToList().Select(x => x.Id).ToList();
        var oracle = DecRows.OrderBy(x => x.Id).Take(10).OrderBy(x => x.Price).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // CONTROL: forward OrderBy on a TimeSpan column sorts numerically.
    [Fact]
    public async Task Control_direct_OrderBy_timespan_sorts_numerically()
    {
        using var ctx = await SeedTsAsync();
        var norm = ctx.Query<TsRow>().OrderBy(x => x.Duration).ToList().Select(x => x.Id).ToList();
        var oracle = TsRows.OrderBy(x => x.Duration).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // BUG: OrderBy after a Take window must still order the TimeSpan key NUMERICALLY.
    [Fact]
    public async Task OrderBy_after_Take_window_timespan_sorts_numerically()
    {
        using var ctx = await SeedTsAsync();
        var norm = ctx.Query<TsRow>().OrderBy(x => x.Id).Take(10).OrderBy(x => x.Duration)
            .ToList().Select(x => x.Id).ToList();
        var oracle = TsRows.OrderBy(x => x.Id).Take(10).OrderBy(x => x.Duration).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // BUG (sibling path): OrderBy after a keyed-set operator (DistinctBy) re-sorts the
    // derived-table result through the SAME uncoerced helper -> lexical decimal order.
    [Fact]
    public async Task OrderBy_after_DistinctBy_decimal_sorts_numerically()
    {
        using var ctx = await SeedDecAsync();
        var norm = ctx.Query<DecRow>().DistinctBy(x => x.Id).OrderBy(x => x.Price)
            .ToList().Select(x => x.Id).ToList();
        var oracle = DecRows.DistinctBy(x => x.Id).OrderBy(x => x.Price).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }
}
