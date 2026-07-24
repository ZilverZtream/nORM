using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A decimal range comparison combined with an OrderBy must return the same rows the predicate does on
/// its own. The filtered-ordered fast path emitted a raw <c>Price &lt; @p</c>; on SQLite decimals are
/// stored as TEXT, so that became a LEXICAL string compare (<c>"79.99" &lt; "100"</c> is false) and
/// silently dropped rows — the full translator numerically CASTs. Regression guard for that fast-path
/// deferral.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DecimalRangeFilterOrderByCorrectnessTests
{
    [Table("PriceRow_Test")]
    public sealed class PriceRow
    {
        [Key] public int Id { get; set; }
        public decimal Price { get; set; }
        public bool InStock { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PriceRow_Test (Id INTEGER PRIMARY KEY, Price TEXT NOT NULL, InStock INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new PriceRow { Id = 1, Price = 79.99m, InStock = true });
        ctx.Add(new PriceRow { Id = 2, Price = 24.50m, InStock = true });
        ctx.Add(new PriceRow { Id = 3, Price = 429.00m, InStock = false });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    [Fact]
    public async Task Decimal_less_than_with_order_by_returns_the_same_rows_as_without()
    {
        using var ctx = NewCtx();
        var withOrder = await ctx.Query<PriceRow>().Where(p => p.Price < 100m).OrderBy(p => p.Price).ToListAsync();
        Assert.Equal(new[] { 2, 1 }, withOrder.Select(r => r.Id).ToArray()); // 24.50 then 79.99, both < 100
    }

    [Fact]
    public async Task Bool_and_decimal_less_than_with_order_by_returns_the_in_range_rows()
    {
        using var ctx = NewCtx();
        var withOrder = await ctx.Query<PriceRow>()
            .Where(p => p.InStock && p.Price < 100m)
            .OrderBy(p => p.Price)
            .ToListAsync();
        Assert.Equal(new[] { 2, 1 }, withOrder.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Decimal_greater_than_with_order_by_is_numeric_not_lexical()
    {
        using var ctx = NewCtx();
        // Lexically "429.00" > "9" would be false; numerically 429 > 100 is true.
        var withOrder = await ctx.Query<PriceRow>().Where(p => p.Price > 100m).OrderBy(p => p.Price).ToListAsync();
        Assert.Equal(new[] { 3 }, withOrder.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Decimal_range_with_order_by_matches_the_no_order_by_result()
    {
        using var ctx = NewCtx();
        var noOrder = await ctx.Query<PriceRow>().Where(p => p.InStock && p.Price < 100m).ToListAsync();
        var withOrder = await ctx.Query<PriceRow>().Where(p => p.InStock && p.Price < 100m).OrderBy(p => p.Price).ToListAsync();
        Assert.Equal(noOrder.OrderBy(r => r.Price).Select(r => r.Id), withOrder.Select(r => r.Id));
        Assert.Equal(2, withOrder.Count);
    }
}
