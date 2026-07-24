using System;
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
/// A <c>decimal</c> equality predicate must match by numeric value, not by the stored TEXT's scale.
/// On SQLite decimals are stored as TEXT; a value written elsewhere with a trailing-zero scale
/// (<c>"24.500"</c>) is numerically equal to <c>24.5m</c>, and the full translator canonicalizes both
/// sides (trailing zeros stripped) so they match. The read fast paths emitted a raw <c>col = @p</c>,
/// which is a lexical TEXT compare (<c>"24.500" = "24.5"</c> is false) and silently drops the row.
/// Regression guard: every fast path that accepts an equality predicate must agree with the full
/// translator (and with C# <c>decimal</c> equality).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DecimalEqualityFastPathScaleTests
{
    [Table("PriceEq_Test")]
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
            // Stored with trailing-zero scale, exactly as external clients / other ORMs write decimals.
            cmd.CommandText =
                "CREATE TABLE PriceEq_Test (Id INTEGER PRIMARY KEY, Price TEXT NOT NULL, InStock INTEGER NOT NULL);" +
                "INSERT INTO PriceEq_Test VALUES (1, '24.500', 1), (2, '9.90', 1), (3, '100.00', 0);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    // Filtered-ordered fast path (Where + OrderBy).
    [Fact]
    public async Task Decimal_equality_with_order_by_matches_trailing_zero_scale()
    {
        using var ctx = NewCtx();
        var rows = await ctx.Query<PriceRow>().Where(p => p.Price == 24.5m).OrderBy(p => p.Id).ToListAsync();
        Assert.Equal(new[] { 1 }, rows.Select(r => r.Id).ToArray()); // 24.500m == 24.5m
    }

    // Simple-Where fast path (Where alone, async).
    [Fact]
    public async Task Decimal_equality_plain_where_matches_trailing_zero_scale()
    {
        using var ctx = NewCtx();
        var rows = await ctx.Query<PriceRow>().Where(p => p.Price == 24.5m).ToListAsync();
        Assert.Equal(new[] { 1 }, rows.Select(r => r.Id).ToArray());
    }

    // Count fast path with a predicate.
    [Fact]
    public async Task Decimal_equality_count_matches_trailing_zero_scale()
    {
        using var ctx = NewCtx();
        var n = await ctx.Query<PriceRow>().CountAsync(p => p.Price == 24.5m);
        Assert.Equal(1, n);
    }

    // Diagnostic: a projection defeats the simple-Where fast path, forcing the full translator.
    // If this passes, the full translator is correct and the fix is to defer decimal equality from
    // the fast paths; if it fails, the bug is broader (ETSV / parameter binding).
    [Fact]
    public async Task Full_translator_decimal_equality_matches_trailing_zero_scale()
    {
        using var ctx = NewCtx();
        var ids = await ctx.Query<PriceRow>().Where(p => p.Price == 24.5m).Select(p => new { p.Id }).ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(r => r.Id).ToArray());
    }
}
