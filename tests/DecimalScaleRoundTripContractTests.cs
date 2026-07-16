using System;
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
/// Contract for decimal precision/scale round-trip (write-path matrix cell: decimal storage fidelity).
///
/// The NUMERIC VALUE round-trips exactly at full 28-digit precision, including
/// <see cref="decimal.MaxValue"/> / <see cref="decimal.MinValue"/>. The SCALE representation is
/// normalized by the underlying Microsoft.Data.Sqlite text binding (trailing fractional zeros
/// trimmed, integral values gain ".0"): 2m reads back as 2.0m and 10.50m as 10.5m - numerically
/// EQUAL under C# semantics (decimal == and GetHashCode are scale-insensitive) but with different
/// ToString()/GetBits. DESIGN-EXCEPTION: nORM's stored text is byte-identical to what a raw
/// SqliteParameter (and therefore EF Core, which uses the same driver) produces, and every
/// C#-observable equality semantic is preserved - Distinct merges scale variants and WHERE-equality
/// matches all of them, exactly like LINQ-to-Objects. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DecimalScaleRoundTripContractTests
{
    [Table("DecScaleContract")]
    private sealed class D { [Key] public int Id { get; set; } public decimal Val { get; set; } }

    private static readonly (int Id, decimal V)[] Rows =
    {
        (1, 2m), (2, 2.0m), (3, 2.00m), (4, 0m), (5, 10.50m), (6, 0.1m),
        (7, 1.2345678901234567890123456789m),   // 28 significant digits
        (8, decimal.MaxValue), (9, decimal.MinValue), (10, -2.50m),
    };

    private static async Task<(SqliteConnection cn, DbContext ctx)> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE DecScaleContract (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);" +
                            "CREATE TABLE DecScaleRaw (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, v) in Rows) await ctx.InsertAsync(new D { Id = id, Val = v });
        return (cn, ctx);
    }

    [Fact]
    public async Task Numeric_value_round_trips_exactly_including_28_digits_and_extremes()
    {
        var (_, ctx) = await SeedAsync();
        using (ctx)
        {
            var back = ((INormQueryable<D>)ctx.Query<D>()).AsNoTracking()
                .OrderBy(d => d.Id).Select(d => d.Val).ToList();

            // decimal == is scale-insensitive, so this asserts exact NUMERIC fidelity for all rows,
            // including the 28-significant-digit value and decimal.MaxValue/MinValue.
            Assert.Equal(Rows.Select(r => r.V).ToList(), back);
            Assert.Equal(1.2345678901234567890123456789m, back[6]);   // every digit intact
            Assert.Equal(decimal.MaxValue, back[7]);
            Assert.Equal(decimal.MinValue, back[8]);
        }
    }

    [Fact]
    public async Task Stored_text_matches_the_raw_driver_binding_byte_for_byte()
    {
        var (cn, ctx) = await SeedAsync();
        using (ctx)
        {
            // Bind the same decimals through a bare SqliteParameter - the path EF Core also uses.
            foreach (var (id, v) in Rows)
            {
                using var c = cn.CreateCommand();
                c.CommandText = "INSERT INTO DecScaleRaw (Id, Val) VALUES (@i, @v);";
                c.Parameters.AddWithValue("@i", id);
                c.Parameters.AddWithValue("@v", v);
                c.ExecuteNonQuery();
            }

            using var cmp = cn.CreateCommand();
            cmp.CommandText = "SELECT COUNT(*) FROM DecScaleContract a JOIN DecScaleRaw b ON a.Id = b.Id AND a.Val = b.Val;";
            var identical = Convert.ToInt32(cmp.ExecuteScalar());

            // The scale normalization (2 -> '2.0', 10.50 -> '10.5') is the DRIVER's canonical decimal
            // text, not a nORM transformation - nORM's write path stores exactly what the driver binds.
            Assert.Equal(Rows.Length, identical);
        }
    }

    [Fact]
    public async Task Scale_variants_stay_equal_under_query_semantics_like_csharp()
    {
        var (_, ctx) = await SeedAsync();
        using (ctx)
        {
            var q = ((INormQueryable<D>)ctx.Query<D>()).AsNoTracking();

            // C# decimal equality ignores scale: 2m == 2.0m == 2.00m. Distinct must merge them and
            // WHERE-equality must match all three, exactly like LINQ-to-Objects.
            var distinct = q.Where(d => d.Id <= 4).Select(d => d.Val).Distinct().ToList();
            Assert.Equal(Rows.Where(r => r.Id <= 4).Select(r => r.V).Distinct().Count(), distinct.Count);

            Assert.Equal(new[] { 1, 2, 3 }, q.Where(d => d.Val == 2m).Select(d => d.Id).ToList().OrderBy(x => x));
        }
    }
}
