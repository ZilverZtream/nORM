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
/// Contract for double/float write/read round-trip fidelity (write-path matrix cell).
///
/// Every finite double and float round-trips BIT-EXACTLY through nORM's write path and materializer
/// (SQLite REAL is an IEEE-754 double, and float widens/narrows losslessly through it): values
/// requiring all 17 significant digits (0.1 + 0.2), the smallest subnormal (<see cref="double.Epsilon"/>),
/// mid-range subnormals, the smallest normal, and <see cref="double.MaxValue"/>/<see cref="double.MinValue"/>
/// all read back with identical bits, and a WHERE-equality predicate at an exact double (including a
/// subnormal) matches exactly the written row. (NaN fail-loud and +/-Infinity round-trip are pinned by
/// <c>DoubleOrderingContractTests</c>.)
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class FloatingPointRoundTripContractTests
{
    [Table("FloatRtContract")]
    private sealed class F
    {
        [Key] public int Id { get; set; }
        public double D { get; set; }
        public float S { get; set; }
    }

    private static readonly (int Id, double D, float S)[] Rows =
    {
        (1, 0.1, 0.1f),
        (2, 1.0 / 3.0, 1.0f / 3.0f),
        (3, 0.1 + 0.2, 0.1f + 0.2f),                    // needs all 17 / 9 digits
        (4, double.Epsilon, float.Epsilon),              // smallest subnormal
        (5, 2.2250738585072014e-308, 1.17549435e-38f),   // smallest normal
        (6, double.MaxValue, float.MaxValue),
        (7, double.MinValue, float.MinValue),
        (8, 1e-310, 1e-42f),                             // mid subnormal
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE FloatRtContract (Id INTEGER PRIMARY KEY, D REAL NOT NULL, S REAL NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, d, s) in Rows) await ctx.InsertAsync(new F { Id = id, D = d, S = s });
        return ctx;
    }

    [Fact]
    public async Task Doubles_and_floats_round_trip_bit_exactly()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<F>)ctx.Query<F>()).AsNoTracking().OrderBy(f => f.Id).ToList();

        Assert.Equal(Rows.Length, back.Count);
        for (int i = 0; i < Rows.Length; i++)
        {
            Assert.Equal(BitConverter.DoubleToInt64Bits(Rows[i].D), BitConverter.DoubleToInt64Bits(back[i].D));
            Assert.Equal(BitConverter.SingleToInt32Bits(Rows[i].S), BitConverter.SingleToInt32Bits(back[i].S));
        }
    }

    [Fact]
    public async Task Where_equality_at_exact_doubles_matches_the_written_row()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<F>)ctx.Query<F>()).AsNoTracking();

        Assert.Equal(new[] { 1 }, q.Where(f => f.D == 0.1).Select(f => f.Id).ToList());
        var sum = 0.1 + 0.2;   // 0.30000000000000004 - matches only the exact bit pattern
        Assert.Equal(new[] { 3 }, q.Where(f => f.D == sum).Select(f => f.Id).ToList());
        Assert.Equal(new[] { 4 }, q.Where(f => f.D == double.Epsilon).Select(f => f.Id).ToList());
    }
}
