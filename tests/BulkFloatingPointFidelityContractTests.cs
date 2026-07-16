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
/// Contract for double/float value fidelity through the BULK insert path (bulk-path matrix cell).
///
/// Finite doubles and floats (17-digit values, the smallest subnormal, mid-subnormals, Max/Min) and
/// +/-Infinity bulk-insert BIT-EXACTLY, with REAL storage identical to the direct path. NaN in a
/// bulk batch FAILS LOUD (the same driver rejection the direct path surfaces, through the bulk
/// pipeline) with NOTHING persisted from the failed batch - atomic, no partial silent writes.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkFloatingPointFidelityContractTests
{
    [Table("BulkFloatFidelity")]
    private sealed class BulkF
    {
        [Key] public int Id { get; set; }
        public double D { get; set; }
        public float S { get; set; }
    }

    [Table("DirFloatFidelity")]
    private sealed class DirF
    {
        [Key] public int Id { get; set; }
        public double D { get; set; }
        public float S { get; set; }
    }

    private static readonly (int Id, double D, float S)[] Rows =
    {
        (1, 0.1 + 0.2, 0.1f + 0.2f),
        (2, double.Epsilon, float.Epsilon),
        (3, 1e-310, 1e-42f),
        (4, double.MaxValue, float.MaxValue),
        (5, double.MinValue, float.MinValue),
        (6, double.PositiveInfinity, float.NegativeInfinity),
    };

    private static (SqliteConnection cn, DbContext ctx) Fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkFloatFidelity (Id INTEGER PRIMARY KEY, D REAL NOT NULL, S REAL NOT NULL);" +
                "CREATE TABLE DirFloatFidelity  (Id INTEGER PRIMARY KEY, D REAL NOT NULL, S REAL NOT NULL);";
            c.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Bulk_floats_are_bit_exact_and_match_the_direct_path_storage()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(Rows.Select(r => new BulkF { Id = r.Id, D = r.D, S = r.S }).ToList());
            foreach (var (id, d, s) in Rows) await ctx.InsertAsync(new DirF { Id = id, D = d, S = s });

            // Storage identity: bulk REAL equals direct REAL for every row (incl. Infinity).
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT COUNT(*) FROM BulkFloatFidelity a JOIN DirFloatFidelity b ON a.Id = b.Id AND a.D = b.D AND a.S = b.S;";
                Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
            }

            var back = ((INormQueryable<BulkF>)ctx.Query<BulkF>()).AsNoTracking().OrderBy(f => f.Id).ToList();
            for (int i = 0; i < Rows.Length; i++)
            {
                Assert.Equal(BitConverter.DoubleToInt64Bits(Rows[i].D), BitConverter.DoubleToInt64Bits(back[i].D));
                Assert.Equal(BitConverter.SingleToInt32Bits(Rows[i].S), BitConverter.SingleToInt32Bits(back[i].S));
            }
        }
    }

    [Fact]
    public async Task NaN_in_a_bulk_batch_fails_loud_and_persists_nothing()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => ctx.BulkInsertAsync(new[]
            {
                new BulkF { Id = 1, D = 1.0, S = 1f },
                new BulkF { Id = 2, D = double.NaN, S = 2f },   // not representable - must fail loud
            }));
            Assert.Contains("NaN", ex.Message);

            using var c = cn.CreateCommand();
            c.CommandText = "SELECT COUNT(*) FROM BulkFloatFidelity;";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));   // atomic: nothing from the batch
        }
    }
}
