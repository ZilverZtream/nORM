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
/// Contract for decimal value fidelity through the BULK insert path (bulk-path matrix cell).
///
/// Decimals bulk-insert with storage text BYTE-IDENTICAL to the direct insert path (the same
/// driver-canonical form: scale-normalized, full 28-digit precision) and read back numerically
/// exact - including 17th-significant-digit values (which stay DISTINCT through bulk), a
/// 28-significant-digit value, and decimal.MaxValue / MinValue.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkDecimalFidelityContractTests
{
    [Table("BulkDecFidelity")]
    private sealed class BulkD { [Key] public int Id { get; set; } public decimal Val { get; set; } }

    [Table("DirDecFidelity")]
    private sealed class DirD { [Key] public int Id { get; set; } public decimal Val { get; set; } }

    private static readonly (int Id, decimal V)[] Rows =
    {
        (1, 2m),                                  // scale-normalizes to '2.0' - must match direct
        (2, 1.00000000000000005m),                // 17th-digit pair - must stay distinct
        (3, 1.00000000000000006m),
        (4, 1.2345678901234567890123456789m),     // 28 significant digits
        (5, decimal.MaxValue),
        (6, decimal.MinValue),
        (7, -2.50m),
    };

    private static (SqliteConnection cn, DbContext ctx) Fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkDecFidelity (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);" +
                "CREATE TABLE DirDecFidelity  (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Bulk_decimal_storage_is_byte_identical_to_direct_and_reads_back_exact()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(Rows.Select(r => new BulkD { Id = r.Id, Val = r.V }).ToList());
            foreach (var (id, v) in Rows) await ctx.InsertAsync(new DirD { Id = id, Val = v });

            // Storage identity: bulk text equals direct text byte-for-byte for every row.
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT COUNT(*) FROM BulkDecFidelity a JOIN DirDecFidelity b ON a.Id = b.Id AND a.Val = b.Val;";
                Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
            }

            // Numeric-exact read-back (decimal == is scale-insensitive; 28-digit + extremes exact).
            var back = ((INormQueryable<BulkD>)ctx.Query<BulkD>()).AsNoTracking()
                .OrderBy(d => d.Id).Select(d => d.Val).ToList();
            Assert.Equal(Rows.Select(r => r.V).ToList(), back);
            Assert.Equal(1.2345678901234567890123456789m, back[3]);
            Assert.Equal(decimal.MaxValue, back[4]);
            Assert.Equal(decimal.MinValue, back[5]);
        }
    }

    [Fact]
    public async Task Bulk_written_17th_digit_values_stay_distinct()
    {
        var (_, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(Rows.Select(r => new BulkD { Id = r.Id, Val = r.V }).ToList());
            var q = ((INormQueryable<BulkD>)ctx.Query<BulkD>()).AsNoTracking();

            // The pair collapses to the same double; through bulk they must remain two distinct values.
            Assert.Equal(2, q.Where(d => d.Id == 2 || d.Id == 3).Select(d => d.Val).Distinct().ToList().Count);
            Assert.Equal(new[] { 2 }, q.Where(d => d.Val == 1.00000000000000005m).Select(d => d.Id).ToList());
            Assert.Equal(new[] { 3 }, q.Where(d => d.Val == 1.00000000000000006m).Select(d => d.Id).ToList());
        }
    }
}
