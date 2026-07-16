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
/// Contract for enum value fidelity through the BULK insert path (bulk-path matrix cell).
///
/// Enums bulk-insert by their underlying numeric value with storage identical to the direct path:
/// defined members, UNDEFINED numeric values ((E)999 stays (E)999 - .NET Enum semantics), and
/// [Flags] combinations all round-trip verbatim through bulk. A ulong-backed enum follows the ulong
/// range contract through bulk: an in-range member round-trips exactly and a member above
/// long.MaxValue FAILS LOUD atomically (nothing persisted from the failed batch).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkEnumFidelityContractTests
{
    private enum IntE { Neg = -5, A = 1, B = 42 }
    [Flags] private enum FlagsE { None = 0, F1 = 1, F2 = 2, F4 = 4 }
    private enum ULongE : ulong { InRange = 9_000_000_000_000_000_000, Big = 10_000_000_000_000_000_000 }

    [Table("BulkEnumFidelity")]
    private sealed class BulkE
    {
        [Key] public int Id { get; set; }
        public IntE I { get; set; }
        public FlagsE F { get; set; }
    }

    [Table("DirEnumFidelity")]
    private sealed class DirE
    {
        [Key] public int Id { get; set; }
        public IntE I { get; set; }
        public FlagsE F { get; set; }
    }

    [Table("BulkEnumUlongFidelity")]
    private sealed class BulkU { [Key] public int Id { get; set; } public ULongE U { get; set; } }

    private static readonly (int Id, IntE I, FlagsE F)[] Rows =
    {
        (1, IntE.Neg, FlagsE.None),
        (2, IntE.B, FlagsE.F1 | FlagsE.F4),
        (3, (IntE)999, (FlagsE)999),   // undefined values
    };

    private static (SqliteConnection cn, DbContext ctx) Fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkEnumFidelity (Id INTEGER PRIMARY KEY, I INTEGER NOT NULL, F INTEGER NOT NULL);" +
                "CREATE TABLE DirEnumFidelity  (Id INTEGER PRIMARY KEY, I INTEGER NOT NULL, F INTEGER NOT NULL);" +
                "CREATE TABLE BulkEnumUlongFidelity (Id INTEGER PRIMARY KEY, U INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Bulk_enums_match_direct_storage_and_round_trip_undefined_and_flags_values()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(Rows.Select(r => new BulkE { Id = r.Id, I = r.I, F = r.F }).ToList());
            foreach (var (id, i, f) in Rows) await ctx.InsertAsync(new DirE { Id = id, I = i, F = f });

            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT COUNT(*) FROM BulkEnumFidelity a JOIN DirEnumFidelity b ON a.Id = b.Id AND a.I = b.I AND a.F = b.F;";
                Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
            }

            var back = ((INormQueryable<BulkE>)ctx.Query<BulkE>()).AsNoTracking().OrderBy(e => e.Id).ToList();
            for (int i = 0; i < Rows.Length; i++)
            {
                Assert.Equal(Rows[i].I, back[i].I);
                Assert.Equal(Rows[i].F, back[i].F);
            }
            Assert.Equal((IntE)999, back[2].I);   // undefined stays undefined through bulk
        }
    }

    [Fact]
    public async Task Bulk_ulong_backed_enum_holds_the_range_contract_atomically()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(new[] { new BulkU { Id = 1, U = ULongE.InRange } });
            Assert.Equal(ULongE.InRange,
                ((INormQueryable<BulkU>)ctx.Query<BulkU>()).AsNoTracking().Where(x => x.Id == 1).Select(x => x.U).Single());

            var ex = await Assert.ThrowsAnyAsync<NormException>(() => ctx.BulkInsertAsync(new[]
            {
                new BulkU { Id = 10, U = ULongE.InRange },
                new BulkU { Id = 11, U = ULongE.Big },   // above long.MaxValue - must fail loud
            }));
            Assert.Contains("ulong", ex.Message, StringComparison.OrdinalIgnoreCase);

            using var c = cn.CreateCommand();
            c.CommandText = "SELECT COUNT(*) FROM BulkEnumUlongFidelity WHERE Id >= 10;";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));   // atomic
        }
    }
}
