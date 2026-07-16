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
/// Contract for integer value fidelity through the BULK insert path (bulk-path matrix cell).
///
/// Boundary values of every integer type bulk-insert with storage BYTE-IDENTICAL to the direct
/// insert path (verified by joining sibling tables written by each path) and read back exactly.
/// The ulong range contract holds through bulk exactly as it does directly: values in
/// [0, long.MaxValue] round-trip, and a value above long.MaxValue FAILS LOUD with a
/// <see cref="NormException"/> (the direct path's <see cref="NormUsageException"/>, surfaced
/// through the bulk pipeline) with NOTHING persisted from the failed batch - atomic, no partial
/// silent writes.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkIntegerFidelityContractTests
{
    [Table("BulkIntFidelity")]
    private sealed class BulkE
    {
        [Key] public int Id { get; set; }
        public uint U32 { get; set; }
        public ushort U16 { get; set; }
        public byte B8 { get; set; }
        public sbyte S8 { get; set; }
        public short S16 { get; set; }
        public long L64 { get; set; }
    }

    [Table("DirIntFidelity")]
    private sealed class DirE
    {
        [Key] public int Id { get; set; }
        public uint U32 { get; set; }
        public ushort U16 { get; set; }
        public byte B8 { get; set; }
        public sbyte S8 { get; set; }
        public short S16 { get; set; }
        public long L64 { get; set; }
    }

    [Table("BulkUlongFidelity")]
    private sealed class BulkU
    {
        [Key] public int Id { get; set; }
        public ulong U { get; set; }
    }

    private static readonly (int Id, uint U32, ushort U16, byte B8, sbyte S8, short S16, long L64)[] Rows =
    {
        (1, uint.MinValue, ushort.MinValue, byte.MinValue, sbyte.MinValue, short.MinValue, long.MinValue),
        (2, uint.MaxValue, ushort.MaxValue, byte.MaxValue, sbyte.MaxValue, short.MaxValue, long.MaxValue),
        (3, (uint)int.MaxValue + 1, 32768, 128, -1, -1, -1),
    };

    private static (SqliteConnection cn, DbContext ctx) Fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkIntFidelity (Id INTEGER PRIMARY KEY, U32 INTEGER NOT NULL, U16 INTEGER NOT NULL, B8 INTEGER NOT NULL, S8 INTEGER NOT NULL, S16 INTEGER NOT NULL, L64 INTEGER NOT NULL);" +
                "CREATE TABLE DirIntFidelity  (Id INTEGER PRIMARY KEY, U32 INTEGER NOT NULL, U16 INTEGER NOT NULL, B8 INTEGER NOT NULL, S8 INTEGER NOT NULL, S16 INTEGER NOT NULL, L64 INTEGER NOT NULL);" +
                "CREATE TABLE BulkUlongFidelity (Id INTEGER PRIMARY KEY, U INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Bulk_inserted_boundaries_match_the_direct_path_byte_for_byte_and_read_back_exactly()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            await ctx.BulkInsertAsync(Rows.Select(r => new BulkE
            {
                Id = r.Id, U32 = r.U32, U16 = r.U16, B8 = r.B8, S8 = r.S8, S16 = r.S16, L64 = r.L64,
            }).ToList());
            foreach (var r in Rows)
                await ctx.InsertAsync(new DirE
                {
                    Id = r.Id, U32 = r.U32, U16 = r.U16, B8 = r.B8, S8 = r.S8, S16 = r.S16, L64 = r.L64,
                });

            // Storage identity: every bulk row equals its direct sibling column-for-column in SQL.
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT COUNT(*) FROM BulkIntFidelity a JOIN DirIntFidelity b ON a.Id=b.Id AND a.U32=b.U32 AND a.U16=b.U16 AND a.B8=b.B8 AND a.S8=b.S8 AND a.S16=b.S16 AND a.L64=b.L64;";
                Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
            }

            var back = ((INormQueryable<BulkE>)ctx.Query<BulkE>()).AsNoTracking().OrderBy(e => e.Id).ToList();
            for (int i = 0; i < Rows.Length; i++)
            {
                Assert.Equal(Rows[i].U32, back[i].U32);
                Assert.Equal(Rows[i].U16, back[i].U16);
                Assert.Equal(Rows[i].B8, back[i].B8);
                Assert.Equal(Rows[i].S8, back[i].S8);
                Assert.Equal(Rows[i].S16, back[i].S16);
                Assert.Equal(Rows[i].L64, back[i].L64);
            }
        }
    }

    [Fact]
    public async Task Bulk_ulong_holds_the_range_contract_and_fails_loud_atomically()
    {
        var (cn, ctx) = Fresh();
        using (ctx)
        {
            // In range: exact through bulk.
            await ctx.BulkInsertAsync(new[] { new BulkU { Id = 1, U = 9_000_000_000_000_000_000UL } });
            var back = ((INormQueryable<BulkU>)ctx.Query<BulkU>()).AsNoTracking()
                .Where(x => x.Id == 1).Select(x => x.U).Single();
            Assert.Equal(9_000_000_000_000_000_000UL, back);

            // Above long.MaxValue inside a batch: fail loud (usage error surfaced through the bulk
            // pipeline) and persist NOTHING from the failed batch.
            var ex = await Assert.ThrowsAnyAsync<NormException>(() => ctx.BulkInsertAsync(new[]
            {
                new BulkU { Id = 10, U = 1UL },
                new BulkU { Id = 11, U = ulong.MaxValue },
            }));
            Assert.Contains("ulong", ex.Message, StringComparison.OrdinalIgnoreCase);

            using var c = cn.CreateCommand();
            c.CommandText = "SELECT COUNT(*) FROM BulkUlongFidelity WHERE Id >= 10;";
            Assert.Equal(0, Convert.ToInt32(c.ExecuteScalar()));
        }
    }
}
