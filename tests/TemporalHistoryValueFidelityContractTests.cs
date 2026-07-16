using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for temporal history-row VALUE fidelity over boundary values (temporal matrix cell).
///
/// The reconstruction fuzzer's value domain is small ints/strings; this cell pins the boundary
/// shapes: a 17th-significant-digit decimal, an offset-suffixed DateTimeOffset, an astral-pair
/// string, and a binary blob. After an UPDATE, the history row must hold the OLD values byte-exact
/// (raw SELECT from the _History table) while the live row holds the new ones, and AsOf between the
/// versions must reconstruct the old values exactly through the materializer. After a DELETE, AsOf
/// before the delete still sees the last values byte-exact while the current query is empty.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalHistoryValueFidelityContractTests
{
    [Table("TempFidelity_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public decimal M { get; set; }
        public DateTimeOffset Dto { get; set; }
        public string S { get; set; } = "";
        public byte[] B { get; set; } = Array.Empty<byte>();
    }

    private static readonly decimal M1 = 1.00000000000000005m;
    private static readonly decimal M2 = 2.00000000000000007m;
    private static readonly DateTimeOffset Dto1 = new(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(5));
    private static readonly DateTimeOffset Dto2 = new(2021, 1, 1, 8, 30, 0, TimeSpan.FromHours(-5));
    private static readonly string S1 = "café \U0001F600 v1";
    private static readonly string S2 = "v2 \U0001F916";
    private static readonly byte[] B1 = { 0, 255, 128, 1 };
    private static readonly byte[] B2 = { 42, 42 };

    private static async Task<(SqliteConnection cn, DbContext ctx, Row row)> SeedTwoVersionsAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TempFidelity_Row (Id INTEGER PRIMARY KEY, M TEXT NOT NULL, Dto TEXT NOT NULL, S TEXT NOT NULL, B BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.EnableTemporalVersioning();
        var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var row = new Row { Id = 1, M = M1, Dto = Dto1, S = S1, B = B1 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();
        await Task.Delay(150);
        row.M = M2; row.Dto = Dto2; row.S = S2; row.B = B2;
        await ctx.SaveChangesAsync();
        return (cn, ctx, row);
    }

    private static DateTime BetweenVersions(SqliteConnection cn)
    {
        using var c = cn.CreateCommand();
        c.CommandText = "SELECT __ValidTo FROM TempFidelity_Row_History WHERE __Operation IS NOT NULL AND __ValidTo <> '9999-12-31' LIMIT 1;";
        var to = (string)c.ExecuteScalar()!;
        return DateTime.Parse(to, System.Globalization.CultureInfo.InvariantCulture).AddMilliseconds(-1);
    }

    [Fact]
    public async Task History_row_holds_old_boundary_values_byte_exact_and_asof_reconstructs_them()
    {
        var (cn, ctx, _) = await SeedTwoVersionsAsync();
        using (ctx)
        {
            // Raw fidelity: the v1 history row holds the OLD stored forms byte-exact.
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT M, Dto, S, B FROM TempFidelity_Row_History WHERE M = '1.00000000000000005';";
                using var r = c.ExecuteReader();
                Assert.True(r.Read(), "v1 history row not found by its exact decimal text");
                Assert.Equal("2020-06-15 12:00:00+05:00", r.GetString(1));
                Assert.Equal(S1, r.GetString(2));
                Assert.True(((byte[])r.GetValue(3)).AsSpan().SequenceEqual(B1));
            }

            // The live row holds the NEW values.
            var live = (await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Where(x => x.Id == 1).ToListAsync()).Single();
            Assert.Equal(M2, live.M);
            Assert.Equal(Dto2, live.Dto);
            Assert.Equal(S2, live.S);
            Assert.True(live.B.AsSpan().SequenceEqual(B2));

            // AsOf between the versions reconstructs v1 exactly through the materializer.
            var old = (await ctx.Query<Row>().AsOf(BetweenVersions(cn)).Where(x => x.Id == 1).ToListAsync()).Single();
            Assert.Equal(M1, old.M);
            Assert.Equal(Dto1, old.Dto);            // offset preserved through history
            Assert.Equal(S1, old.S);                // astral pair preserved
            Assert.True(old.B.AsSpan().SequenceEqual(B1));
        }
    }

    [Fact]
    public async Task AsOf_before_a_delete_sees_the_last_values_byte_exact()
    {
        var (cn, ctx, row) = await SeedTwoVersionsAsync();
        using (ctx)
        {
            await Task.Delay(150);
            var beforeDelete = DateTime.UtcNow;
            await Task.Delay(150);
            ctx.Remove(row);
            await ctx.SaveChangesAsync();

            // Current query: empty.
            Assert.Empty(await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Where(x => x.Id == 1).ToListAsync());

            // AsOf before the delete: the final (v2) values, exact.
            var last = (await ctx.Query<Row>().AsOf(beforeDelete).Where(x => x.Id == 1).ToListAsync()).Single();
            Assert.Equal(M2, last.M);
            Assert.Equal(Dto2, last.Dto);
            Assert.Equal(S2, last.S);
            Assert.True(last.B.AsSpan().SequenceEqual(B2));
        }
    }
}
