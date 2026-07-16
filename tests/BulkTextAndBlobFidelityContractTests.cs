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
/// Contract for string / char / byte[] value fidelity through the BULK insert path (bulk-path matrix
/// cell). Strings (astral surrogate pairs, ~100k chars, whitespace, embedded quotes/newlines), chars
/// (non-ASCII), and byte arrays (empty and 1MB) bulk-insert with storage identical to the direct
/// path, round-trip exactly, and predicates match emoji/quoted content written through bulk.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkTextAndBlobFidelityContractTests
{
    [Table("BulkTextFidelity")]
    private sealed class BulkT
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = "";
        public char C { get; set; }
        public byte[] B { get; set; } = Array.Empty<byte>();
    }

    [Table("DirTextFidelity")]
    private sealed class DirT
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = "";
        public char C { get; set; }
        public byte[] B { get; set; } = Array.Empty<byte>();
    }

    // \U0001F600 grinning face, \U0001F916 robot (astral surrogate pairs); é escaped for ASCII safety.
    private static readonly string Emoji = "café \U0001F600 \U0001F916";
    private static readonly string Quoted = "it's a \"test\"\nwith\tlines";
    private static readonly string Big = new string('x', 100_000) + "\U0001F600end";
    private static readonly byte[] BigBlob = CreateBlob();

    private static byte[] CreateBlob()
    {
        var b = new byte[1_000_000];
        new Random(7).NextBytes(b);
        return b;
    }

    private static readonly (int Id, string S, char C, byte[] B)[] Rows =
    {
        (1, Emoji, 'é', new byte[] { 0, 255, 128 }),
        (2, "", 'A', Array.Empty<byte>()),
        (3, "  padded  ", 'z', new byte[] { 42 }),
        (4, Quoted, 'Q', new byte[] { 1 }),
        (5, Big, 'q', BigBlob),
    };

    [Fact]
    public async Task Bulk_text_and_blobs_match_direct_storage_and_round_trip_exactly()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkTextFidelity (Id INTEGER PRIMARY KEY, S TEXT NOT NULL, C TEXT NOT NULL, B BLOB NOT NULL);" +
                "CREATE TABLE DirTextFidelity  (Id INTEGER PRIMARY KEY, S TEXT NOT NULL, C TEXT NOT NULL, B BLOB NOT NULL);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        await ctx.BulkInsertAsync(Rows.Select(r => new BulkT { Id = r.Id, S = r.S, C = r.C, B = r.B }).ToList());
        foreach (var (id, s, ch, b) in Rows) await ctx.InsertAsync(new DirT { Id = id, S = s, C = ch, B = b });

        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM BulkTextFidelity a JOIN DirTextFidelity b ON a.Id = b.Id AND a.S = b.S AND a.C = b.C AND a.B = b.B;";
            Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
        }

        var back = ((INormQueryable<BulkT>)ctx.Query<BulkT>()).AsNoTracking().OrderBy(t => t.Id).ToList();
        for (int i = 0; i < Rows.Length; i++)
        {
            Assert.Equal(Rows[i].S, back[i].S, StringComparer.Ordinal);
            Assert.Equal(Rows[i].C, back[i].C);
            Assert.True(Rows[i].B.AsSpan().SequenceEqual(back[i].B));
        }
        Assert.Empty(back[1].B);

        // Predicates match bulk-written emoji/quoted content.
        var q = ((INormQueryable<BulkT>)ctx.Query<BulkT>()).AsNoTracking();
        Assert.Equal(new[] { 1 }, q.Where(t => t.S == Emoji).Select(t => t.Id).ToList());
        Assert.Equal(new[] { 4 }, q.Where(t => t.S == Quoted).Select(t => t.Id).ToList());
    }
}
