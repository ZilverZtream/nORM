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
/// Contract for string / char / byte[] write/read round-trip fidelity (write-path matrix cell).
///
/// Strings round-trip ordinally exact: BMP non-ASCII, astral surrogate pairs (emoji), the empty
/// string, ~100k-char values, leading/trailing whitespace, and embedded quotes/newlines/tabs are all
/// preserved verbatim, and WHERE-equality/Contains match on quoted and astral-plane content. chars
/// round-trip including non-ASCII and U+FFFD. byte arrays round-trip byte-exactly including the
/// EMPTY array (length 0, not null) and a 1MB blob.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TextAndBlobRoundTripContractTests
{
    [Table("TextBlobRtContract")]
    private sealed class T
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = "";
        public char C { get; set; }
        public byte[] B { get; set; } = Array.Empty<byte>();
    }

    // Unicode escapes keep this file ASCII-safe: \u00E9 = e-acute, \U0001F600 = grinning face,
    // \U0001F916 = robot face (both astral, i.e. surrogate pairs in UTF-16), \uFFFD = replacement char.
    private static readonly string Emoji = "caf\u00E9 \U0001F600 \U0001F916";
    private static readonly string Quoted = "it's a \"test\" with 'quotes'\nand\tnew\r\nlines";
    private static readonly string Big = new string('x', 100_000) + "\u00E9\U0001F600end";
    private static readonly byte[] BigBlob = CreateBlob();

    private static byte[] CreateBlob()
    {
        var b = new byte[1_000_000];
        new Random(42).NextBytes(b);
        return b;
    }

    private static readonly (int Id, string S, char C, byte[] B)[] Rows =
    {
        (1, Emoji, 'A', new byte[] { 0, 1, 255, 128 }),
        (2, "", '\u00E9', Array.Empty<byte>()),
        (3, "  leading and trailing  ", '\uFFFD', new byte[] { 42 }),
        (4, Quoted, 'Z', new byte[] { 1 }),
        (5, Big, 'q', BigBlob),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE TextBlobRtContract (Id INTEGER PRIMARY KEY, S TEXT NOT NULL, C TEXT NOT NULL, B BLOB NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, s, ch, b) in Rows) await ctx.InsertAsync(new T { Id = id, S = s, C = ch, B = b });
        return ctx;
    }

    [Fact]
    public async Task Strings_chars_and_blobs_round_trip_exactly()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();

        Assert.Equal(Rows.Length, back.Count);
        for (int i = 0; i < Rows.Length; i++)
        {
            Assert.Equal(Rows[i].S, back[i].S, StringComparer.Ordinal);   // incl. spaces, emoji, 100k
            Assert.Equal(Rows[i].C, back[i].C);
            Assert.True(Rows[i].B.AsSpan().SequenceEqual(back[i].B));      // byte-exact incl. 1MB
        }
        Assert.Empty(back[1].B);   // the EMPTY array reads back as length 0, not null
        Assert.Equal("", back[1].S);
    }

    [Fact]
    public async Task Predicates_match_quoted_and_astral_content_exactly()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking();

        Assert.Equal(new[] { 4 }, q.Where(t => t.S == Quoted).Select(t => t.Id).ToList());
        Assert.Equal(new[] { 1 }, q.Where(t => t.S == Emoji).Select(t => t.Id).ToList());
        Assert.Equal(new[] { 1, 5 },
            q.Where(t => t.S.Contains("\U0001F600")).Select(t => t.Id).ToList().OrderBy(x => x));
    }
}
