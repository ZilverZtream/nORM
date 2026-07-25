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
/// Adversarial hostile-condition sweep for the text write/read path (A+ dimension C), beyond the
/// standard <see cref="TextAndBlobRoundTripContractTests"/> corpus. These payloads are the classic
/// silent-corruption edges a native driver can mangle: an EMBEDDED NUL (a C-string terminator can
/// truncate "a\0b" to "a"), an all-NUL string, BiDi/zero-width control characters, a mid-string BOM,
/// and a DECOMPOSED grapheme (must NOT be silently NFC-normalized to its precomposed form). Every
/// payload must round-trip ordinally exact through the nORM write path AND match on a WHERE-equality
/// predicate — a truncation or normalization would be exactly the silent-wrong data loss the project
/// bars.
///
/// The file stays pure ASCII: every adversarial code point is built at runtime from its integer value
/// via <see cref="Cp"/>, so no source escape sequence can be corrupted by tooling.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TextEncodingAdversarialFidelityTests
{
    [Table("TextAdvContract")]
    private sealed class T
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = "";
    }

    // Build a string from raw code points (ASCII char literals or hex ints). Sidesteps escape-sequence
    // mangling: the source is pure ASCII and the actual code points materialize only at runtime.
    private static string Cp(params int[] codes) => new string(codes.Select(c => (char)c).ToArray());

    private const int NUL = 0x0000;
    private const int ZWJ = 0x200D;   // zero-width joiner
    private const int ZWNJ = 0x200C;  // zero-width non-joiner
    private const int RLO = 0x202E;   // right-to-left override
    private const int PDF = 0x202C;   // pop directional formatting
    private const int BOM = 0xFEFF;   // byte-order mark / zero-width no-break space
    private const int ACUTE = 0x0301; // combining acute accent

    private static readonly (int Id, string S, string Label)[] Rows =
    {
        (1, Cp('a', NUL, 'b'), "embedded NUL"),
        (2, Cp(NUL, NUL, NUL), "all NUL"),
        (3, Cp(NUL, 'l', 'e', 'a', 'd'), "leading NUL"),
        (4, Cp('t', 'r', 'a', 'i', 'l', NUL), "trailing NUL"),
        (5, Cp('a', ZWJ, 'b', ZWNJ, 'c'), "zero-width joiner/non-joiner"),
        (6, Cp(RLO, 'a', 'b', 'c', PDF), "RTL override + pop"),
        (7, Cp('x', BOM, 'y'), "mid-string BOM"),
        (8, Cp('e', ACUTE), "decomposed grapheme (must stay 2 code units)"),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE TextAdvContract (Id INTEGER PRIMARY KEY, S TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, s, _) in Rows) await ctx.InsertAsync(new T { Id = id, S = s });
        return ctx;
    }

    [Fact]
    public async Task Adversarial_encoding_payloads_round_trip_ordinally_exact()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();

        Assert.Equal(Rows.Length, back.Count);
        for (int i = 0; i < Rows.Length; i++)
            Assert.True(string.Equals(Rows[i].S, back[i].S, StringComparison.Ordinal),
                $"payload '{Rows[i].Label}' corrupted on round-trip: " +
                $"wrote {Describe(Rows[i].S)} read {Describe(back[i].S)}");

        // The decomposed grapheme must not have been NFC-normalized (would collapse 2 code units to 1).
        Assert.Equal(2, back.Single(r => r.Id == 8).S.Length);
    }

    [Fact]
    public async Task Adversarial_encoding_payloads_match_on_where_equality()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking();

        // WHERE-equality binds the same adversarial value as a parameter; a bind-side truncation would
        // return the wrong row set even if storage were exact.
        foreach (var (id, s, _) in Rows)
            Assert.Equal(new[] { id }, q.Where(t => t.S == s).Select(t => t.Id).ToList());
    }

    [Fact]
    public async Task Adversarial_payloads_survive_a_tracked_update_round_trip()
    {
        // The UPDATE path binds the column value as a parameter independently of INSERT, so it is a
        // distinct truncation surface: a row written benign then changed to an adversarial value via
        // SaveChanges must persist the new value byte-exact, not silently truncate at a NUL.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE TextAdvContract (Id INTEGER PRIMARY KEY, S TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        foreach (var (id, _, _) in Rows) await ctx.InsertAsync(new T { Id = id, S = "benign" });

        var tracked = ((INormQueryable<T>)ctx.Query<T>()).OrderBy(t => t.Id).ToList();
        for (int i = 0; i < tracked.Count; i++) tracked[i].S = Rows[i].S;
        await ctx.SaveChangesAsync();

        var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();
        Assert.Equal(Rows.Length, back.Count);
        for (int i = 0; i < Rows.Length; i++)
            Assert.True(string.Equals(Rows[i].S, back[i].S, StringComparison.Ordinal),
                $"payload '{Rows[i].Label}' corrupted on UPDATE round-trip: " +
                $"wrote {Describe(Rows[i].S)} read {Describe(back[i].S)}");
    }

    private static string Describe(string s) =>
        $"[{s.Length}] " + string.Join(" ", s.Select(ch => ((int)ch).ToString("X4")));
}
