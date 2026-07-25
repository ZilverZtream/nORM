using System;
using System.Collections.Generic;
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
/// Harness improvement for the audit's F2 miss: the existing LINQ fuzzer executed each generated shape ONCE,
/// so it could never observe plan-cache poisoning — a shape that bakes the first caller's closure value into a
/// cached plan and replays it. This fuzzer runs the SAME projection expression TWICE with TWO DIFFERENT closure
/// values and checks each result against an in-memory LINQ-to-Objects oracle. If a runtime string operand were
/// inlined into a reusable plan, the second execution would return the FIRST value's result and diverge from
/// the oracle. Covers the string-match projection shapes (Contains / StartsWith / EndsWith) that F1/F2 hit.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ProjectionClosureValueReuseFuzzTests
{
    [Table("PcrRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // Deterministic xorshift RNG — no Math.Random (unavailable / non-reproducible in this environment).
    private struct Rng
    {
        private uint _s;
        public Rng(int seed) => _s = (uint)seed | 1u;
        public int Next(int maxExclusive) { _s ^= _s << 13; _s ^= _s >> 17; _s ^= _s << 5; return (int)(_s % (uint)maxExclusive); }
    }

    private const string Alphabet = "abcABC_\\%'"; // include the MySQL-hazard chars \ and ' and LIKE wildcards

    private static string RandString(ref Rng rng, int maxLen)
    {
        var len = rng.Next(maxLen + 1);
        if (len == 0) return "";
        var chars = new char[len];
        for (var i = 0; i < len; i++) chars[i] = Alphabet[rng.Next(Alphabet.Length)];
        return new string(chars);
    }

    private static (DbContext ctx, List<Row> oracle) Seed(int seed)
    {
        var rng = new Rng(seed);
        var oracle = new List<Row>();
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PcrRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var rowCount = 5 + rng.Next(20);
        for (var i = 1; i <= rowCount; i++)
        {
            var name = RandString(ref rng, 6);
            oracle.Add(new Row { Id = i, Name = name });
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO PcrRow (Id, Name) VALUES (@id, @n)";
            ins.Parameters.AddWithValue("@id", i);
            ins.Parameters.AddWithValue("@n", name);
            ins.ExecuteNonQuery();
        }
        return (new DbContext(cn, new SqliteProvider()), oracle);
    }

    // The SAME expression structure, executed with the captured `term` — nORM fingerprints structure (not the
    // closure value), so a second call with a different `term` hits the cached plan and must NOT reuse the old.
    private static List<(int Id, bool Hit)> RunNorm(DbContext ctx, int shape, string term)
        => (shape switch
        {
            0 => ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Id, Hit = r.Name.Contains(term) }),
            1 => ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Id, Hit = r.Name.StartsWith(term, StringComparison.Ordinal) }),
            _ => ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Id, Hit = r.Name.EndsWith(term, StringComparison.Ordinal) }),
        }).ToList().Select(x => (x.Id, x.Hit)).ToList();

    private static List<(int Id, bool Hit)> Oracle(List<Row> rows, int shape, string term)
        => rows.OrderBy(r => r.Id).Select(r => (r.Id, shape switch
        {
            0 => r.Name.Contains(term, StringComparison.Ordinal),
            1 => r.Name.StartsWith(term, StringComparison.Ordinal),
            _ => r.Name.EndsWith(term, StringComparison.Ordinal),
        })).ToList();

    private static void RunSeed(int seed)
    {
        var (ctx, oracle) = Seed(seed);
        using (ctx)
        {
            var rng = new Rng(unchecked(seed * 2654435761u.GetHashCode()) ^ 0x51ED);
            for (var shape = 0; shape < 3; shape++)
            {
                // Prefer terms that actually occur (a substring of some row) plus a random term, so both
                // match and non-match paths are exercised; the two terms differ so poisoning is observable.
                var pick = oracle.Count > 0 ? oracle[rng.Next(oracle.Count)].Name : "a";
                var term1 = pick.Length > 0 ? pick.Substring(0, 1 + rng.Next(pick.Length)) : RandString(ref rng, 3);
                var term2 = RandString(ref rng, 3);
                if (term1 == term2) term2 += "z"; // ensure the two runs use distinct values

                Assert.Equal(Oracle(oracle, shape, term1), RunNorm(ctx, shape, term1));
                // Second run: SAME expression shape, DIFFERENT closure value. A poisoned/reused plan fails here.
                Assert.Equal(Oracle(oracle, shape, term2), RunNorm(ctx, shape, term2));
                // Back to term1 to catch a plan that cached term2 on the second call.
                Assert.Equal(Oracle(oracle, shape, term1), RunNorm(ctx, shape, term1));
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(42)]
    [InlineData(20260725)]
    [InlineData(777001)]
    [InlineData(31337)]
    [InlineData(500009)]
    public void Projection_string_match_is_not_cache_poisoned_across_values(int seed) => RunSeed(seed);

    /// <summary>
    /// Environment-directed seed sweep: set NORM_PROJECTION_REUSE_FUZZ_SWEEP to "start:count[:dop]" to run a
    /// range for the release dry window. Unset, this is a no-op so the fixed seeds stay the baseline.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_PROJECTION_REUSE_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        var dop = parts.Length > 2 ? int.Parse(parts[2], System.Globalization.CultureInfo.InvariantCulture) : Environment.ProcessorCount;
        var options = new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, dop) };
        await Parallel.ForEachAsync(Enumerable.Range(start, count), options, (s, _) => { RunSeed(s); return ValueTask.CompletedTask; });
    }
}
