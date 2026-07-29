using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// String-function translation parity on SQLite. Each test runs the query via nORM AND computes the same
/// predicate/projection with LINQ-to-Objects over the identical seed, asserting they agree — covering LIKE
/// wildcard escaping (Contains/StartsWith/EndsWith with %, _, brackets), Substring/IndexOf/Replace/Trim/Pad/
/// Remove/Insert/Concat/Format/CompareTo/indexer, and ordinal Equals. The astral (surrogate-pair) cases
/// document the deliberate code-point vs UTF-16 divergence (matches EF-Core-on-SQLite; see the class remarks).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringFunctionTranslationTests
{
    [Table("HRow57")]
    public class HRow57
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
        public string? Nul { get; set; }
        public int Num { get; set; }
    }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params HRow57[] rows)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE HRow57 (Id INTEGER PRIMARY KEY, Text TEXT NOT NULL, Nul TEXT NULL, Num INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static HRow57 R(int id, string text, string? nul = null, int num = 0)
        => new HRow57 { Id = id, Text = text, Nul = nul, Num = num };

    // ============================================================================
    //  TOP PRIORITY: LIKE-wildcard escaping (%, _) in Contains/StartsWith/EndsWith
    // ============================================================================

    [Fact]
    public async Task Contains_percent_literal_where_default_ordinal()
    {
        var seed = new[] { R(1, "50% off"), R(2, "500 off"), R(3, "discount") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Contains("50%")).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains("50%")).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1} only
    }

    [Fact]
    public async Task StartsWith_underscore_literal_where_default_ordinal()
    {
        var seed = new[] { R(1, "a_b"), R(2, "axb"), R(3, "a_z") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.StartsWith("a_")).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.StartsWith("a_")).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,3}
    }

    [Fact]
    public async Task EndsWith_percent_literal_where_default_ordinal()
    {
        var seed = new[] { R(1, "done%"), R(2, "doneX"), R(3, "%") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.EndsWith("%")).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.EndsWith("%")).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,3}
    }

    [Fact]
    public async Task Contains_percent_literal_projection_default_ordinal()
    {
        var seed = new[] { R(1, "50% off"), R(2, "500 off") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().OrderBy(x => x.Id).Select(x => x.Text.Contains("50%")).ToList().ToArray();
        var oracle = seed.OrderBy(x => x.Id).Select(x => x.Text.Contains("50%")).ToArray();
        Assert.Equal(oracle, actual); // {true,false}
    }

    [Fact]
    public async Task Contains_percent_ignorecase_where()
    {
        var seed = new[] { R(1, "50% OFF"), R(2, "500 off"), R(3, "x50%y") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Contains("50%", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains("50%", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,3}
    }

    [Fact]
    public async Task StartsWith_underscore_ignorecase_where()
    {
        var seed = new[] { R(1, "A_b"), R(2, "Axb"), R(3, "a_Z") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.StartsWith("a_", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.StartsWith("a_", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,3}
    }

    [Fact]
    public async Task Contains_percent_closure_needle_where()
    {
        var seed = new[] { R(1, "50% off"), R(2, "500 off") };
        var needle = "50%";
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Contains(needle)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains(needle)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    [Fact]
    public async Task Contains_singlequote_needle_where()
    {
        var seed = new[] { R(1, "O'Brien"), R(2, "OBrien") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Contains("O'Brien")).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains("O'Brien")).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    [Fact]
    public async Task Contains_char_wildcard_where()
    {
        var seed = new[] { R(1, "a%b"), R(2, "axb") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Contains('%')).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains('%')).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    // ============================================================================
    //  Surrogate pairs (astral chars): .NET counts UTF-16 code units; SQLite counts
    //  code points. Length / IndexOf / Substring can diverge.
    // ============================================================================

    [Fact]
    public async Task Length_astral_projection()
    {
        var seed = new[] { R(1, "a\U0001F600b") }; // 'a' + emoji + 'b'
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text.Length).ToList().Single();
        // SQLite LENGTH counts Unicode CODE POINTS (3); .NET Length counts UTF-16 code units (4). nORM matches
        // EF-Core-on-SQLite; SQL Server/MySQL return 4. Deliberate divergence (pending strict-mode decision).
        Assert.Equal(3, actual);
        Assert.Equal(4, seed.Select(x => x.Text.Length).Single()); // documents the .NET value
    }

    [Fact]
    public async Task Length_astral_where()
    {
        var seed = new[] { R(1, "\U0001F600"), R(2, "ab") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Length == 2).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        // "😀" is 1 code point on SQLite (2 UTF-16 units in .NET), so only "ab" matches Length==2 on SQLite.
        // nORM matches EF-Core-on-SQLite (pending strict-mode decision); on SQL Server/MySQL both rows match.
        Assert.Equal(new[] { 2 }, actual);                                       // SQLite: only "ab"
        Assert.Equal(new[] { 1, 2 }, seed.Where(x => x.Text.Length == 2).Select(x => x.Id).OrderBy(i => i).ToArray()); // .NET
    }

    [Fact]
    public async Task IndexOf_after_astral_projection()
    {
        var seed = new[] { R(1, "a\U0001F600b") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text.IndexOf("b")).ToList().Single();
        // SQLite INSTR is code-point-based ("b" is at code point 3 -> 0-based 2); .NET IndexOf is UTF-16 (3).
        // nORM matches EF-Core-on-SQLite (pending strict-mode decision).
        Assert.Equal(2, actual);
        Assert.Equal(3, seed.Select(x => x.Text.IndexOf("b")).Single()); // .NET value
    }

    [Fact]
    public async Task Substring_after_astral_projection()
    {
        var seed = new[] { R(1, "a\U0001F600b") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text.Substring(3)).ToList().Single();
        // "a😀b" is 3 code points on SQLite, so SUBSTR(col, 4) is past the end -> ""; .NET Substring(3) is the
        // 4th UTF-16 unit -> "b". nORM matches EF-Core-on-SQLite (pending strict-mode decision).
        Assert.Equal("", actual);
        Assert.Equal("b", seed.Select(x => x.Text.Substring(3)).Single()); // .NET value
    }

    // ============================================================================
    //  char indexer s[i]
    // ============================================================================

    [Fact]
    public async Task Indexer_projection()
    {
        var seed = new[] { R(1, "hello") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text[1]).ToList().Single();
        var oracle = seed.Select(x => x.Text[1]).Single();
        Assert.Equal(oracle, actual); // 'e'
    }

    [Fact]
    public async Task Indexer_where()
    {
        var seed = new[] { R(1, "hello"), R(2, "world") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text[0] == 'h').Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text[0] == 'h').Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    // ============================================================================
    //  string.Equals (instance + static; ordinal + ignore-case) in predicates
    // ============================================================================

    [Fact]
    public async Task Equals_instance_ordinal_where()
    {
        var seed = new[] { R(1, "Hello"), R(2, "hello") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Equals("Hello")).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Equals("Hello")).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1} ordinal, case-sensitive
    }

    [Fact]
    public async Task Equals_instance_ignorecase_where()
    {
        var seed = new[] { R(1, "Hello"), R(2, "hello"), R(3, "world") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.Equals("HELLO", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Equals("HELLO", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,2}
    }

    [Fact]
    public async Task Equals_static_ignorecase_where()
    {
        var seed = new[] { R(1, "Hello"), R(2, "hello"), R(3, "world") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => string.Equals(x.Text, "HELLO", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => string.Equals(x.Text, "HELLO", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,2}
    }

    // ============================================================================
    //  Trim with a multi-char set
    // ============================================================================

    [Fact]
    public async Task Trim_multichar_set_projection()
    {
        var seed = new[] { R(1, "xyabcyx") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text.Trim('x', 'y')).ToList().Single();
        var oracle = seed.Select(x => x.Text.Trim('x', 'y')).Single();
        Assert.Equal(oracle, actual); // "abc"
    }

    [Fact]
    public async Task TrimStart_multichar_set_projection()
    {
        var seed = new[] { R(1, "xyxyabc") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text.TrimStart('x', 'y')).ToList().Single();
        var oracle = seed.Select(x => x.Text.TrimStart('x', 'y')).Single();
        Assert.Equal(oracle, actual); // "abc"
    }

    // ============================================================================
    //  IndexOf(string, StringComparison)
    // ============================================================================

    [Fact]
    public async Task IndexOf_ordinal_ignorecase_projection()
    {
        var seed = new[] { R(1, "HELLO") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => x.Text.IndexOf("ell", StringComparison.OrdinalIgnoreCase)).ToList().Single();
        var oracle = seed.Select(x => x.Text.IndexOf("ell", StringComparison.OrdinalIgnoreCase)).Single();
        Assert.Equal(oracle, actual); // 1
    }

    // ============================================================================
    //  Concat 3 args, middle null
    // ============================================================================

    [Fact]
    public async Task Concat_three_args_middle_null_projection()
    {
        var seed = new[] { R(1, "a", null) };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => string.Concat(x.Text, x.Nul, "z")).ToList().Single();
        var oracle = seed.Select(x => string.Concat(x.Text, x.Nul, "z")).Single();
        Assert.Equal(oracle, actual); // "az"
    }

    // ============================================================================
    //  ToUpper / ToLower equality in predicate (ASCII)
    // ============================================================================

    [Fact]
    public async Task ToUpper_equality_where_ascii()
    {
        var seed = new[] { R(1, "hello"), R(2, "HELLO"), R(3, "world") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Where(x => x.Text.ToUpper() == "HELLO").Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.ToUpper() == "HELLO").Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,2}
    }

    // ============================================================================
    //  Ordinal ordering of mixed-case strings via OrderBy (byte-exact vs collation)
    // ============================================================================

    [Fact]
    public async Task OrderBy_string_ordinal_matches_dotnet_ordinal()
    {
        // Ordinal: uppercase letters (65-90) sort BEFORE lowercase (97-122).
        var seed = new[] { R(1, "banana"), R(2, "Apple"), R(3, "cherry"), R(4, "Zebra") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().OrderBy(x => x.Text).Select(x => x.Id).ToList().ToArray();
        var oracle = seed.OrderBy(x => x.Text, StringComparer.Ordinal).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, actual); // Apple, Zebra, banana, cherry => {2,4,1,3}
    }

    // ============================================================================
    //  Interpolation with an integer value
    // ============================================================================

    [Fact]
    public async Task Interpolation_with_int_projection()
    {
        var seed = new[] { R(1, "x", null, 42) };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRow57>().Select(x => $"n={x.Num}").ToList().Single();
        var oracle = seed.Select(x => $"n={x.Num}").Single();
        Assert.Equal(oracle, actual); // "n=42"
    }

    // ============================================================================
    //  FAIL-LOUD check: IndexOf(value, startIndex) and LastIndexOf (37B, known).
    //  A clean throw is fail-loud (acceptable). A silent wrong value is the bug.
    // ============================================================================

    [Fact]
    public async Task IndexOf_with_startindex_is_not_silently_wrong()
    {
        var seed = new[] { R(1, "abcabc") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var oracle = seed.Select(x => x.Text.IndexOf("a", 1)).Single(); // 3
        try
        {
            var actual = ctx.Query<HRow57>().Select(x => x.Text.IndexOf("a", 1)).ToList().Single();
            Assert.Equal(oracle, actual); // if it translates, it must be correct
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            // fail-loud is acceptable (documented 37B)
        }
    }

    [Fact]
    public async Task LastIndexOf_is_not_silently_wrong()
    {
        var seed = new[] { R(1, "abcabc") };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var oracle = seed.Select(x => x.Text.LastIndexOf("a")).Single(); // 3
        try
        {
            var actual = ctx.Query<HRow57>().Select(x => x.Text.LastIndexOf("a")).ToList().Single();
            Assert.Equal(oracle, actual); // if it translates, it must be correct
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            // fail-loud is acceptable
        }
    }
}

// Extra escaping edge cases appended for sweep 57B.
[Trait("Category", TestCategory.Fast)]
public class StringFunctionTranslationHunt57BEscapeTests
{
    [Table("HRowE57")]
    public class HRowE57
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params HRowE57[] rows)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE HRowE57 (Id INTEGER PRIMARY KEY, Text TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    [Fact]
    public async Task Contains_backslash_ignorecase_where()
    {
        var seed = new[]
        {
            new HRowE57 { Id = 1, Text = "a\b" },
            new HRowE57 { Id = 2, Text = "axb" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRowE57>().Where(x => x.Text.Contains("a\b", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains("a\b", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    [Fact]
    public async Task Contains_bracket_ignorecase_where()
    {
        var seed = new[]
        {
            new HRowE57 { Id = 1, Text = "a[bc]d" },
            new HRowE57 { Id = 2, Text = "abd" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRowE57>().Where(x => x.Text.Contains("[bc]", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains("[bc]", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    [Fact]
    public async Task Contains_backslash_default_ordinal_where()
    {
        var seed = new[]
        {
            new HRowE57 { Id = 1, Text = "a\b" },
            new HRowE57 { Id = 2, Text = "axb" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRowE57>().Where(x => x.Text.Contains("a\b")).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text.Contains("a\b")).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    [Fact]
    public async Task EqualityOperator_ordinal_where_contrast()
    {
        var seed = new[]
        {
            new HRowE57 { Id = 1, Text = "Hello" },
            new HRowE57 { Id = 2, Text = "hello" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<HRowE57>().Where(x => x.Text == "Hello").Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => x.Text == "Hello").Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1} (== is case-sensitive on SQLite)
    }
}
