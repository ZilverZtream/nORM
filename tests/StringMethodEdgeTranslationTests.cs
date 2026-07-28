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
/// Adversarial sweep: less-common System.String method translation on SQLite.
/// Each test asserts the SQL translation against the LINQ-to-Objects oracle
/// (the same lambda over an in-memory List). Silent-wrong = a WRONG value or a
/// dropped/added row without throwing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringMethodEdgeTranslationTests
{
    [Table("SRow")]
    public class SRow
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
        public string? Nul { get; set; }
    }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params SRow[] rows)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SRow (Id INTEGER PRIMARY KEY, Text TEXT NOT NULL, Nul TEXT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    // ---------- Substring ----------

    [Fact]
    public async Task Substring_start_only_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Substring(2)).ToList().Single();
        var oracle = seed.Select(x => x.Text.Substring(2)).Single();
        Assert.Equal(oracle, actual); // "llo"
    }

    [Fact]
    public async Task Substring_start_len_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Substring(1, 3)).ToList().Single();
        var oracle = seed.Select(x => x.Text.Substring(1, 3)).Single();
        Assert.Equal(oracle, actual); // "ell"
    }

    // ---------- IndexOf ----------

    [Fact]
    public async Task IndexOf_found_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.IndexOf("l")).ToList().Single();
        var oracle = seed.Select(x => x.Text.IndexOf("l")).Single();
        Assert.Equal(oracle, actual); // 2
    }

    [Fact]
    public async Task IndexOf_not_found_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.IndexOf("z")).ToList().Single();
        var oracle = seed.Select(x => x.Text.IndexOf("z")).Single();
        Assert.Equal(oracle, actual); // -1
    }

    [Fact]
    public async Task IndexOf_empty_needle_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.IndexOf("")).ToList().Single();
        var oracle = seed.Select(x => x.Text.IndexOf("")).Single();
        Assert.Equal(oracle, actual); // .NET => 0
    }

    [Fact]
    public async Task IndexOf_char_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.IndexOf('l')).ToList().Single();
        var oracle = seed.Select(x => x.Text.IndexOf('l')).Single();
        Assert.Equal(oracle, actual); // 2
    }

    // ---------- Replace ----------

    [Fact]
    public async Task Replace_string_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "a.b.c" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Replace(".", "-")).ToList().Single();
        var oracle = seed.Select(x => x.Text.Replace(".", "-")).Single();
        Assert.Equal(oracle, actual); // "a-b-c"
    }

    [Fact]
    public async Task Replace_with_empty_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "a.b.c" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Replace(".", "")).ToList().Single();
        var oracle = seed.Select(x => x.Text.Replace(".", "")).Single();
        Assert.Equal(oracle, actual); // "abc"
    }

    [Fact]
    public async Task Replace_char_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "a.b.c" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Replace('.', '-')).ToList().Single();
        var oracle = seed.Select(x => x.Text.Replace('.', '-')).Single();
        Assert.Equal(oracle, actual); // "a-b-c"
    }

    // ---------- PadLeft / PadRight ----------

    [Fact]
    public async Task PadLeft_default_space_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "42" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.PadLeft(5)).ToList().Single();
        var oracle = seed.Select(x => x.Text.PadLeft(5)).Single();
        Assert.Equal(oracle, actual); // "   42"
    }

    [Fact]
    public async Task PadLeft_fillchar_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "42" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.PadLeft(5, '0')).ToList().Single();
        var oracle = seed.Select(x => x.Text.PadLeft(5, '0')).Single();
        Assert.Equal(oracle, actual); // "00042"
    }

    [Fact]
    public async Task PadLeft_fillchar_star_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "42" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.PadLeft(5, '*')).ToList().Single();
        var oracle = seed.Select(x => x.Text.PadLeft(5, '*')).Single();
        Assert.Equal(oracle, actual); // "***42"
    }

    [Fact]
    public async Task PadRight_fillchar_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "42" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.PadRight(5, '.')).ToList().Single();
        var oracle = seed.Select(x => x.Text.PadRight(5, '.')).Single();
        Assert.Equal(oracle, actual); // "42..."
    }

    [Fact]
    public async Task PadLeft_already_long_enough_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "abcdef" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.PadLeft(3, '0')).ToList().Single();
        var oracle = seed.Select(x => x.Text.PadLeft(3, '0')).Single();
        Assert.Equal(oracle, actual); // "abcdef"
    }

    // ---------- Insert / Remove ----------

    [Fact]
    public async Task Remove_start_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Remove(3)).ToList().Single();
        var oracle = seed.Select(x => x.Text.Remove(3)).Single();
        Assert.Equal(oracle, actual); // "hel"
    }

    [Fact]
    public async Task Remove_start_count_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Remove(1, 2)).ToList().Single();
        var oracle = seed.Select(x => x.Text.Remove(1, 2)).Single();
        Assert.Equal(oracle, actual); // "hlo"
    }

    [Fact]
    public async Task Insert_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.Insert(2, "XY")).ToList().Single();
        var oracle = seed.Select(x => x.Text.Insert(2, "XY")).Single();
        Assert.Equal(oracle, actual); // "heXYllo"
    }

    // ---------- TrimStart/TrimEnd with char set ----------

    [Fact]
    public async Task TrimStart_charset_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "xxhello" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.TrimStart('x')).ToList().Single();
        var oracle = seed.Select(x => x.Text.TrimStart('x')).Single();
        Assert.Equal(oracle, actual); // "hello"
    }

    [Fact]
    public async Task TrimEnd_charset_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hello!!" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.TrimEnd('!')).ToList().Single();
        var oracle = seed.Select(x => x.Text.TrimEnd('!')).Single();
        Assert.Equal(oracle, actual); // "hello"
    }

    // ---------- Concat / interpolation with NULL ----------

    [Fact]
    public async Task Concat_with_null_operand_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hi", Nul = null } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => string.Concat(x.Text, x.Nul)).ToList().Single();
        var oracle = seed.Select(x => string.Concat(x.Text, x.Nul)).Single();
        Assert.Equal(oracle, actual); // "hi"
    }

    [Fact]
    public async Task Interpolation_with_null_operand_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hi", Nul = null } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => $"[{x.Nul}]{x.Text}").ToList().Single();
        var oracle = seed.Select(x => $"[{x.Nul}]{x.Text}").Single();
        Assert.Equal(oracle, actual); // "[]hi"
    }

    [Fact]
    public async Task String_plus_null_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "hi", Nul = null } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text + x.Nul + "!").ToList().Single();
        var oracle = seed.Select(x => x.Text + x.Nul + "!").Single();
        Assert.Equal(oracle, actual); // "hi!"
    }

    // ---------- IsNullOrEmpty / IsNullOrWhiteSpace in WHERE (3VL) ----------

    [Fact]
    public async Task IsNullOrEmpty_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow { Id = 1, Text = "a", Nul = null },
            new SRow { Id = 2, Text = "b", Nul = "" },
            new SRow { Id = 3, Text = "c", Nul = "x" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Where(x => string.IsNullOrEmpty(x.Nul)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => string.IsNullOrEmpty(x.Nul)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,2}
    }

    [Fact]
    public async Task IsNullOrWhiteSpace_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow { Id = 1, Text = "a", Nul = null },
            new SRow { Id = 2, Text = "b", Nul = "   " },
            new SRow { Id = 3, Text = "c", Nul = "\t\n" },
            new SRow { Id = 4, Text = "d", Nul = "x" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Where(x => string.IsNullOrWhiteSpace(x.Nul)).Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => string.IsNullOrWhiteSpace(x.Nul)).Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,2,3}
    }

    // ---------- Length ----------

    [Fact]
    public async Task Length_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow { Id = 1, Text = "ab" },
            new SRow { Id = 2, Text = "abcd" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Where(x => x.Text.Length > 2).Select(x => x.Id).ToList().ToArray();
        var oracle = seed.Where(x => x.Text.Length > 2).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, actual); // {2}
    }

    // ---------- Compare / CompareTo ----------

    [Fact]
    public async Task CompareTo_sign_projection_matches_oracle()
    {
        var seed = new[] { new SRow { Id = 1, Text = "apple", Nul = "banana" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Select(x => x.Text.CompareTo(x.Nul)).ToList().Single();
        var oracle = seed.Select(x => Math.Sign(x.Text.CompareTo(x.Nul!))).Single();
        Assert.Equal(oracle, Math.Sign(actual)); // apple < banana => -1
    }

    // ---------- StartsWith / Contains with StringComparison ----------

    [Fact]
    public async Task StartsWith_ordinal_ignorecase_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow { Id = 1, Text = "Hello" },
            new SRow { Id = 2, Text = "world" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Where(x => x.Text.StartsWith("HELL", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().ToArray();
        var oracle = seed.Where(x => x.Text.StartsWith("HELL", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    [Fact]
    public async Task Contains_ordinal_ignorecase_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow { Id = 1, Text = "HeLLo" },
            new SRow { Id = 2, Text = "world" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow>().Where(x => x.Text.Contains("ell", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToList().ToArray();
        var oracle = seed.Where(x => x.Text.Contains("ell", StringComparison.OrdinalIgnoreCase)).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }
}
