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
/// Harder edge sweep of String method translation on SQLite: single-element
/// concat/format aggregates over NULL, out-of-range indices, boundary indices.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringMethodEdge2TranslationTests
{
    [Table("SRow2")]
    public class SRow2
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
        public string? Nul { get; set; }
    }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params SRow2[] rows)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SRow2 (Id INTEGER PRIMARY KEY, Text TEXT NOT NULL, Nul TEXT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    // single-hole interpolation over a NULL value: .NET => "" (Format renders null as empty).
    [Fact]
    public async Task Interpolation_single_hole_null_matches_oracle()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "hi", Nul = null } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow2>().Select(x => $"{x.Nul}").ToList().Single();
        var oracle = seed.Select(x => $"{x.Nul}").Single();
        Assert.Equal(oracle, actual); // ""
    }

    // explicit string.Format with a single placeholder over NULL: .NET => "".
    [Fact]
    public async Task Format_single_placeholder_null_matches_oracle()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "hi", Nul = null } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow2>().Select(x => string.Format("{0}", x.Nul)).ToList().Single();
        var oracle = seed.Select(x => string.Format("{0}", x.Nul)).Single();
        Assert.Equal(oracle, actual); // ""
    }

    // control: single-hole interpolation over a NON-null value must still work.
    [Fact]
    public async Task Interpolation_single_hole_nonnull_matches_oracle()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "hi", Nul = "abc" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow2>().Select(x => $"{x.Nul}").ToList().Single();
        var oracle = seed.Select(x => $"{x.Nul}").Single();
        Assert.Equal(oracle, actual); // "abc"
    }

    // Contains(char)
    [Fact]
    public async Task Contains_char_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow2 { Id = 1, Text = "hello" },
            new SRow2 { Id = 2, Text = "world" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow2>().Where(x => x.Text.Contains('e')).Select(x => x.Id).ToList().ToArray();
        var oracle = seed.Where(x => x.Text.Contains('e')).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, actual); // {1}
    }

    // Substring boundary: startIndex == Length => "".
    [Fact]
    public async Task Substring_start_equals_length_matches_oracle()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "abc" } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow2>().Select(x => x.Text.Substring(3)).ToList().Single();
        var oracle = seed.Select(x => x.Text.Substring(3)).Single();
        Assert.Equal(oracle, actual); // ""
    }

    // ---- Out-of-range indices: .NET throws ArgumentOutOfRangeException; SQLite substr()
    // clamps. Server-side SQL cannot cleanly raise a typed ArgumentOutOfRangeException, and
    // EF Core on SQLite exhibits the identical clamp — so this is a documented EF-parity
    // divergence pending a strict-mode / client-eval user decision, NOT a silent-wrong bug in
    // normal (in-range) operation. These characterization tests pin the CURRENT clamp result so
    // any accidental change is caught; if strict-mode lands, they flip to assert the throw. ----
    [Fact]
    public async Task Substring_start_out_of_range_clamps_like_ef_on_sqlite()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "abc" } };
        // .NET oracle throws; nORM/SQLite clamps start past the end to an empty string.
        Assert.ThrowsAny<ArgumentOutOfRangeException>(() => seed.Select(x => x.Text.Substring(10)).Single());
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var normResult = ctx.Query<SRow2>().Select(x => x.Text.Substring(10)).ToList().Single();
        Assert.Equal("", normResult);
    }

    [Fact]
    public async Task Substring_start_len_out_of_range_clamps_like_ef_on_sqlite()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "abc" } };
        Assert.ThrowsAny<ArgumentOutOfRangeException>(() => seed.Select(x => x.Text.Substring(1, 10)).Single());
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var normResult = ctx.Query<SRow2>().Select(x => x.Text.Substring(1, 10)).ToList().Single();
        Assert.Equal("bc", normResult); // clamps length to the remaining chars
    }

    [Fact]
    public async Task Remove_out_of_range_clamps_like_ef_on_sqlite()
    {
        var seed = new[] { new SRow2 { Id = 1, Text = "abc" } };
        Assert.ThrowsAny<ArgumentOutOfRangeException>(() => seed.Select(x => x.Text.Remove(1, 10)).Single());
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var normResult = ctx.Query<SRow2>().Select(x => x.Text.Remove(1, 10)).ToList().Single();
        Assert.Equal("a", normResult); // keeps the prefix before start; clamps the removed span
    }
}
