using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A search term that itself contains LIKE metacharacters (% or _) must be matched literally.
/// If nORM fails to escape the term before embedding it in a LIKE pattern, Contains("50%") silently
/// matches "500..." and Contains("a_b") matches "axb" — a silent wrong-result (and mild injection)
/// bug. Covers both the constant-pattern path (folded literal) and the variable-pattern path
/// (parameterized value, escaped at runtime via the provider's REPLACE-based ESCAPE translation).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class LikeWildcardEscapingCorrectnessTests
{
    [Table("Doc")]
    private class Doc
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Doc (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "INSERT INTO Doc (Id, Name) VALUES " +
                "(1,'50% off'),(2,'500 dollars'),(3,'a_b'),(4,'axb'),(5,'plain');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static List<string> Names(IEnumerable<Doc> docs) =>
        docs.Select(d => d.Name).OrderBy(n => n).ToList();

    [Fact]
    public void Contains_constant_percent_matches_literally()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        var hits = Names(ctx.Query<Doc>().Where(d => d.Name.Contains("50%")).ToList());
        Assert.Equal(new[] { "50% off" }, hits);   // NOT "500 dollars"
    }

    [Fact]
    public void Contains_constant_underscore_matches_literally()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        var hits = Names(ctx.Query<Doc>().Where(d => d.Name.Contains("a_b")).ToList());
        Assert.Equal(new[] { "a_b" }, hits);        // NOT "axb"
    }

    [Fact]
    public void StartsWith_and_EndsWith_constant_escape_wildcards()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(new[] { "50% off" },
            Names(ctx.Query<Doc>().Where(d => d.Name.StartsWith("50%")).ToList()));
        Assert.Equal(new[] { "a_b" },
            Names(ctx.Query<Doc>().Where(d => d.Name.EndsWith("_b")).ToList())); // literal "_b", not "xb"
    }

    [Fact]
    public void Contains_variable_percent_matches_literally()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // Captured local -> parameterized pattern (runtime ESCAPE path, not the folded-literal path).
        var term = "50%";
        var hits = Names(ctx.Query<Doc>().Where(d => d.Name.Contains(term)).ToList());
        Assert.Equal(new[] { "50% off" }, hits);
    }

    [Fact]
    public void Contains_variable_underscore_matches_literally()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        var term = "a_b";
        var hits = Names(ctx.Query<Doc>().Where(d => d.Name.Contains(term)).ToList());
        Assert.Equal(new[] { "a_b" }, hits);
    }
}
