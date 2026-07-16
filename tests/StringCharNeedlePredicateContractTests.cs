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
/// Pins the char-needle string predicate contract: Contains(char) / StartsWith(char) /
/// EndsWith(char) translate through the same handlers as their string-needle overloads,
/// so they match ordinally (case-sensitive), keep NULL rows under negation, re-bind a
/// closure-captured needle on every execution of a cached plan, treat LIKE
/// metacharacters as literals, and work in projections. Before the handlers accepted
/// the char overloads, this shape translated only on SQLite (whose TranslateFunction
/// covers Contains/StartsWith/EndsWith) and threw on every server provider.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringCharNeedlePredicateContractTests
{
    [Table("CharNeedle_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }
    }

    private static DbContext NewCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CharNeedle_Test (Id INTEGER PRIMARY KEY, Name TEXT NULL);" +
                "INSERT INTO CharNeedle_Test VALUES (1,'alpha'),(2,'Beta'),(3,'gamma'),(4,NULL),(5,'AZURE');";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static List<int> Ids(IQueryable<Row> q) => q.Select(x => x.Id).ToList();

    [Fact]
    public void Char_needle_matches_ordinally()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        Assert.Equal(new[] { 5 }, Ids(ctx.Query<Row>().Where(x => x.Name!.Contains('A')).OrderBy(x => x.Id)));
        Assert.Equal(new[] { 1, 2, 3 }, Ids(ctx.Query<Row>().Where(x => x.Name!.Contains('a')).OrderBy(x => x.Id)));
        Assert.Equal(new[] { 2 }, Ids(ctx.Query<Row>().Where(x => x.Name!.StartsWith('B')).OrderBy(x => x.Id)));
        Assert.Empty(Ids(ctx.Query<Row>().Where(x => x.Name!.StartsWith('b'))));
        Assert.Equal(new[] { 5 }, Ids(ctx.Query<Row>().Where(x => x.Name!.EndsWith('E')).OrderBy(x => x.Id)));
        Assert.Equal(new[] { 1, 2, 3 }, Ids(ctx.Query<Row>().Where(x => x.Name!.EndsWith('a')).OrderBy(x => x.Id)));
    }

    [Fact]
    public void Char_needle_with_ignore_case_comparison_folds_case()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // 'z' ignore-case matches only AZURE ('Z'); ordinal 'z' matches nothing.
        Assert.Equal(new[] { 5 }, Ids(ctx.Query<Row>()
            .Where(x => x.Name!.Contains('z', StringComparison.OrdinalIgnoreCase)).OrderBy(x => x.Id)));
        Assert.Empty(Ids(ctx.Query<Row>().Where(x => x.Name!.Contains('z'))));
    }

    [Fact]
    public void Negated_char_needle_keeps_null_rows()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // A null string matches nothing, so the negation is TRUE for the NULL row —
        // the same C#-semantics rescue as negated string-needle predicates.
        Assert.Equal(new[] { 4, 5 }, Ids(ctx.Query<Row>().Where(x => !x.Name!.Contains('a')).OrderBy(x => x.Id)));
    }

    [Fact]
    public void Closure_captured_char_needle_rebinds_across_cached_plans()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var needle = 'g';
        Assert.Equal(new[] { 3 }, Ids(ctx.Query<Row>().Where(x => x.Name!.Contains(needle)).OrderBy(x => x.Id)));
        needle = 'B';
        Assert.Equal(new[] { 2 }, Ids(ctx.Query<Row>().Where(x => x.Name!.Contains(needle)).OrderBy(x => x.Id)));
    }

    [Fact]
    public void Like_metacharacter_char_needle_matches_literally()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CharNeedle_Test VALUES (6,'100%'),(7,'100x'),(8,'a_b'),(9,'axb');";
            cmd.ExecuteNonQuery();
        }

        Assert.Equal(new[] { 6 }, Ids(ctx.Query<Row>().Where(x => x.Name!.Contains('%')).OrderBy(x => x.Id)));
        Assert.Equal(new[] { 6 }, Ids(ctx.Query<Row>().Where(x => x.Name!.EndsWith('%')).OrderBy(x => x.Id)));
        Assert.Equal(new[] { 8 }, Ids(ctx.Query<Row>().Where(x => x.Name!.Contains('_')).OrderBy(x => x.Id)));
    }

    [Fact]
    public void Char_needle_predicate_projects_as_bool_column()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var rows = ctx.Query<Row>().Where(x => x.Name != null).OrderBy(x => x.Id)
            .Select(x => new { x.Id, HasA = x.Name!.Contains('a') })
            .ToList();
        Assert.Equal(new[] { (1, true), (2, true), (3, true), (5, false) },
            rows.Select(r => (r.Id, r.HasA)).ToArray());
    }
}
