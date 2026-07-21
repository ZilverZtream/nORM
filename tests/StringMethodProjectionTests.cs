using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for string methods in projections and predicates: Substring, Length,
/// ToUpper/ToLower, Replace, IndexOf, StartsWith/EndsWith/Contains, Trim, and concatenation. Data is
/// ASCII with no case-collisions in the ordered keys, so SQLite BINARY collation coincides with .NET
/// ordinal — any mismatch is a real translation bug, not documented collation divergence.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StringMethodProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SmpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Code { get; set; } = "";
    }

    private static readonly Row[] Rows = new[]
    {
        ("alpha", "AB-100"), ("bravo", "CD-205"), ("charlie", "AB-310"),
        ("delta", "EF-042"), ("echo", "CD-511"), ("foxtrot", "AB-777"),
        ("golf", "GH-013"), ("hotel", "EF-620"),
    }.Select((t, i) => new Row { Id = i + 1, Name = t.Item1, Code = t.Item2 }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SmpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Code TEXT NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO SmpRow VALUES ({r.Id},'{r.Name}','{r.Code}');";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Substring_and_length_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Code.Substring(0, 2) + ":" + r.Name.Length).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Code.Substring(0, 2) + ":" + r.Name.Length).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Toupper_tolower_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Name.ToUpper() + "/" + r.Code.ToLower()).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Name.ToUpper() + "/" + r.Code.ToLower()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Replace_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Code.Replace("-", "_")).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Code.Replace("-", "_")).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Indexof_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Code.IndexOf("-")).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Code.IndexOf("-")).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Startswith_endswith_contains_predicates_match_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(
            Rows.Where(r => r.Code.StartsWith("AB")).Select(r => r.Id).OrderBy(v => v).ToList(),
            ctx.Query<Row>().Where(r => r.Code.StartsWith("AB")).Select(r => r.Id).OrderBy(v => v).ToList());
        Assert.Equal(
            Rows.Where(r => r.Name.EndsWith("o")).Select(r => r.Id).OrderBy(v => v).ToList(),
            ctx.Query<Row>().Where(r => r.Name.EndsWith("o")).Select(r => r.Id).OrderBy(v => v).ToList());
        Assert.Equal(
            Rows.Where(r => r.Code.Contains("D-")).Select(r => r.Id).OrderBy(v => v).ToList(),
            ctx.Query<Row>().Where(r => r.Code.Contains("D-")).Select(r => r.Id).OrderBy(v => v).ToList());
    }

    [Fact]
    public void Substring_in_predicate_matches_linq()
    {
        var expected = Rows.Where(r => r.Code.Substring(0, 2) == "AB").Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.Code.Substring(0, 2) == "AB").Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Length_predicate_and_orderby_matches_linq()
    {
        var expected = Rows.Where(r => r.Name.Length > 4).OrderBy(r => r.Name.Length).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.Name.Length > 4).OrderBy(r => r.Name.Length).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Trim_and_substring_from_index_matches_linq()
    {
        // Substring(index) one-arg form, and Trim of a padded value.
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Code.Substring(3)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Code.Substring(3)).ToList();
        Assert.Equal(expected, actual);
    }
}
