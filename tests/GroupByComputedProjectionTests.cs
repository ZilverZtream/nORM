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
/// Oracle-compared coverage for a GroupBy projection whose body is a COMPUTED scalar mixing the group
/// key and an aggregate — e.g. Select(g => (int)g.Key + ":" + g.Count()) — which previously emitted an
/// empty SELECT list ("SELECT  FROM ...") and crashed. The body is now translated as a single output
/// column with the grouping key registered.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupByComputedProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GcpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Cat { get; set; }
        public int A { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 30).Select(i => new Row
    {
        Id = i, Cat = i % 4, A = i * 2,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GcpRow (Id INTEGER PRIMARY KEY, Cat INTEGER NOT NULL, A INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO GcpRow VALUES ({r.Id},{r.Cat},{r.A});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Key_and_count_string_concat_matches_linq()
    {
        var expected = Rows.GroupBy(r => r.Cat).OrderBy(g => g.Key).Select(g => g.Key + ":" + g.Count()).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.Cat).OrderBy(g => g.Key).Select(g => g.Key + ":" + g.Count()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Key_arithmetic_with_sum_matches_linq()
    {
        // Computed numeric body mixing key and an aggregate: Key * 1000 + Sum(A).
        var expected = Rows.GroupBy(r => r.Cat).OrderBy(g => g.Key).Select(g => g.Key * 1000 + g.Sum(x => x.A)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.Cat).OrderBy(g => g.Key).Select(g => g.Key * 1000 + g.Sum(x => x.A)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Count_arithmetic_only_matches_linq()
    {
        // Aggregate-only computed body (no key): Count() * 10.
        var expected = Rows.GroupBy(r => r.Cat).OrderBy(g => g.Key).Select(g => g.Count() * 10).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.Cat).OrderBy(g => g.Key).Select(g => g.Count() * 10).ToList();
        Assert.Equal(expected, actual);
    }
}
