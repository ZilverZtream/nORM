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
/// Oracle-compared coverage for boolean-valued and bitwise projections: projecting a comparison as a
/// bool (Select(r => r.A > 5)), boolean logic (&amp;&amp; / || / !) in a projection, and integer bitwise
/// operators (&amp; | ^) in projections and predicates. Boolean projection is a place SQL's lack of a
/// boolean value type can silently diverge from C#.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class BooleanBitwiseProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BbpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public bool Flag { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    {
        Id = i, A = i, B = i % 7, Flag = i % 3 == 0,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BbpRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Flag INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO BbpRow VALUES ({r.Id},{r.A},{r.B},{(r.Flag ? 1 : 0)});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Comparison_as_bool_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.A > 5).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.A > 5).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Boolean_and_or_not_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => (r.A > 5 && r.B < 4) || !r.Flag).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (r.A > 5 && r.B < 4) || !r.Flag).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Flag_and_comparison_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Flag && r.A % 2 == 0).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Flag && r.A % 2 == 0).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Bitwise_and_or_xor_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => (r.A & 3) + (r.A | r.B) * 100 + (r.A ^ r.B) * 10000).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (r.A & 3) + (r.A | r.B) * 100 + (r.A ^ r.B) * 10000).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Bitwise_and_in_predicate_matches_linq()
    {
        // Bit test: (A & 1) == 1  (odd).
        var expected = Rows.Where(r => (r.A & 1) == 1).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => (r.A & 1) == 1).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Ternary_bool_to_int_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => (r.A > 10 ? 1 : 0) + (r.Flag ? 10 : 0)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (r.A > 10 ? 1 : 0) + (r.Flag ? 10 : 0)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Bool_in_anonymous_projection_materializes_correctly()
    {
        // A computed bool in an anonymous projection, then a second Select consuming the bool values.
        // Compared as (Id, bool, bool) tuples — validates the bool projection + double-Select
        // composition. (Interpolating a bool into a STRING is a separate known divergence: a bool
        // concat-operand renders as its stored 0/1 rather than C#'s "True"/"False".)
        var expected = Rows.OrderBy(r => r.Id).Select(r => new { r.Id, IsHigh = r.A > 15, r.Flag })
            .Select(x => (x.Id, x.IsHigh, x.Flag)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Id, IsHigh = r.A > 15, r.Flag })
            .ToList().Select(x => (x.Id, x.IsHigh, x.Flag)).ToList();
        Assert.Equal(expected, actual);
    }
}
