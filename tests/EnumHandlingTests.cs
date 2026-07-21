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
/// Oracle-compared coverage for enum columns across projection, predicate, GroupBy key, Contains, and
/// ordering. Enums store as their integer value and require int-lift handling that can differ between
/// the projection, predicate, and key visitors (a known bug vector).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class EnumHandlingTests
{
    public enum Status { Pending = 0, Active = 1, Closed = 2, Archived = 3 }

    [System.ComponentModel.DataAnnotations.Schema.Table("EnhRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public Status State { get; set; }
        public int A { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 24).Select(i => new Row
    {
        Id = i,
        State = (Status)(i % 4),
        A = i * 2,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EnhRow (Id INTEGER PRIMARY KEY, State INTEGER NOT NULL, A INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO EnhRow VALUES ({r.Id},{(int)r.State},{r.A});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Enum_equality_predicate_matches_linq()
    {
        var expected = Rows.Where(r => r.State == Status.Active).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.State == Status.Active).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_comparison_predicate_matches_linq()
    {
        // Enum ordering comparison (State >= Closed).
        var expected = Rows.Where(r => r.State >= Status.Closed).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.State >= Status.Closed).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.State).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.State).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_cast_to_int_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => (int)r.State).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (int)r.State).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_groupby_key_matches_linq()
    {
        // GroupBy an enum column, projecting the enum Key and the Count separately (the natural shape).
        var expected = Rows.GroupBy(r => r.State).Select(g => new { g.Key, N = g.Count() })
            .OrderBy(x => x.Key).Select(x => ((int)x.Key, x.N)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.State).Select(g => new { g.Key, N = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => ((int)x.Key, x.N)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_contains_predicate_matches_linq()
    {
        var wanted = new[] { Status.Active, Status.Archived };
        var expected = Rows.Where(r => wanted.Contains(r.State)).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => wanted.Contains(r.State)).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_conditional_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.State == Status.Closed ? "closed" : "open").ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.State == Status.Closed ? "closed" : "open").ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Enum_orderby_matches_linq()
    {
        var expected = Rows.OrderByDescending(r => r.State).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderByDescending(r => r.State).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }
}
