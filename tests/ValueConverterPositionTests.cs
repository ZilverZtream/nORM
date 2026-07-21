using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for a value-converter column (Priority enum stored as a string via a custom
/// converter) across predicate, projection, GroupBy key, Contains, and ordering. A converter binds the
/// provider representation; the predicate visitor and projection visitor must apply it consistently, or
/// a WHERE would compare against the wrong representation while a SELECT reads the raw value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ValueConverterPositionTests
{
    public enum Priority { Low, Medium, High, Critical }

    private sealed class PriorityConverter : ValueConverter<Priority, string>
    {
        public override object? ConvertToProvider(Priority p) => p.ToString();
        public override object? ConvertFromProvider(string s) => (Priority)Enum.Parse(typeof(Priority), s);
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("VcpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public Priority Prio { get; set; }
        public int A { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    {
        Id = i,
        Prio = (Priority)(i % 4),
        A = i * 2,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Stored as the enum NAME (string) via the converter below.
            cmd.CommandText = "CREATE TABLE VcpRow (Id INTEGER PRIMARY KEY, Prio TEXT NOT NULL, A INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO VcpRow VALUES ({r.Id},'{r.Prio}',{r.A});";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property(r => r.Prio).HasConversion(new PriorityConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Converter_equality_predicate_matches_linq()
    {
        var expected = Rows.Where(r => r.Prio == Priority.High).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.Prio == Priority.High).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Converter_projection_roundtrips()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Prio).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Prio).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Converter_contains_predicate_matches_linq()
    {
        var wanted = new[] { Priority.Low, Priority.Critical };
        var expected = Rows.Where(r => wanted.Contains(r.Prio)).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => wanted.Contains(r.Prio)).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Converter_groupby_key_groups_correctly()
    {
        // Grouping by a converter column produces the right keys and counts. Compared as an
        // order-independent set: nORM orders a converter column by its STORED (string) representation,
        // whereas LINQ OrderBy(enum) uses the enum value — a documented converter-ordering divergence
        // (EF Core behaves the same). Sorting both sides by the enum value client-side isolates the
        // grouping correctness from that ordering difference.
        var expected = Rows.GroupBy(r => r.Prio).Select(g => (Key: g.Key, N: g.Count()))
            .OrderBy(x => (int)x.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.Prio).Select(g => new { g.Key, N = g.Count() })
            .ToList().Select(x => (Key: x.Key, N: x.N)).OrderBy(x => (int)x.Key).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Converter_in_conditional_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Prio == Priority.Critical ? "urgent" : "normal").ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Prio == Priority.Critical ? "urgent" : "normal").ToList();
        Assert.Equal(expected, actual);
    }
}
