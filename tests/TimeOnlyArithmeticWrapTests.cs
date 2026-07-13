using System;
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
/// .NET TimeOnly arithmetic wraps around midnight: 23:30 + 2.5h is 02:00, and negative
/// deltas wrap backwards. These pin the wrap semantics for TimeOnly.AddHours /
/// AddMinutes / Add(TimeSpan) in projections and predicates on the SQLite provider;
/// TimeSpan equality and grouping stay tick-exact (TEXT storage is canonical), and an
/// empty Contains list matches nothing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TimeOnlyArithmeticWrapTests
{
    [Table("TimeWrap_Shift")]
    private class Shift
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public TimeOnly Start { get; set; }
        public TimeSpan Dur { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TimeWrap_Shift (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Start TEXT NOT NULL,
                    Dur TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        var big = TimeSpan.FromDays(10000);
        ctx.Add(new Shift { Start = new TimeOnly(23, 30, 0), Dur = big });
        ctx.Add(new Shift { Start = new TimeOnly(1, 15, 0), Dur = big + TimeSpan.FromTicks(1) });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return (cn, ctx);
    }

    [Fact]
    public void AddHours_projection_wraps_midnight()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Shift>().OrderBy(s => s.Id).Select(s => s.Start.AddHours(2.5)).ToList();
        Assert.Equal(new[] { new TimeOnly(2, 0, 0), new TimeOnly(3, 45, 0) }, actual);
    }

    [Fact]
    public void AddHours_negative_projection_wraps_backwards()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Shift>().OrderBy(s => s.Id).Select(s => s.Start.AddHours(-3)).ToList();
        Assert.Equal(new[] { new TimeOnly(20, 30, 0), new TimeOnly(22, 15, 0) }, actual);
    }

    [Fact]
    public void AddMinutes_projection_wraps()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Shift>().OrderBy(s => s.Id).Select(s => s.Start.AddMinutes(45)).ToList();
        Assert.Equal(new[] { new TimeOnly(0, 15, 0), new TimeOnly(2, 0, 0) }, actual);
    }

    [Fact]
    public void Add_timespan_projection_wraps()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var delta = TimeSpan.FromHours(1);
        var actual = ctx.Query<Shift>().OrderBy(s => s.Id).Select(s => s.Start.Add(delta)).ToList();
        Assert.Equal(new[] { new TimeOnly(0, 30, 0), new TimeOnly(2, 15, 0) }, actual);
    }

    [Fact]
    public void AddHours_predicate_wraps()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var cutoff = new TimeOnly(3, 0, 0);
        var ids = ctx.Query<Shift>().Where(s => s.Start.AddHours(2.5) < cutoff).Select(s => s.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void TimeSpan_equality_and_grouping_stay_tick_exact()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var big = TimeSpan.FromDays(10000);
        Assert.Equal(new[] { 1 }, ctx.Query<Shift>().Where(s => s.Dur == big).Select(s => s.Id).ToList());
        Assert.Equal(2, ctx.Query<Shift>().GroupBy(s => s.Dur).Select(g => g.Count()).ToList().Count);
    }

    [Fact]
    public void Contains_on_empty_list_matches_nothing_and_negation_everything()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var empty = new System.Collections.Generic.List<int>();
        Assert.Empty(ctx.Query<Shift>().Where(s => empty.Contains(s.Id)).ToList());
        Assert.Equal(2, ctx.Query<Shift>().Where(s => !empty.Contains(s.Id)).ToList().Count);
    }
}
