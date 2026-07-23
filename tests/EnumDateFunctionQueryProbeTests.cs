#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probes enum handling, DateTime component extraction, and string/decimal functions in key positions
/// (GroupBy / OrderBy) against a LINQ-to-Objects oracle — the combinations that carry position-specific
/// translation risk beyond the plain WHERE/projection cases the ledger already hardens. Passing shapes
/// stay as regression keepers; a mismatch or throw is a gap to fix.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class EnumDateFunctionQueryProbeTests
{
    public enum Status { Active = 0, Inactive = 1, Pending = 2 }

    [Table("FqRow")]
    public sealed class FqRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public DateTime Created { get; set; }
        public Status St { get; set; }
        public decimal Amount { get; set; }
    }

    private static readonly FqRow[] Data =
    {
        new() { Id = 1, Name = "a",     Created = new DateTime(2023, 1, 15), St = Status.Active,   Amount = 10.25m },
        new() { Id = 2, Name = "bb",    Created = new DateTime(2023, 6, 20), St = Status.Inactive, Amount = 20.55m },
        new() { Id = 3, Name = "ccc",   Created = new DateTime(2024, 1, 10), St = Status.Active,   Amount = 30.15m },
        new() { Id = 4, Name = "dddd",  Created = new DateTime(2024, 6, 5),  St = Status.Pending,  Amount = 5.05m  },
        new() { Id = 5, Name = "ee",    Created = new DateTime(2024, 6, 25), St = Status.Active,   Amount = 40.95m },
        new() { Id = 6, Name = "ffffff",Created = new DateTime(2023, 1, 30), St = Status.Pending,  Amount = 40.95m },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FqRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Created TEXT NOT NULL, St INTEGER NOT NULL, Amount TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<FqRow>().HasKey(x => x.Id) }, ownsConnection: true);
        foreach (var r in Data) ctx.Add(new FqRow { Id = r.Id, Name = r.Name, Created = r.Created, St = r.St, Amount = r.Amount });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    private static IQueryable<FqRow> Q(DbContext ctx) => ctx.Query<FqRow>();

    [Fact]
    public void Where_enum_equality()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Where(x => x.St == Status.Active).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.Where(x => x.St == Status.Active).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Where_enum_in_set()
    {
        using var ctx = NewCtx();
        var wanted = new[] { Status.Active, Status.Pending };
        var norm = Q(ctx).Where(x => wanted.Contains(x.St)).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.Where(x => wanted.Contains(x.St)).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_enum_key_with_count()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.St).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => ((int)x.Key, x.C)).ToList();
        var oracle = Data.GroupBy(x => x.St).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => ((int)x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Projection_returns_enum()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.Id).Select(x => x.St).ToList();
        var oracle = Data.OrderBy(x => x.Id).Select(x => x.St).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_datetime_year()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.Created.Year).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.GroupBy(x => x.Created.Year).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_datetime_year_and_month_composite()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => new { x.Created.Year, x.Created.Month }).Select(g => new { g.Key.Year, g.Key.Month, C = g.Count() })
            .OrderBy(x => x.Year).ThenBy(x => x.Month).ToList().Select(x => (x.Year, x.Month, x.C)).ToList();
        var oracle = Data.GroupBy(x => new { x.Created.Year, x.Created.Month }).Select(g => new { g.Key.Year, g.Key.Month, C = g.Count() })
            .OrderBy(x => x.Year).ThenBy(x => x.Month).Select(x => (x.Year, x.Month, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Where_datetime_component()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Where(x => x.Created.Year == 2024).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.Where(x => x.Created.Year == 2024).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void OrderBy_string_length()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.Name.Length).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.OrderBy(x => x.Name.Length).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_string_substring_first_char()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.Name.Substring(0, 1)).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.GroupBy(x => x.Name.Substring(0, 1)).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_enum_with_sum_decimal()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.St).Select(g => new { g.Key, S = g.Sum(x => x.Amount) })
            .OrderBy(x => x.Key).ToList().Select(x => ((int)x.Key, x.S)).ToList();
        var oracle = Data.GroupBy(x => x.St).Select(g => new { g.Key, S = g.Sum(x => x.Amount) })
            .OrderBy(x => x.Key).Select(x => ((int)x.Key, x.S)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void OrderBy_enum_then_project()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.St).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.OrderBy(x => x.St).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Where_datetime_arithmetic_adddays()
    {
        using var ctx = NewCtx();
        var cutoff = new DateTime(2024, 6, 10);
        var norm = Q(ctx).Where(x => x.Created.AddDays(10) > cutoff).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.Where(x => x.Created.AddDays(10) > cutoff).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Projection_decimal_round()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.Id).Select(x => Math.Round(x.Amount, 1)).ToList();
        var oracle = Data.OrderBy(x => x.Id).Select(x => Math.Round(x.Amount, 1)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Where_enum_not_equal_and_string_contains()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Where(x => x.St != Status.Inactive && x.Name.Contains("f")).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.Where(x => x.St != Status.Inactive && x.Name.Contains("f")).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }
}
