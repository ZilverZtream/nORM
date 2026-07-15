using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for floating-point ordering and edge-value fidelity (Query/LINQ matrix cell:
/// OrderBy x double/float operand, incl. NaN, +/-Infinity, -0.0).
///
/// Finite values and both infinities order CONSISTENTLY with .NET <c>double.CompareTo</c> on SQLite:
/// SQLite stores them as REAL and orders numerically. Two provider-level edge behaviours are pinned
/// as design exceptions (both identical to EF Core, which uses the same Microsoft.Data.Sqlite layer):
///
///  * NaN cannot be represented in a SQLite REAL. Attempting to store it FAILS LOUD with
///    <see cref="InvalidOperationException"/> ("Cannot store 'NaN' values.") rather than silently
///    coercing to NULL - so NaN never enters an ordered set. (+/-Infinity ARE storable and correct.)
///  * -0.0 and +0.0 compare EQUAL in SQL and -0.0 reads back as +0.0 (its sign is lost), whereas
///    .NET <c>double.CompareTo</c> orders -0.0 before +0.0. The ordering result still coincides for
///    the zeros because they are adjacent, but a tiebreak is required for deterministic placement.
///
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DoubleOrderingContractTests
{
    [Table("DblOrdContract")]
    private sealed class D { [Key] public int Id { get; set; } public double Val { get; set; } }

    [Table("FltOrdContract")]
    private sealed class Fl { [Key] public int Id { get; set; } public float Val { get; set; } }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = ddl;
        c.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task OrderBy_double_including_infinities_matches_dotnet()
    {
        var rows = new (int Id, double Val)[]
        {
            (1, double.NegativeInfinity),
            (2, -1.5),
            (3, 0.5),
            (4, 1.0),
            (5, double.PositiveInfinity),
        };
        using var ctx = new DbContext(Open("CREATE TABLE DblOrdContract (Id INTEGER PRIMARY KEY, Val REAL);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new D { Id = id, Val = val });

        var norm = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().OrderBy(e => e.Val).Select(e => e.Id).ToList();
        var oracle = rows.OrderBy(r => r.Val).Select(r => r.Id).ToList();

        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, norm);
        Assert.Equal(oracle, norm);

        var desc = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().OrderByDescending(e => e.Val).Select(e => e.Id).ToList();
        Assert.Equal(rows.OrderByDescending(r => r.Val).Select(r => r.Id).ToList(), desc);
    }

    [Fact]
    public async Task OrderBy_float_including_infinities_matches_dotnet()
    {
        var rows = new (int Id, float Val)[]
        {
            (1, float.NegativeInfinity),
            (2, -1.5f),
            (3, 0.5f),
            (4, float.PositiveInfinity),
        };
        using var ctx = new DbContext(Open("CREATE TABLE FltOrdContract (Id INTEGER PRIMARY KEY, Val REAL);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new Fl { Id = id, Val = val });

        var norm = ((INormQueryable<Fl>)ctx.Query<Fl>())
            .AsNoTracking().OrderBy(e => e.Val).Select(e => e.Id).ToList();
        Assert.Equal(rows.OrderBy(r => r.Val).Select(r => r.Id).ToList(), norm);
    }

    [Fact]
    public async Task Storing_double_NaN_fails_loud_and_does_not_corrupt()
    {
        using var ctx = new DbContext(Open("CREATE TABLE DblOrdContract (Id INTEGER PRIMARY KEY, Val REAL);"),
            new SqliteProvider());

        // NaN is not representable in a SQLite REAL: nORM surfaces the provider's clear error rather
        // than silently writing NULL. This is why NaN never appears in a double OrderBy on SQLite.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.InsertAsync(new D { Id = 1, Val = double.NaN }));
        Assert.Contains("NaN", ex.Message);

        // And nothing was persisted - no corrupt/partial row.
        var count = ((INormQueryable<D>)ctx.Query<D>()).AsNoTracking().Count();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Negative_zero_compares_equal_to_positive_zero_and_loses_sign()
    {
        using var ctx = new DbContext(Open("CREATE TABLE DblOrdContract (Id INTEGER PRIMARY KEY, Val REAL);"),
            new SqliteProvider());
        await ctx.InsertAsync(new D { Id = 1, Val = -0.0 });
        await ctx.InsertAsync(new D { Id = 2, Val = 0.0 });

        // SQL treats -0.0 and +0.0 as equal, so a deterministic order needs a tiebreak. .NET
        // double.CompareTo would order -0 before +0 without one; this pins the SQL-equal contract.
        var withTiebreak = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1, 2 }, withTiebreak);

        // -0.0 does not round-trip: its sign is lost (reads back as +0.0). .NET preserves the sign.
        var back = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().OrderBy(e => e.Id).Select(e => e.Val).First();
        Assert.Equal(0.0, back);
        Assert.False(double.IsNegative(back));
    }
}
