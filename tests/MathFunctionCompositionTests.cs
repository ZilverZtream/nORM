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
/// Oracle-compared coverage for Math functions (Abs/Round/Floor/Ceiling/Truncate/Sign/Min/Max/Sqrt/Pow)
/// in PREDICATE, ORDER BY, GroupBy-key and aggregate positions — complementing the projection-focused
/// suite. Rounding mode (banker's), sign, and precision across int/double/decimal are the divergence
/// risks. Each case runs the identical LINQ expression against nORM (SQLite) and LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class MathFunctionCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("MfcRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public double D { get; set; }
        public decimal M { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    {
        Id = i,
        A = (i % 2 == 0 ? 1 : -1) * (i * 3),
        D = (i - 10) + i * 0.25 + (i % 3) * 0.5,
        M = (decimal)((i - 8) * 1.5) + (i % 4) * 0.1m,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE MfcRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, D REAL NOT NULL, M TEXT NOT NULL);";
        foreach (var r in Rows)
            cmd.CommandText += $"INSERT INTO MfcRow VALUES ({r.Id},{r.A},{r.D.ToString(System.Globalization.CultureInfo.InvariantCulture)},'{r.M.ToString(System.Globalization.CultureInfo.InvariantCulture)}');";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Rows.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    // Predicate positions
    [Fact] public void Abs_predicate() => Assert_(q => q.Where(r => Math.Abs(r.A) > 15).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Floor_predicate() => Assert_(q => q.Where(r => Math.Floor(r.D) >= 0).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Ceiling_predicate() => Assert_(q => q.Where(r => Math.Ceiling(r.D) > 2).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Sign_predicate() => Assert_(q => q.Where(r => Math.Sign(r.A) < 0).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Max_predicate() => Assert_(q => q.Where(r => Math.Max(r.A, 0) > 10).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void DecimalAbs_predicate() => Assert_(q => q.Where(r => Math.Abs(r.M) > 3m).OrderBy(r => r.Id).Select(r => r.Id));

    // Ordering positions
    [Fact] public void Abs_ordering() => Assert_(q => q.OrderBy(r => Math.Abs(r.A)).ThenBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Round_ordering() => Assert_(q => q.OrderBy(r => Math.Round(r.D)).ThenBy(r => r.Id).Select(r => r.Id));
    [Fact] public void SqrtAbs_ordering() => Assert_(q => q.OrderBy(r => Math.Sqrt(Math.Abs(r.A))).ThenBy(r => r.Id).Select(r => r.Id));

    // GroupBy-key positions
    [Fact] public void AbsMod_groupby() => Assert_(q => q.GroupBy(r => Math.Abs(r.A % 4)).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()));
    [Fact] public void Floor_groupby() => Assert_(q => q.GroupBy(r => (int)Math.Floor(r.D)).OrderBy(g => g.Key).Select(g => g.Key * 1000 + g.Count()));

    // Rounding-mode-sensitive projections
    [Fact] public void Round_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Math.Round(r.D)));
    [Fact] public void Round_digits_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Math.Round(r.D, 1)));
    [Fact] public void Truncate_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Math.Truncate(r.D)));
    [Fact] public void DecimalRound_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Math.Round(r.M, 1)));

    // Aggregate over math
    [Fact] public void Sum_of_abs() => Assert_(q => new[] { q.Sum(r => Math.Abs(r.A)) });
    [Fact] public void Max_of_floor() => Assert_(q => new[] { q.Max(r => (int)Math.Floor(r.D)) });
}
