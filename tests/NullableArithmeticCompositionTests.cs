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
/// Oracle-compared coverage for nullable-arithmetic null propagation — a common silent-wrong surface where
/// SQL's NULL arithmetic (any NULL operand → NULL) must match C#'s lifted-nullable arithmetic (any null →
/// null). Covers +, -, *, %, coalesce chains, conditionals, predicates (null excluded), aggregates over
/// nullable arithmetic, and a nullable-keyed GroupBy, over int? and decimal? columns. Each case runs the
/// identical LINQ expression against nORM (SQLite) and LINQ-to-Objects over rows that include nulls.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NullableArithmeticCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NacRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int? A { get; set; }
        public int? B { get; set; }
        public decimal? M { get; set; }
    }

    private static readonly Row[] Seed = Enumerable.Range(1, 10).Select(i => new Row
    {
        Id = i,
        A = (i % 3 == 0) ? (int?)null : i,
        B = (i % 4 == 0) ? (int?)null : (i * 2),
        M = (i % 5 == 0) ? (decimal?)null : (i * 1.5m),
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE NacRow (Id INTEGER PRIMARY KEY, A INTEGER NULL, B INTEGER NULL, M TEXT NULL);";
        foreach (var r in Seed)
        {
            var a = r.A.HasValue ? r.A.Value.ToString() : "NULL";
            var b = r.B.HasValue ? r.B.Value.ToString() : "NULL";
            var m = r.M.HasValue ? $"'{r.M.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)}'" : "NULL";
            cmd.CommandText += $"INSERT INTO NacRow VALUES ({r.Id},{a},{b},{m});";
        }
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Seed.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Add_two_nullables() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A + r.B));
    [Fact] public void Add_nullable_constant() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A + 5));
    [Fact] public void Multiply_two_nullables() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A * r.B));
    [Fact] public void Subtract_two_nullables() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A - r.B));
    [Fact] public void Coalesced_addition() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (r.A ?? 0) + (r.B ?? 0)));
    [Fact] public void Decimal_nullable_add() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.M + 1.5m));
    [Fact] public void Decimal_times_int_nullable() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.M * r.A));
    [Fact] public void Coalesce_chain() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A ?? r.B ?? -1));
    [Fact] public void Conditional_hasvalue() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A.HasValue ? r.A.Value + 1 : -1));
    [Fact] public void Conditional_nullable_compare() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.A > r.B ? 1 : 0));
    [Fact] public void Coalesce_over_sum() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (r.A + r.B) ?? -99));

    [Fact] public void Predicate_sum_gt() => Assert_(q => q.Where(r => r.A + r.B > 10).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_nullable_lt() => Assert_(q => q.Where(r => r.A < r.B).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_coalesced_sum() => Assert_(q => q.Where(r => (r.A ?? 0) + (r.B ?? 0) > 8).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_scaled_equals() => Assert_(q => q.Where(r => r.A * 2 == r.B).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact] public void Sum_coalesced() => Assert_(q => new[] { q.Sum(r => (r.A ?? 0) + (r.B ?? 0)) });
    [Fact] public void Sum_nullable() => Assert_(q => new int?[] { q.Sum(r => r.A + r.B) });
    [Fact] public void Max_nullable_product() => Assert_(q => new int?[] { q.Max(r => r.A * r.B) });
    [Fact] public void GroupBy_coalesced_modulo() => Assert_(q => q.GroupBy(r => (r.A ?? 0) % 3).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()));
}
