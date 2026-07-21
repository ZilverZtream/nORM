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
/// Oracle-compared coverage for NULLABLE values in KEY positions — where SQL three-valued logic and
/// null-ordering can silently diverge from C#: GroupBy over a nullable key (null forms its own group),
/// Distinct over nullable scalar/anon, nullable ORDER BY (null-first parity), Count(Distinct) counting
/// null as a value, nullable equality/inequality predicates, coalesce predicates, and per-group
/// aggregates of a nullable (all-null group -> null). Each case runs the identical LINQ expression
/// against nORM (SQLite) and LINQ-to-Objects over the same seeded rows (which include null K and null S).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NullableKeyCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NkcRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int? K { get; set; }
        public string? S { get; set; }
        public int V { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 24).Select(i => new Row
    {
        Id = i,
        K = (i % 5 == 0) ? (int?)null : (i % 4),
        S = (i % 3 == 0) ? null : (i % 2 == 0 ? "abc" : "xyz"),
        V = i,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE NkcRow (Id INTEGER PRIMARY KEY, K INTEGER NULL, S TEXT NULL, V INTEGER NOT NULL);";
        foreach (var r in Rows)
        {
            var k = r.K.HasValue ? r.K.Value.ToString() : "NULL";
            var s = r.S is null ? "NULL" : $"'{r.S}'";
            cmd.CommandText += $"INSERT INTO NkcRow VALUES ({r.Id},{k},{s},{r.V});";
        }
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

    [Fact]
    public void GroupBy_nullable_int_count() => Assert_(q => q.GroupBy(r => r.K)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.Key));

    [Fact]
    public void GroupBy_nullable_string_count() => Assert_(q => q.GroupBy(r => r.S)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.Key));

    [Fact]
    public void GroupBy_nullable_int_sum() => Assert_(q => q.GroupBy(r => r.K)
        .Select(g => new { g.Key, S = g.Sum(x => x.V) }).OrderBy(x => x.Key));

    [Fact]
    public void Distinct_nullable_int() => Assert_(q => q.Select(r => r.K).Distinct().OrderBy(k => k));

    [Fact]
    public void Distinct_nullable_string() => Assert_(q => q.Select(r => r.S).Distinct().OrderBy(s => s));

    [Fact]
    public void Distinct_anon_with_nullables() => Assert_(q => q.Select(r => new { r.K, r.S }).Distinct()
        .OrderBy(x => x.K).ThenBy(x => x.S));

    [Fact]
    public void OrderBy_nullable_ascending() => Assert_(q => q.OrderBy(r => r.K).ThenBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void OrderBy_nullable_descending() => Assert_(q => q.OrderByDescending(r => r.K).ThenBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void Count_distinct_nullable_int() => Assert_(q => new[] { q.Select(r => r.K).Distinct().Count() });

    [Fact]
    public void Count_distinct_nullable_string() => Assert_(q => new[] { q.Select(r => r.S).Distinct().Count() });

    [Fact]
    public void Nullable_equality_predicate() => Assert_(q => q.Where(r => r.K == 2).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void Nullable_inequality_predicate() => Assert_(q => q.Where(r => r.K != 2).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void Nullable_is_null_predicate() => Assert_(q => q.Where(r => r.K == null).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void Nullable_coalesce_predicate() => Assert_(q => q.Where(r => (r.K ?? -1) > 1).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void GroupBy_string_max_of_nullable() => Assert_(q => q.GroupBy(r => r.S)
        .Select(g => new { g.Key, Mx = g.Max(x => x.K) }).OrderBy(x => x.Key));

    [Fact]
    public void Sum_of_nullable() => Assert_(q => new int?[] { q.Sum(r => r.K) });
}
