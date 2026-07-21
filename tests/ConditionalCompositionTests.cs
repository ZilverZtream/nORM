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
/// Oracle-compared coverage for conditional (ternary) compositions — the expression-tree-legal way to
/// express switch-like logic (C# <c>switch</c> expressions are forbidden in expression trees, so they
/// never reach a query provider). Covers nested / deep ternaries, enum dispatch, nullable branches,
/// boolean-valued results, and ternaries in projection, predicate and GroupBy-key positions, including
/// coalesce and arithmetic inside branches. Each case runs the identical LINQ expression against nORM
/// (SQLite) and LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ConditionalCompositionTests
{
    public enum Kind { A = 0, B = 1, C = 2, D = 3 }

    [System.ComponentModel.DataAnnotations.Schema.Table("CocRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int V { get; set; }
        public Kind K { get; set; }
        public int? NV { get; set; }
    }

    private static readonly Row[] Seed = Enumerable.Range(1, 12).Select(i => new Row
    { Id = i, V = i, K = (Kind)(i % 4), NV = (i % 3 == 0) ? (int?)null : i }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CocRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, K INTEGER NOT NULL, NV INTEGER NULL);";
        foreach (var r in Seed)
            cmd.CommandText += $"INSERT INTO CocRow VALUES ({r.Id},{r.V},{(int)r.K},{(r.NV.HasValue ? r.NV.Value.ToString() : "NULL")});";
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

    [Fact] public void Nested_ternary_int() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V < 4 ? 1 : (r.V < 8 ? 2 : 3)));
    [Fact] public void Deep_ternary_string() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V < 3 ? "a" : r.V < 6 ? "b" : r.V < 9 ? "c" : "d"));
    [Fact] public void Enum_dispatch_ternary() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.K == Kind.A ? 10 : r.K == Kind.B ? 20 : r.K == Kind.C ? 30 : 0));
    [Fact] public void Ternary_nullable_branch() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V > 6 ? r.NV : (int?)null));
    [Fact] public void Enum_set_ternary_bool() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (r.K == Kind.A || r.K == Kind.C) ? true : false));
    [Fact] public void Ternary_in_predicate() => Assert_(q => q.Where(r => (r.V < 4 ? 1 : r.V < 8 ? 2 : 3) == 2).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Ternary_in_groupby_key() => Assert_(q => q.GroupBy(r => r.V < 4 ? 0 : r.V < 8 ? 1 : 2).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()));
    [Fact] public void Ternary_arithmetic_branches() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V % 2 == 0 ? r.V * 10 : r.V + 100));
    [Fact] public void Ternary_boolean_value() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V > 5 ? r.K == Kind.A : r.K == Kind.B));
    [Fact] public void Ternary_mixed_null_and_arithmetic() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.NV.HasValue ? (r.NV.Value > 5 ? r.NV.Value * 2 : r.NV.Value) : -1));
    [Fact] public void Coalesce_inside_ternary() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V > 6 ? (r.NV ?? -1) : 0));
}
