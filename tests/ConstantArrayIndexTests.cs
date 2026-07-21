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
/// Oracle-compared coverage for a constant array-index expression (<c>arr[i]</c> — a captured array
/// indexed by a constant) used in a query. Such an expression is a <c>BinaryExpression</c> of type
/// ArrayIndex with no SQL operator; it previously reached the operator emitter and threw
/// "Binary operator 'ArrayIndex' has no portable SQL equivalent" in both predicate and projection
/// positions. It is now folded to its element value and bound/inlined as a constant. Covers predicate,
/// projection, ordering-key arithmetic, conditional and anonymous-projection positions across int / string
/// / Guid element types. Each case runs the identical LINQ expression against nORM (SQLite) and
/// LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ConstantArrayIndexTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CaiRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int V { get; set; }
        public string Name { get; set; } = "";
        public Guid Gid { get; set; }
    }

    private static readonly Guid[] Guids = Enumerable.Range(1, 8)
        .Select(i => new Guid($"{i:D8}-0000-0000-0000-000000000000")).ToArray();
    private static readonly int[] Ints = { 20, 40, 60, 80 };
    private static readonly string[] Strs = { "n2", "n4", "n6" };

    private static readonly Row[] Seed = Enumerable.Range(1, 8)
        .Select(i => new Row { Id = i, V = i * 10, Name = "n" + i, Gid = Guids[i - 1] }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CaiRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Name TEXT NOT NULL, Gid TEXT NOT NULL);";
        foreach (var r in Seed) cmd.CommandText += $"INSERT INTO CaiRow VALUES ({r.Id},{r.V},'{r.Name}','{r.Gid}');";
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

    // Predicate (ETSV)
    [Fact] public void Predicate_int_eq_arrayindex() => Assert_(q => q.Where(r => r.V == Ints[1]).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_string_eq_arrayindex() => Assert_(q => q.Where(r => r.Name == Strs[0]).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_arrayindex_arithmetic() => Assert_(q => q.Where(r => r.V == Ints[0] + Ints[1]).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void Predicate_arrayindex_closure_index()
    {
        int idx = 2;
        Assert_(q => q.Where(r => r.V > Ints[idx]).OrderBy(r => r.Id).Select(r => r.Id));
    }

    [Fact] public void Predicate_guid_eq_arrayindex() => Assert_(q => q.Where(r => r.Gid == Guids[2]).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_guid_neq_arrayindex() => Assert_(q => q.Where(r => r.Gid != Guids[2]).OrderBy(r => r.Id).Select(r => r.Id));

    // Projection (SCV)
    [Fact] public void Projection_int_plus_arrayindex() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V + Ints[1]));
    [Fact] public void Projection_string_concat_arrayindex() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name + Strs[0]));
    [Fact] public void Projection_anon_arrayindex() => Assert_(q => q.OrderBy(r => r.Id).Select(r => new { r.Id, C = Ints[2] }).Select(x => $"{x.Id}:{x.C}"));
    [Fact] public void Projection_conditional_arrayindex() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V > Ints[1] ? 1 : 0));

    // Ordering-key arithmetic
    [Fact]
    public void OrderBy_arrayindex_arithmetic()
    {
        int idx = 2;
        Assert_(q => q.OrderBy(r => r.V % Ints[idx]).ThenBy(r => r.Id).Select(r => r.Id));
    }
}
