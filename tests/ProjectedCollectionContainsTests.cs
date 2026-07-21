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
/// Oracle-compared coverage for a collection <c>Contains</c> projected as a BOOLEAN value —
/// <c>Select(r =&gt; ids.Contains(r.Id))</c> / <c>new { IsSelected = ids.Contains(r.Id) }</c>. It works in
/// a predicate (→ IN) but the projection path had no IN emitter and threw
/// <c>no such function: CONTAINS</c>. It now rewrites to an OR-chain of equality comparisons, which the
/// projection visitor already renders as a boolean value with correct null (a null element → IS NULL) and
/// ordinal-string semantics. Each case runs the identical LINQ expression against nORM (SQLite) and
/// LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ProjectedCollectionContainsTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("PccRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int V { get; set; }
        public int? NV { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly Row[] Seed = Enumerable.Range(1, 10).Select(i => new Row
    { Id = i, V = i, NV = (i % 3 == 0) ? (int?)null : i, Name = "n" + i }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PccRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, NV INTEGER NULL, Name TEXT NOT NULL);";
        foreach (var r in Seed)
            cmd.CommandText += $"INSERT INTO PccRow VALUES ({r.Id},{r.V},{(r.NV.HasValue ? r.NV.Value.ToString() : "NULL")},'{r.Name}');";
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

    private static readonly List<int> List = new() { 2, 4, 6, 8 };
    private static readonly List<int> Empty = new();
    private static readonly List<string> StrList = new() { "n2", "n5" };
    private static readonly HashSet<int> Set = new() { 3, 6, 9 };
    private static readonly List<int?> NvWanted = new() { 1, 2, null };

    [Fact] public void Contains_bool_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => List.Contains(r.V)));
    [Fact] public void Negated_contains_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => !List.Contains(r.V)));
    [Fact] public void Empty_collection_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Empty.Contains(r.V)));
    [Fact] public void String_list_contains_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => StrList.Contains(r.Name)));
    [Fact] public void HashSet_contains_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Set.Contains(r.V)));
    [Fact] public void Nullable_list_with_null_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => NvWanted.Contains(r.NV)));
    [Fact] public void Contains_in_conditional() => Assert_(q => q.OrderBy(r => r.Id).Select(r => List.Contains(r.V) ? 1 : 0));
    [Fact] public void Contains_and_comparison() => Assert_(q => q.OrderBy(r => r.Id).Select(r => List.Contains(r.V) && r.V > 3));
    [Fact] public void Static_enumerable_contains() => Assert_(q => q.OrderBy(r => r.Id).Select(r => Enumerable.Contains(List, r.V)));
    [Fact] public void Contains_computed_value() => Assert_(q => q.OrderBy(r => r.Id).Select(r => List.Contains(r.V * 2)));
    [Fact] public void Contains_coalesced_nullable() => Assert_(q => q.OrderBy(r => r.Id).Select(r => List.Contains(r.NV ?? -1)));

    [Fact]
    public void Contains_in_anonymous_projection()
    {
        // Compare (Id, bool) directly — interpolating the bool would trip the separate,
        // documented bool-into-string divergence, not the Contains projection under test.
        var expected = Seed.AsQueryable().OrderBy(r => r.Id).Select(r => new { r.Id, In = List.Contains(r.V) })
            .ToList().Select(x => (x.Id, x.In)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Id, In = List.Contains(r.V) })
            .ToList().Select(x => (x.Id, x.In)).ToList();
        Assert.Equal(expected, actual);
    }
}
