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
/// Coverage for set operations (Union / Concat / Intersect / Except) where one arm is a LOCAL in-memory
/// sequence — e.g. <c>db.Select(x =&gt; x.Col).Union(new[] { "All", "None" })</c>, the sentinel-value
/// pattern. Value-equatable element types (scalars, nullables, anonymous types) evaluate the set
/// operation client-side over the DB arm's materialized rows with LINQ-to-Objects, which is exactly C#
/// set semantics; a trailing OrderBy composes and sorts the combined result. Two DB arms still translate
/// server-side. Each case runs the identical LINQ expression against nORM (SQLite) and LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SetOperationWithLocalSequenceTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SolsRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int? K { get; set; }
        public string S { get; set; } = "";
        public decimal M { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 12).Select(i => new Row
    {
        Id = i,
        K = (i % 5 == 0) ? (int?)null : (i % 4),
        S = (i % 3 == 0) ? "a" : (i % 2 == 0 ? "b" : "c"),
        M = (i % 4) * 1.5m,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SolsRow (Id INTEGER PRIMARY KEY, K INTEGER NULL, S TEXT NOT NULL, M TEXT NOT NULL);";
        foreach (var r in Rows)
            cmd.CommandText += $"INSERT INTO SolsRow VALUES ({r.Id},{(r.K.HasValue ? r.K.Value.ToString() : "NULL")},'{r.S}','{r.M.ToString(System.Globalization.CultureInfo.InvariantCulture)}');";
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
    public void Int_union_local_ordered() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Union(new[] { 100, 200 }).OrderBy(v => v));

    [Fact]
    public void Int_concat_local_ordered() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Concat(new[] { 100, 1 }).OrderBy(v => v));

    [Fact]
    public void Int_intersect_local() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Intersect(new[] { 1, 2, 99 }).OrderBy(v => v));

    [Fact]
    public void Int_except_local() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Except(new[] { 1, 99 }).OrderBy(v => v));

    [Fact]
    public void NullableInt_union_with_null_element() => Assert_(q => q.Select(r => r.K)
        .Union(new int?[] { null, 99 }).OrderBy(v => v));

    [Fact]
    public void NullableInt_except_null_element() => Assert_(q => q.Select(r => r.K)
        .Except(new int?[] { null }).OrderBy(v => v));

    [Fact]
    public void Decimal_union_local_ordered() => Assert_(q => q.Select(r => r.M)
        .Union(new[] { 9.5m, 0m }).OrderBy(m => m));

    [Fact]
    public void Left_arm_local_union_db() => Assert_(q => new[] { 100, 0 }.AsQueryable()
        .Union(q.Where(r => r.K.HasValue).Select(r => r.K!.Value)).OrderBy(v => v));

    [Fact]
    public void AsQueryable_explicit_local_arm() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Union(new[] { 100, 200 }.AsQueryable()).OrderBy(v => v));

    [Fact]
    public void Union_without_trailing_orderby() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Union(new[] { 100, 200 }));

    [Fact]
    public void Two_db_arms_still_translate_server_side() => Assert_(q => q.Where(r => r.K.HasValue).Select(r => r.K!.Value)
        .Union(q.Where(r => r.K.HasValue).Select(r => r.K!.Value + 50)).OrderBy(v => v));

    [Fact]
    public void Anonymous_element_union_local() => Assert_(q => q.Select(r => new { r.Id, r.S })
        .Where(x => x.Id <= 3)
        .Union(new[] { new { Id = 99, S = "z" } })
        .OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.S}"));

    [Fact]
    public void String_union_local_values_match()
    {
        // String ORDER BY culture-vs-collation is a separate documented divergence; validate the
        // Union VALUES by sorting both sides ordinally after materialization.
        var expected = Rows.AsQueryable().Select(r => r.S).Union(new[] { "All", "None" })
            .OrderBy(s => s, StringComparer.Ordinal).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => r.S).Union(new[] { "All", "None" })
            .ToList().OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(expected, actual);
    }
}
