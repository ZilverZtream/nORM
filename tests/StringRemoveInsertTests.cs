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
/// Oracle-compared coverage for <c>string.Remove</c> and <c>string.Insert</c>, previously untranslatable
/// (they fell to the client-evaluation policy throw). Both compose from Substring + null-safe
/// concatenation: <c>Remove(i)</c> = s[..i]; <c>Remove(i,c)</c> = s[..i] + s[i+c..];
/// <c>Insert(i, v)</c> = s[..i] + v + s[i..]. Covers projection and predicate positions, constant /
/// computed / closure indices, chaining, length-after, and an anonymous projection. Each case runs the
/// identical LINQ expression against nORM (SQLite) and LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StringRemoveInsertTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SriRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly string[] Names = { "Alpha", "beta", "Gamma", "delta", "Echo" };
    private static readonly Row[] Rows = Names.Select((n, i) => new Row { Id = i + 1, Name = n }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SriRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        foreach (var r in Rows) cmd.CommandText += $"INSERT INTO SriRow VALUES ({r.Id},'{r.Name}');";
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

    [Fact] public void Remove_from_index() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(3)));
    [Fact] public void Remove_range() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(1, 2)));
    [Fact] public void Insert_at_start() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Insert(0, "X")));
    [Fact] public void Insert_in_middle() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Insert(2, "--")));

    [Fact] public void Remove_predicate() => Assert_(q => q.Where(r => r.Name.Remove(2) == "Al").OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Insert_predicate() => Assert_(q => q.Where(r => r.Name.Insert(0, "Z") == "Zbeta").OrderBy(r => r.Id).Select(r => r.Id));

    [Fact] public void Remove_computed_index() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(r.Name.Length - 1)));
    [Fact] public void Insert_at_length() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Insert(r.Name.Length, "!")));

    [Fact]
    public void Remove_closure_index()
    {
        int k = 2;
        Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(k)));
    }

    [Fact]
    public void Remove_range_closure_count()
    {
        int k = 2;
        Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(1, k)));
    }

    [Fact] public void Remove_range_length() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(1, 2).Length));
    [Fact] public void Insert_then_length() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Insert(1, "XY").Length));
    [Fact] public void Remove_then_insert_chained() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Remove(3).Insert(0, ">")));

    [Fact]
    public void Remove_in_anonymous_projection() => Assert_(q => q.OrderBy(r => r.Id)
        .Select(r => new { r.Id, S = r.Name.Remove(2) }).Select(x => $"{x.Id}:{x.S}"));
}
