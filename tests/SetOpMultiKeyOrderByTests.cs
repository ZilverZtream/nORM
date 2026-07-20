using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A multi-key OrderBy (OrderBy(...).ThenBy(...)) after a set operation must order by unqualified result
/// columns, as a compound SELECT requires. The set-op ordering path applied that only to the FIRST key; a
/// trailing ThenBy fell through to the normal path and emitted a table-qualified term, which SQLite rejects
/// with "Nth ORDER BY term does not match any column in the result set" - but only when both arms are
/// unfiltered (a bare entity query), which was the shape the coverage-guided fuzzer hit.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOpMultiKeyOrderByTests
{
    [Table("SmRow")]
    public class Row { [Key] public int Id { get; set; } public int A { get; set; } public int B { get; set; } }

    private static DbContext Ctx(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SmRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
            "INSERT INTO SmRow VALUES (1,4,1),(2,2,3),(3,5,2);";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
    }

    [Fact]
    public void Union_unfiltered_multikey_orderby_orders_by_both_keys()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var ids = ctx.Query<Row>().Union(ctx.Query<Row>())
            .OrderBy(r => r.B).ThenBy(r => r.Id).ToList().Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3, 2 }, ids);   // by B: 1(Id1), 2(Id3), 3(Id2)
    }

    [Fact]
    public void Concat_unfiltered_multikey_orderby_keeps_duplicates_ordered()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var ids = ctx.Query<Row>().Concat(ctx.Query<Row>())
            .OrderBy(r => r.B).ThenBy(r => r.Id).ToList().Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 1, 3, 3, 2, 2 }, ids);
    }

    [Fact]
    public void Except_unfiltered_multikey_orderby_is_empty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        // A EXCEPT A = empty; ordering must still translate.
        var ids = ctx.Query<Row>().Except(ctx.Query<Row>())
            .OrderBy(r => r.B).ThenBy(r => r.Id).ToList();
        Assert.Empty(ids);
    }

    [Fact]
    public void Union_unfiltered_multikey_orderby_with_paging()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var ids = ctx.Query<Row>().Union(ctx.Query<Row>())
            .OrderBy(r => r.B).ThenBy(r => r.Id).Skip(1).Take(1).ToList().Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3 }, ids);   // ordered [1,3,2], skip 1 -> [3,2], take 1 -> [3]
    }
}
