using System;
using System.Collections.Generic;
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
/// A client-side sequence tail after a GroupJoin (Take / Skip / Distinct / OrderBy / Reverse) is applied
/// after materialization as a PostMaterializeTransform, because a server LIMIT would truncate a parent's
/// children mid-group. The async group-join materializer never applied that transform (the sync one does),
/// so AsAsyncEnumerable (and ToListAsync on a true-async provider) silently returned the UNTRANSFORMED
/// group-join result — wrong count, wrong order, missing dedup, no exception. Async must match sync.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinAsyncEnumerableTailTransformTests
{
    [Table("GjttParent")]
    public sealed class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("GjttChild")]
    public sealed class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GjttParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE GjttChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);" +
                "INSERT INTO GjttParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol'),(4,'Dave'),(5,'Eve');" +
                "INSERT INTO GjttChild VALUES (1,1),(2,1),(3,2);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static IQueryable<string> Gj(DbContext ctx) =>
        ctx.Query<Parent>()
           .GroupJoin(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, cs) => new { p.Name, Count = cs.Count() })
           .Select(x => x.Name);

    private static async Task<List<string>> AsyncList(IQueryable<string> q)
    {
        var list = new List<string>();
        await foreach (var s in ((INormQueryable<string>)q).AsAsyncEnumerable())
            list.Add(s);
        return list;
    }

    [Fact]
    public async Task GroupJoin_take_tail_matches_sync_over_asyncenumerable()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var sync = Gj(ctx).Take(2).ToList();
        var async = await AsyncList(Gj(ctx).Take(2));

        Assert.Equal(new[] { "Alice", "Bob" }, sync.ToArray());   // oracle
        Assert.Equal(sync, async);
    }

    [Fact]
    public async Task GroupJoin_skip_tail_matches_sync_over_asyncenumerable()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var sync = Gj(ctx).Skip(2).ToList();
        var async = await AsyncList(Gj(ctx).Skip(2));

        Assert.Equal(new[] { "Carol", "Dave", "Eve" }, sync.ToArray());
        Assert.Equal(sync, async);
    }

    [Fact]
    public async Task GroupJoin_orderby_tail_matches_sync_over_asyncenumerable()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var sync = Gj(ctx).OrderByDescending(n => n).ToList();
        var async = await AsyncList(Gj(ctx).OrderByDescending(n => n));

        Assert.Equal(new[] { "Eve", "Dave", "Carol", "Bob", "Alice" }, sync.ToArray());
        Assert.Equal(sync, async);
    }
}
