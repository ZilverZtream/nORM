using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A Join projecting whole entities (`(l,r) => new { l, r }`) or a chained join executes client-side via a
/// nested-loop join. It used object.Equals for key matching, and object.Equals(null, null) is TRUE, so two
/// rows whose nullable join key is NULL on both sides spuriously matched — a row both SQL and LINQ-to-Objects
/// Join exclude. The scalar-member projection path (SQL join, `T0.K = T1.K`) correctly excludes nulls, so the
/// bug hid behind projection shape.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class WholeEntityJoinNullKeyTests
{
    [Table("WejL")] public class L { [Key] public int Id { get; set; } public int? K { get; set; } }
    [Table("WejR")] public class R { [Key] public int Id { get; set; } public int? K { get; set; } }

    private static readonly (int id, int? k)[] LRows = { (1, 5), (2, null) };
    private static readonly (int id, int? k)[] RRows = { (10, 5), (20, null) };

    private static DbContext Ctx(out List<L> ls, out List<R> rs)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE WejL (Id INTEGER PRIMARY KEY, K INTEGER NULL);" +
                              "CREATE TABLE WejR (Id INTEGER PRIMARY KEY, K INTEGER NULL);" +
                              "INSERT INTO WejL VALUES (1,5),(2,NULL);" +
                              "INSERT INTO WejR VALUES (10,5),(20,NULL);";
            cmd.ExecuteNonQuery();
        }
        ls = LRows.Select(x => new L { Id = x.id, K = x.k }).ToList();
        rs = RRows.Select(x => new R { Id = x.id, K = x.k }).ToList();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void WholeEntity_join_nullable_key_never_matches_null_to_null()
    {
        using var ctx = Ctx(out var ls, out var rs);
        var norm = ctx.Query<L>().Join(ctx.Query<R>(), l => l.K, r => r.K, (l, r) => new { l, r })
            .ToList().Select(x => (x.l.Id, x.r.Id)).OrderBy(x => x.Item1).ThenBy(x => x.Item2).ToList();
        var oracle = ls.Join(rs, l => l.K, r => r.K, (l, r) => (l.Id, r.Id))
            .OrderBy(x => x.Item1).ThenBy(x => x.Item2).ToList();
        Assert.Equal(oracle, norm);   // [(1,10)] — the (2,20) null==null match must NOT appear
    }
}
