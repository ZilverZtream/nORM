using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A projection that uses a captured NESTED closure member (a ValueTuple field / an object property on a
/// local) inside a computed operand — Select(x => new { P = x.A + local.Field }) — must fold that member to
/// its captured value, exactly as the predicate path already does. The projection path left it unresolved and
/// emitted NULL, so the computed column materialized as NULL and threw "data is NULL at ordinal N".
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CapturedTupleMemberProjectionTests
{
    [Table("CtpRec")]
    public class Rec { [Key] public int Id { get; set; } public int A { get; set; } public int B { get; set; } }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CtpRec (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
                              "INSERT INTO CtpRec VALUES (1, 5, 7), (2, 10, 20);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Projection_arithmetic_with_captured_tuple_member_folds_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn);
        var lp = (minA: 3, projAdd: 1000);
        var rows = ctx.Query<Rec>()
            .OrderBy(x => x.Id)
            .Select(x => new { x.Id, P = x.A + lp.projAdd, Q = x.B })
            .ToList();
        Assert.Equal(new[] { 1005, 1010 }, rows.Select(r => r.P).ToArray());   // A + 1000
        Assert.Equal(new[] { 7, 20 }, rows.Select(r => r.Q).ToArray());
    }

    [Fact]
    public void Predicate_with_captured_tuple_member_still_works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn);
        var lp = (minA: 6, projAdd: 1000);
        var ids = ctx.Query<Rec>().Where(x => x.A >= lp.minA).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 2 }, ids.ToArray());   // control: predicate path already folds the member
    }
}
