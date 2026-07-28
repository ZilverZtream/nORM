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
/// A nested set-op chain `a.SETOP(b).SETOP(c)` must align every arm's member-init projection by MEMBER, not
/// position. Arm-order normalization compared the right arm against the left, but in a chain the outer node's
/// left arm is itself a Union/Concat/etc — not a member-init Select — so normalization bailed and the third
/// arm (c), built in a different binding order, was never reordered. SQL set ops match columns positionally,
/// so c's values landed under the wrong members (column swap / wrong dedup / dropped rows).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NestedSetOpArmAlignmentTests
{
    [Table("NsoaEnt")]
    public class Ent { [Key] public int Id { get; set; } public int X { get; set; } public int Y { get; set; } }

    public sealed class Dto : IEquatable<Dto>
    {
        public int A { get; set; }
        public int B { get; set; }
        public bool Equals(Dto? o) => o != null && A == o.A && B == o.B;
        public override bool Equals(object? o) => Equals(o as Dto);
        public override int GetHashCode() => (A, B).GetHashCode();
    }

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NsoaEnt (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL, Y INTEGER NOT NULL);" +
                              "INSERT INTO NsoaEnt VALUES (1,10,20),(2,11,21),(3,12,22);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Nested_union_reversed_third_arm_aligns_by_member()
    {
        using var ctx = Ctx();
        var a = ctx.Query<Ent>().Where(e => e.Id == 1).Select(e => new Dto { A = e.X, B = e.Y });
        var b = ctx.Query<Ent>().Where(e => e.Id == 2).Select(e => new Dto { A = e.X, B = e.Y });
        var c = ctx.Query<Ent>().Where(e => e.Id == 3).Select(e => new Dto { B = e.Y, A = e.X }); // reversed
        var got = a.Union(b).Union(c).ToList().OrderBy(d => d.A).Select(d => $"{d.A},{d.B}").ToList();
        Assert.Equal(new[] { "10,20", "11,21", "12,22" }, got);
    }

    [Fact]
    public void Nested_concat_reversed_third_arm_aligns_by_member()
    {
        using var ctx = Ctx();
        var a = ctx.Query<Ent>().Where(e => e.Id == 1).Select(e => new Dto { A = e.X, B = e.Y });
        var b = ctx.Query<Ent>().Where(e => e.Id == 2).Select(e => new Dto { A = e.X, B = e.Y });
        var c = ctx.Query<Ent>().Where(e => e.Id == 3).Select(e => new Dto { B = e.Y, A = e.X }); // reversed
        var got = a.Concat(b).Concat(c).ToList().OrderBy(d => d.A).Select(d => $"{d.A},{d.B}").ToList();
        Assert.Equal(new[] { "10,20", "11,21", "12,22" }, got);
    }

    [Fact]
    public void Nested_except_reversed_third_arm_removes_correct_row()
    {
        using var ctx = Ctx();
        // (a ∪ b) except c, where c reversed logically equals {A=12,B=22} (Id=3). Only Id=3's projection is in
        // both, so it must be removed -> {10,20},{11,21}.
        var a = ctx.Query<Ent>().Where(e => e.Id == 1 || e.Id == 3).Select(e => new Dto { A = e.X, B = e.Y });
        var b = ctx.Query<Ent>().Where(e => e.Id == 2).Select(e => new Dto { A = e.X, B = e.Y });
        var c = ctx.Query<Ent>().Where(e => e.Id == 3).Select(e => new Dto { B = e.Y, A = e.X }); // reversed
        var got = a.Union(b).Except(c).ToList().OrderBy(d => d.A).Select(d => $"{d.A},{d.B}").ToList();
        Assert.Equal(new[] { "10,20", "11,21" }, got);
    }
}
