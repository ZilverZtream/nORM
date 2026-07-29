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
/// Swap hunt on projection shapes that use DIFFERENT SELECT emitters than a plain anon/DTO: GroupBy key vs
/// aggregate ordering, Distinct-projection column order, projection interacting with an ORDER BY key
/// expansion, and computed constructor arguments. Every value is distinct so a positional swap is observable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionMaterializationGroupOrderTests
{
    [Table("Pm56Go_Test")]
    public sealed class Ent
    {
        [Key] public int Id { get; set; }
        public int Cat { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56Go_Test (Id INTEGER PRIMARY KEY, Cat INTEGER NOT NULL, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL);" +
                // One group (Cat=7). A=10,B=20,C=30 distinct.
                "INSERT INTO Pm56Go_Test VALUES (1, 7, 10, 20, 30);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    // GroupBy key FIRST, aggregate second.
    [Fact]
    public void GroupBy_key_first_aggregate_second_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().GroupBy(x => x.Cat)
            .Select(g => new { Key = g.Key, Sum = g.Sum(x => x.A) }).First();

        Assert.Equal(7, r.Key);
        Assert.Equal(10, r.Sum);
    }

    // GroupBy aggregate FIRST, key second — key column not in leading position.
    [Fact]
    public void GroupBy_aggregate_first_key_second_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().GroupBy(x => x.Cat)
            .Select(g => new { Sum = g.Sum(x => x.A), Key = g.Key }).First();

        Assert.Equal(10, r.Sum);
        Assert.Equal(7, r.Key);
    }

    // GroupBy with multiple aggregates in a scrambled order plus the key in the middle.
    [Fact]
    public void GroupBy_multi_aggregate_scrambled_with_key_middle_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().GroupBy(x => x.Cat)
            .Select(g => new { SumB = g.Sum(x => x.B), Key = g.Key, SumA = g.Sum(x => x.A), SumC = g.Sum(x => x.C) })
            .First();

        Assert.Equal(20, r.SumB); // sum of B
        Assert.Equal(7, r.Key);
        Assert.Equal(10, r.SumA);
        Assert.Equal(30, r.SumC);
    }

    // Distinct projection with reversed member order.
    [Fact]
    public void Distinct_projection_reversed_members_align()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { Second = x.B, First = x.A }).Distinct().First();

        Assert.Equal(20, r.Second);
        Assert.Equal(10, r.First);
    }

    // OrderBy on a column NOT projected, then reversed-member projection. The ORDER BY key expansion must not
    // shift the projection columns.
    [Fact]
    public void OrderBy_unprojected_column_then_reversed_projection_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().OrderBy(x => x.C).Select(x => new { Second = x.B, First = x.A }).First();

        Assert.Equal(20, r.Second);
        Assert.Equal(10, r.First);
    }

    // Projection THEN OrderBy on a projected member.
    [Fact]
    public void Projection_then_orderby_projected_member_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { Second = x.B, First = x.A }).OrderBy(t => t.First).First();

        Assert.Equal(20, r.Second);
        Assert.Equal(10, r.First);
    }

    // Constructor DTO whose args are COMPUTED expressions in a swapped order.
    public sealed class SumDto
    {
        public SumDto(int sumFirst, int plain) { SumFirst = sumFirst; Plain = plain; }
        public int SumFirst { get; }
        public int Plain { get; }
    }

    [Fact]
    public void Ctor_dto_computed_arg_first_plain_second_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new SumDto(x.A + x.B, x.C)).First();

        Assert.Equal(30, r.SumFirst); // 10 + 20
        Assert.Equal(30, r.Plain);    // x.C (coincidentally 30, but bound distinctly)
    }

    // Constructor DTO: plain column first, computed second — verify the computed lands in the right slot.
    [Fact]
    public void Ctor_dto_plain_first_computed_second_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new SumDto(x.A, x.B + x.C)).First();

        Assert.Equal(10, r.SumFirst); // x.A
        Assert.Equal(50, r.Plain);    // 20 + 30
    }
}
