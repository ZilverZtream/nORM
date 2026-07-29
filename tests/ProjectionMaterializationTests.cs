using System;
using System.Collections.Generic;
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
/// Guards projection materialization against silently binding columns to the WRONG members: a Select into an
/// anonymous type, DTO, record, tuple, or constructor with permuted / repeated / mixed members must land each
/// value in its intended member. Every row is seeded with DISTINCT column values so any positional
/// misalignment is observable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionMaterializationTests
{
    [Table("Pm56Ent_Test")]
    public sealed class Ent
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
        public int D { get; set; }
        public int E { get; set; }
        public int F { get; set; }
        public int G { get; set; }
        public string Name { get; set; } = "";
    }

    // Distinct so any swap is obvious: Id=1, A=10, B=20, C=30, D=40, E=50, F=60, G=70, Name="n1"
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56Ent_Test (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, " +
                "C INTEGER NOT NULL, D INTEGER NOT NULL, E INTEGER NOT NULL, F INTEGER NOT NULL, G INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO Pm56Ent_Test VALUES (1, 10, 20, 30, 40, 50, 60, 70, 'n1');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    // ---------- Surface 1: anonymous type, same-typed members in reversed order ----------
    [Fact]
    public void Anon_same_typed_members_reversed_order_not_swapped()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { Second = x.B, First = x.A }).First();

        Assert.Equal(20, r.Second); // x.B
        Assert.Equal(10, r.First);  // x.A
    }

    // ---------- Surface 2: constructor DTO + positional record, param order != property order ----------
    public sealed class Dto2
    {
        public Dto2(int second, int first) { Second = second; First = first; }
        public int Second { get; }
        public int First { get; }
    }

    public sealed record Rec2(int Second, int First);

    [Fact]
    public void Ctor_dto_param_order_reversed_not_swapped()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new Dto2(x.B, x.A)).First();

        Assert.Equal(20, r.Second); // ctor arg 0 = x.B
        Assert.Equal(10, r.First);  // ctor arg 1 = x.A
    }

    [Fact]
    public void Positional_record_param_order_reversed_not_swapped()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new Rec2(x.B, x.A)).First();

        Assert.Equal(20, r.Second); // x.B
        Assert.Equal(10, r.First);  // x.A
    }

    // ---------- Surface 3: DTO object-initializer, members assigned out of column order ----------
    public sealed class Dto3
    {
        public int X { get; set; }
        public int Y { get; set; }
    }

    [Fact]
    public void MemberInit_out_of_order_not_swapped()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new Dto3 { Y = x.B, X = x.A }).First();

        Assert.Equal(10, r.X); // x.A
        Assert.Equal(20, r.Y); // x.B
    }

    // ---------- Surface 4: mixed columns + constants + computed + method calls ----------
    [Fact]
    public void Mixed_columns_constants_computed_methods_each_land_in_right_member()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { x.A, K = 42, Sum = x.A + x.B, Up = x.Name.ToUpper() }).First();

        Assert.Equal(10, r.A);      // column
        Assert.Equal(42, r.K);      // constant
        Assert.Equal(30, r.Sum);    // computed 10 + 20
        Assert.Equal("N1", r.Up);   // method
    }

    // Same but with the column LAST so a permutation would surface.
    [Fact]
    public void Mixed_column_last_each_land_in_right_member()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { K = 42, Sum = x.A + x.B, Up = x.Name.ToUpper(), Last = x.C }).First();

        Assert.Equal(42, r.K);
        Assert.Equal(30, r.Sum);
        Assert.Equal("N1", r.Up);
        Assert.Equal(30, r.Last); // x.C
    }

    // ---------- Surface 5: nested anonymous projection ----------
    [Fact]
    public void Nested_anon_projection_members_align()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { x.A, Inner = new { x.B, x.C } }).First();

        Assert.Equal(10, r.A);
        Assert.Equal(20, r.Inner.B);
        Assert.Equal(30, r.Inner.C);
    }

    // Nested with reversed inner members to stress inner alignment.
    [Fact]
    public void Nested_anon_reversed_inner_members_align()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { x.A, Inner = new { Q = x.C, P = x.B } }).First();

        Assert.Equal(10, r.A);
        Assert.Equal(30, r.Inner.Q); // x.C
        Assert.Equal(20, r.Inner.P); // x.B
    }

    // ---------- Surface 6: projection repeating a column ----------
    [Fact]
    public void Repeated_column_both_members_get_value()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { First = x.A, Again = x.A }).First();

        Assert.Equal(10, r.First);
        Assert.Equal(10, r.Again);
    }

    // Repeat one column between two DISTINCT columns to catch a drop/dup shifting alignment.
    [Fact]
    public void Repeated_column_between_distinct_columns_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { P = x.B, Dup = x.A, Q = x.C, Dup2 = x.A }).First();

        Assert.Equal(20, r.P);    // x.B
        Assert.Equal(10, r.Dup);  // x.A
        Assert.Equal(30, r.Q);    // x.C
        Assert.Equal(10, r.Dup2); // x.A
    }

    // ---------- Surface 9: ValueTuple projections ----------
    [Fact]
    public void ValueTuple_positional_reversed_not_swapped()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new ValueTuple<int, int>(x.B, x.A)).First();

        Assert.Equal(20, r.Item1); // x.B
        Assert.Equal(10, r.Item2); // x.A
    }

    // ValueTuple.Create(...) is a factory METHOD call in the projection — nORM cannot translate it to SQL and
    // fails loud (NormUnsupportedFeatureException). This is FAIL-LOUD, not silent-wrong. The explicit ctor form
    // `new ValueTuple<int,int>(x.B, x.A)` (above) IS supported and aligns correctly.
    [Fact]
    public void ValueTuple_create_factory_fails_loud_not_silent_wrong()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<Ent>().Select(x => ValueTuple.Create(x.B, x.A)).First());
    }

    // ---------- Surface 10: wide projection (8 members) mixing types, permuted ----------
    [Fact]
    public void Wide_projection_permuted_members_all_align()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new
        {
            M1 = x.G,      // 70
            M2 = x.Name,   // "n1"
            M3 = x.A,      // 10
            M4 = x.F,      // 60
            M5 = x.B,      // 20
            M6 = x.E,      // 50
            M7 = x.C,      // 30
            M8 = x.D       // 40
        }).First();

        Assert.Equal(70, r.M1);
        Assert.Equal("n1", r.M2);
        Assert.Equal(10, r.M3);
        Assert.Equal(60, r.M4);
        Assert.Equal(20, r.M5);
        Assert.Equal(50, r.M6);
        Assert.Equal(30, r.M7);
        Assert.Equal(40, r.M8);
    }

    // ---------- Surface 11: different-typed members with SQL order permuted ----------
    // int members interleaved with a string; a wrong permutation would try to read a string as int (throw)
    // or assign an int where a string is expected. Verify each member holds its source value.
    [Fact]
    public void Different_typed_members_interleaved_align_or_fail_loud()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { S = x.Name, I1 = x.B, I2 = x.A }).First();

        Assert.Equal("n1", r.S);
        Assert.Equal(20, r.I1); // x.B
        Assert.Equal(10, r.I2); // x.A
    }

    // ---------- Surface 2b: record with MORE members, param order scrambled ----------
    public sealed record Rec4(int C, int A, int D, int B);

    [Fact]
    public void Record_four_scrambled_params_align()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new Rec4(x.C, x.A, x.D, x.B)).First();

        Assert.Equal(30, r.C); // x.C
        Assert.Equal(10, r.A); // x.A
        Assert.Equal(40, r.D); // x.D
        Assert.Equal(20, r.B); // x.B
    }

    // ---------- Surface 3b: DTO object-init with a member NAME that shadows a different column ----------
    // The DTO property names deliberately collide with entity column names but are bound to DIFFERENT columns.
    // If the materializer resolves columns by NAME instead of by the bound expression, it swaps.
    public sealed class DtoNameCollide
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    [Fact]
    public void MemberInit_property_names_collide_with_columns_but_bound_crossed()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // DTO.A is bound to entity.B; DTO.B is bound to entity.A. Names match columns but crossed.
        var r = ctx.Query<Ent>().Select(x => new DtoNameCollide { A = x.B, B = x.A }).First();

        Assert.Equal(20, r.A); // bound to x.B
        Assert.Equal(10, r.B); // bound to x.A
    }

    // Anonymous type whose member names collide with columns but bound crossed.
    [Fact]
    public void Anon_member_names_collide_with_columns_but_bound_crossed()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { A = x.B, B = x.A }).First();

        Assert.Equal(20, r.A); // bound to x.B
        Assert.Equal(10, r.B); // bound to x.A
    }
}
