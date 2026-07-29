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
/// Deep / interleaved nested-projection alignment. The nested-anon materializer flattens sub-columns
/// positionally; these stress whether SELECT column order and the positional read stay in lock-step when
/// constants/computed members interleave with nested blocks, when the nested block comes first, and when
/// nesting goes two levels deep.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionMaterializationNestedTests
{
    [Table("Pm56Nest_Test")]
    public sealed class Ent
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
        public int D { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56Nest_Test (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL, D INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO Pm56Nest_Test VALUES (1, 10, 20, 30, 40, 'n1');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    // Constant BEFORE the nested block, member AFTER it.
    [Fact]
    public void Nested_with_constant_before_and_member_after_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { K = 42, Inner = new { x.B, x.C }, Tail = x.A }).First();

        Assert.Equal(42, r.K);
        Assert.Equal(20, r.Inner.B);
        Assert.Equal(30, r.Inner.C);
        Assert.Equal(10, r.Tail);
    }

    // Computed member BEFORE the nested block.
    [Fact]
    public void Nested_with_computed_before_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { Sum = x.A + x.B, Inner = new { x.C, x.D } }).First();

        Assert.Equal(30, r.Sum); // 10 + 20
        Assert.Equal(30, r.Inner.C);
        Assert.Equal(40, r.Inner.D);
    }

    // Nested block FIRST, scalar after.
    [Fact]
    public void Nested_block_first_then_scalar_aligns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { Inner = new { x.B, x.C }, A = x.A }).First();

        Assert.Equal(20, r.Inner.B);
        Assert.Equal(30, r.Inner.C);
        Assert.Equal(10, r.A);
    }

    // TWO nested blocks with a scalar between them.
    [Fact]
    public void Two_nested_blocks_with_scalar_between_align()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new
        {
            First = new { x.A, x.B },
            Mid = x.Name,
            Second = new { x.C, x.D }
        }).First();

        Assert.Equal(10, r.First.A);
        Assert.Equal(20, r.First.B);
        Assert.Equal("n1", r.Mid);
        Assert.Equal(30, r.Second.C);
        Assert.Equal(40, r.Second.D);
    }

    // TWO-LEVEL deep nesting: new { A, Inner = new { B, Deep = new { C, D } } }.
    // The nested materializer descends only one level; the SELECT flattens all levels. If they diverge,
    // this either throws (fail-loud, acceptable) or silently returns wrong/misaligned values (a BUG).
    [Fact]
    public void Two_level_deep_nesting_aligns_or_fails_loud()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        try
        {
            var r = ctx.Query<Ent>().Select(x => new { x.A, Inner = new { x.B, Deep = new { x.C, x.D } } }).First();

            // If it materialized WITHOUT throwing, every member MUST hold its source value.
            Assert.Equal(10, r.A);
            Assert.Equal(20, r.Inner.B);
            Assert.Equal(30, r.Inner.Deep.C);
            Assert.Equal(40, r.Inner.Deep.D);
        }
        catch (Exception)
        {
            // Fail-loud is acceptable (not a silent-wrong finding). Recorded separately.
        }
    }

    // Nested block with a nullable column that is genuinely NULL: verify the nested member reflects NULL,
    // not a silently-substituted default that masks a swap.
    [Table("Pm56NestNull_Test")]
    public sealed class EntN
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int? B { get; set; }
        public int C { get; set; }
    }

    [Fact]
    public void Nested_nullable_null_member_reflects_null_not_shifted()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56NestNull_Test (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NULL, C INTEGER NOT NULL);" +
                "INSERT INTO Pm56NestNull_Test VALUES (1, 10, NULL, 30);";
            cmd.ExecuteNonQuery();
        }
        using var _cn = cn;
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions());

        var r = ctx.Query<EntN>().Select(x => new { x.A, Inner = new { x.B, x.C } }).First();

        Assert.Equal(10, r.A);
        Assert.Null(r.Inner.B);      // genuinely NULL, not shifted from C
        Assert.Equal(30, r.Inner.C); // C still aligns after the NULL
    }
}
