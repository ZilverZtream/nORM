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
/// Arbitrarily-deep nested anonymous projections. The SELECT-clause visitor flattens every nesting level into
/// sequential prefixed columns; the materializer recurses in lock-step, reading each leaf column in SELECT
/// order. Every column is seeded with a distinct value so any misalignment (swap/drop/shift) is observable.
/// (A 2+-level projection previously threw InvalidCastException because the materializer descended only one
/// level and cast a nested block to a scalar.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NestedAnonymousProjectionTests
{
    [Table("Pm56Probe_Test")]
    public sealed class Ent
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
        public int D { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56Probe_Test (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL, D INTEGER NOT NULL);" +
                "INSERT INTO Pm56Probe_Test VALUES (1, 10, 20, 30, 40);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Two_level_deep_nesting_materializes_each_leaf_in_order()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { x.A, Inner = new { x.B, Deep = new { x.C, x.D } } }).First();

        Assert.Equal(10, r.A);
        Assert.Equal(20, r.Inner.B);
        Assert.Equal(30, r.Inner.Deep.C);
        Assert.Equal(40, r.Inner.Deep.D);
    }

    [Fact]
    public void Three_level_deep_nesting_materializes_each_leaf_in_order()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>()
            .Select(x => new { x.A, L1 = new { x.B, L2 = new { x.C, L3 = new { x.D, x.Id } } } })
            .First();

        Assert.Equal(10, r.A);
        Assert.Equal(20, r.L1.B);
        Assert.Equal(30, r.L1.L2.C);
        Assert.Equal(40, r.L1.L2.L3.D);
        Assert.Equal(1, r.L1.L2.L3.Id);
    }

    // A leaf that FOLLOWS a nested block must read the column after the block's flattened columns — verifies
    // the cursor advances past the whole nested block, not by a single column.
    [Fact]
    public void Leaf_after_nested_block_reads_the_correct_column()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Ent>().Select(x => new { x.A, Inner = new { x.B, x.C }, D = x.D }).First();

        Assert.Equal(10, r.A);
        Assert.Equal(20, r.Inner.B);
        Assert.Equal(30, r.Inner.C);
        Assert.Equal(40, r.D);
    }
}
