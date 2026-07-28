using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SQL set operators align columns positionally and take result names from the LEFT arm. When the two arms
/// project a named DTO with member-init bindings in DIFFERENT declaration order, the right arm's columns must
/// be reordered to match the left, or the right arm's rows come back with fields swapped. Verifies the
/// normalization holds across arm shapes, including an operator sitting after the projecting Select.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOpArmMemberOrderTests
{
    [Table("SoamEnt_Test")]
    public class Ent
    {
        [Key] public int Id { get; set; }
        public int X { get; set; }
        public int Y { get; set; }
        public bool Active { get; set; }
    }

    public class Row
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE SoamEnt_Test (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL, Y INTEGER NOT NULL, Active INTEGER NOT NULL);" +
                // X != Y so a positional field-swap is observable.
                "INSERT INTO SoamEnt_Test VALUES (1, 10, 20, 1);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new nORM.Configuration.DbContextOptions()));
    }

    [Fact]
    public void Reversed_binding_order_direct_selects_align_columns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Left binds {A=X, B=Y}; right binds {B=Y, A=X} (reversed). Both logically produce {A=10, B=20},
        // so Union must dedup to a single {A=10, B=20} row — not two rows with the right arm swapped.
        var rows = ctx.Query<Ent>().Select(e => new Row { A = e.X, B = e.Y })
            .Union(ctx.Query<Ent>().Select(e => new Row { B = e.Y, A = e.X }))
            .ToList();

        Assert.Single(rows);
        Assert.Equal(10, rows[0].A);
        Assert.Equal(20, rows[0].B);
    }

    [Fact]
    public void Reversed_binding_order_with_operator_after_right_select_aligns_columns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Right arm has a Distinct() AFTER the projecting Select, so its outermost call is not Select.
        var rows = ctx.Query<Ent>().Select(e => new Row { A = e.X, B = e.Y })
            .Union(ctx.Query<Ent>().Select(e => new Row { B = e.Y, A = e.X }).Distinct())
            .ToList();

        Assert.Single(rows);
        Assert.Equal(10, rows[0].A);
        Assert.Equal(20, rows[0].B);
    }

    [Fact]
    public void Reversed_binding_order_with_where_before_right_select_aligns_columns()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Right arm filters before projecting; Select is still outermost.
        var rows = ctx.Query<Ent>().Select(e => new Row { A = e.X, B = e.Y })
            .Union(ctx.Query<Ent>().Where(e => e.Active).Select(e => new Row { B = e.Y, A = e.X }))
            .ToList();

        Assert.Single(rows);
        Assert.Equal(10, rows[0].A);
        Assert.Equal(20, rows[0].B);
    }
}
