using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// GroupJoin yields ONE result per outer ELEMENT (LINQ-to-Objects contract). The
/// flattened LEFT JOIN stream is segmented by the outer row's PRIMARY KEY, not the
/// join key: segmenting by key fused distinct outers that share a key value into a
/// single result — silently dropping rows and multiplying their children. Ordering
/// carries a PK tiebreak inside each key tie so every outer's block is contiguous
/// (skipped when the key IS the PK — SQL Server rejects duplicate ORDER BY columns).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinOuterRowSegmentationTests
{
    [Table("GjSeg_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("GjSeg_Badge")]
    private class Badge
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GjSeg_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE GjSeg_Badge (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                INSERT INTO GjSeg_Emp VALUES (1, 'ann'), (2, 'bob'), (3, 'ann');
                INSERT INTO GjSeg_Badge VALUES (1, 'ann'), (2, 'ann'), (3, 'cid');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Emp>().HasKey(e => e.Id); mb.Entity<Badge>().HasKey(b => b.Id); }
        });
    }

    [Fact]
    public void Duplicate_outer_keys_yield_one_result_per_outer_row()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .GroupJoin(ctx.Query<Badge>(), e => e.Name, b => b.Label, (e, bs) => new { e.Id, Count = bs.Count() })
            .ToList().OrderBy(r => r.Id).ToList();
        // LINQ-to-Objects: 3 outer elements → 3 results; both 'ann' rows own BOTH
        // matching badges independently, and 'bob' owns none.
        Assert.Equal(3, rows.Count);
        Assert.Equal(2, rows.Single(r => r.Id == 1).Count);
        Assert.Equal(0, rows.Single(r => r.Id == 2).Count);
        Assert.Equal(2, rows.Single(r => r.Id == 3).Count);
    }

    [Fact]
    public void Key_that_is_the_primary_key_does_not_duplicate_order_by()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .GroupJoin(ctx.Query<Badge>(), e => e.Id, b => b.Id, (e, bs) => new { e.Name, Count = bs.Count() })
            .ToList();
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.Equal(1, r.Count));
    }
}
