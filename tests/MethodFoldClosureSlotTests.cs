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
/// The generic method-call constant-fold (Math.Abs(captured), and any static method on a "safe" declaring
/// type) folds the whole call to a fixed parameter but reserved NO compiled-param slot for the closure
/// members it consumed. The parameter-value extractor still collects those members, so the value list grew
/// longer than the compiled-param slot list — and the positional execution binder then shifted a following
/// captured scalar onto the wrong slot, returning the wrong (empty) rows. It must reserve one _unused slot per
/// consumed closure member (which also marks the fold non-cacheable, so a changed capture re-folds).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MethodFoldClosureSlotTests
{
    [Table("MfcItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MfcItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Age INTEGER NOT NULL);" +
                "INSERT INTO MfcItem VALUES (1, 'Ann', 10), (2, 'Bob', 40), (5, 'Eve', 40);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Method_fold_before_captured_scalar_binds_correct_slots()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var negAge = -40;   // Math.Abs(-40) == 40
        var name = "Eve";

        var ids = ctx.Query<Item>()
            .Where(x => x.Age == Math.Abs(negAge) && x.Name == name)
            .ToList().Select(x => x.Id).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { 5 }, ids);
    }

    [Fact]
    public void Method_fold_with_two_captured_args_before_scalar_binds_correct_slots()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var lo = 40; var hi = 30;   // Math.Max(40, 30) == 40
        var name = "Eve";

        var ids = ctx.Query<Item>()
            .Where(x => x.Age == Math.Max(lo, hi) && x.Name == name)
            .ToList().Select(x => x.Id).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { 5 }, ids);
    }

    [Fact]
    public void Method_fold_alone_still_works()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var negAge = -40;
        var ids = ctx.Query<Item>()
            .Where(x => x.Age == Math.Abs(negAge))
            .ToList().Select(x => x.Id).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { 2, 5 }, ids);
    }

    [Fact]
    public void Method_fold_reevaluates_changed_capture_across_calls()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // First call folds Math.Abs(-40)=40; a cache hit must NOT replay 40 for the second capture.
        var neg = -40;
        var first = ctx.Query<Item>().Where(x => x.Age == Math.Abs(neg)).ToList().Count;
        Assert.Equal(2, first);   // Age 40 -> Bob, Eve

        neg = -10;
        var second = ctx.Query<Item>().Where(x => x.Age == Math.Abs(neg)).ToList().Count;
        Assert.Equal(1, second);  // Age 10 -> Ann (would be a stale 2 if the fold were cached)
    }
}
