using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// TranslateInSubContext must fully save/restore the client-tail state
/// (_postMaterializeTransform, _clientProjection, reshape/streaming flags). If it
/// doesn't, a reshape or client projection on the OUTER query leaks into the
/// sub-context translation of a correlated subquery or set-op arm — corrupting
/// results. These pins exercise those shapes.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SubContextTailStateLeakTests
{
    [Table("SctItem")]
    private class SctItem
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }

    [Table("SctChild")]
    private class SctChild
    {
        [Key] public int Id { get; set; }
        public int ItemId { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SctItem (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);
                CREATE TABLE SctChild (Id INTEGER PRIMARY KEY, ItemId INTEGER NOT NULL);
                INSERT INTO SctItem VALUES (1,10),(2,20),(3,30);
                INSERT INTO SctChild VALUES (100,1),(101,3);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<SctItem>(); mb.Entity<SctChild>(); }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Reshape_on_outer_does_not_corrupt_a_correlated_subquery()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Outer query carries a client-tail reshape (Append); the Where contains a
        // correlated EXISTS subquery translated in a sub-context. The reshape state
        // must not leak into that sub-context.
        var sentinel = new SctItem { Id = 999, Value = 999 };
        var rows = await ctx.Query<SctItem>()
            .Where(i => ctx.Query<SctChild>().Any(c => c.ItemId == i.Id))
            .OrderBy(i => i.Id)
            .Append(sentinel)
            .ToListAsync();

        // Items 1 and 3 have children; plus the appended sentinel = 3 rows.
        Assert.Equal(new[] { 1, 3, 999 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Subquery_after_reshape_count_is_correct()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var sentinel = new SctItem { Id = 999, Value = 999 };
        var count = await ctx.Query<SctItem>()
            .Where(i => ctx.Query<SctChild>().Any(c => c.ItemId == i.Id))
            .Append(sentinel)
            .CountAsync();

        Assert.Equal(3, count); // 2 matching items + appended sentinel
    }

    [Fact]
    public async Task Concat_arms_each_translate_independently()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Two set-op arms, each with its own filter; the sub-context translation of
        // one arm must not leak state into the other.
        var combined = await ctx.Query<SctItem>().Where(i => i.Value < 15)
            .Concat(ctx.Query<SctItem>().Where(i => i.Value > 25))
            .ToListAsync();

        Assert.Equal(new[] { 10, 30 }, combined.Select(r => r.Value).OrderBy(v => v).ToArray());
    }
}
