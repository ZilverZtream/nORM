using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Bulk operations mixed with tracked SaveChanges inside ONE caller-owned transaction. Bulk bypasses the
/// change tracker while the tracked writes carry the deferred-accept flag/baseline snapshots, so both must
/// land atomically (all on commit, none on rollback) and neither must disturb the other — in particular a
/// tracked entity inserted alongside a bulk op must not be re-inserted by a later save after the commit.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkMixedWithTrackedUnderTransactionTests
{
    [Table("BmtItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection, DbContext) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BmtItem (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static int[] Ids(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM BmtItem ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var v = new System.Collections.Generic.List<int>(); while (r.Read()) v.Add(r.GetInt32(0));
        return v.ToArray();
    }

    [Fact]
    public async Task Tracked_insert_then_bulk_insert_commit_persists_all()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new Item { Id = 1, Value = 10 }); await ctx.SaveChangesAsync();
        await ctx.BulkInsertAsync(new[] { new Item { Id = 2, Value = 20 }, new Item { Id = 3, Value = 30 } });
        await tx.CommitAsync();

        Assert.Equal(new[] { 1, 2, 3 }, Ids(cn));
    }

    [Fact]
    public async Task Tracked_insert_then_bulk_insert_rollback_persists_nothing()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new Item { Id = 1, Value = 10 }); await ctx.SaveChangesAsync();
            await ctx.BulkInsertAsync(new[] { new Item { Id = 2, Value = 20 } });
            await tx.RollbackAsync();
        }

        Assert.Empty(Ids(cn));
    }

    [Fact]
    public async Task Bulk_insert_then_tracked_insert_commit_persists_all()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.BulkInsertAsync(new[] { new Item { Id = 2, Value = 20 }, new Item { Id = 3, Value = 30 } });
        ctx.Add(new Item { Id = 1, Value = 10 }); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(new[] { 1, 2, 3 }, Ids(cn));
    }

    [Fact]
    public async Task Tracked_and_bulk_committed_then_saving_outside_does_not_reinsert_the_tracked_row()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new Item { Id = 1, Value = 10 }); await ctx.SaveChangesAsync();
            await ctx.BulkInsertAsync(new[] { new Item { Id = 2, Value = 20 } });
            await tx.CommitAsync();
        }

        ctx.Add(new Item { Id = 4, Value = 40 }); await ctx.SaveChangesAsync();

        Assert.Equal(new[] { 1, 2, 4 }, Ids(cn));
    }
}
