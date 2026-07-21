using System;
using System.Linq;
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
/// Delete / re-insert of a client-keyed entity across multiple SaveChanges calls inside one caller-owned
/// transaction. Under a caller-owned transaction a Deleted entity stays tracked (AcceptChanges is deferred),
/// so a same-key re-add, a redundant second save, or a rollback must each behave exactly: the row ends in the
/// state the sequence of edits describes, once, with no double-delete, dropped re-insert, or resurrection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeleteReinsertUnderTransactionTests
{
    [Table("DrtItem")]
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
            cmd.CommandText = "CREATE TABLE DrtItem (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL); INSERT INTO DrtItem VALUES (71, 10);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static int? Value71(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Value FROM DrtItem WHERE Id = 71";
        var o = cmd.ExecuteScalar();
        return o == null ? (int?)null : Convert.ToInt32(o);
    }

    private static long Count(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM DrtItem WHERE Id = {id}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Delete_then_reinsert_same_key_across_two_saves_lands_the_new_row()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var item = await ctx.Query<Item>().FirstAsync(i => i.Id == 71);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Remove(item); await ctx.SaveChangesAsync();
        ctx.Add(new Item { Id = 71, Value = 99 }); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(99, Value71(cn));
    }

    [Fact]
    public async Task Delete_saved_in_tx_then_rolled_back_restores_the_row()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var item = await ctx.Query<Item>().FirstAsync(i => i.Id == 71);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Remove(item); await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        Assert.Equal(10, Value71(cn));
    }

    [Fact]
    public async Task Delete_then_redundant_second_save_keeps_the_row_deleted()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var item = await ctx.Query<Item>().FirstAsync(i => i.Id == 71);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Remove(item); await ctx.SaveChangesAsync();
        await ctx.SaveChangesAsync();   // must not resurrect or throw
        await tx.CommitAsync();

        Assert.Null(Value71(cn));
    }

    [Fact]
    public async Task Insert_then_delete_same_key_across_two_saves_leaves_no_row()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new Item { Id = 200, Value = 1 }); await ctx.SaveChangesAsync();
        var e = await ctx.Query<Item>().FirstAsync(i => i.Id == 200);
        ctx.Remove(e); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(0, Count(cn, 200));
    }
}
