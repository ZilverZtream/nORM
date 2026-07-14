using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Direct InsertAsync/UpdateAsync/DeleteAsync must keep the change tracker
/// coherent when the entity they touch is tracked by the same context:
/// a successful direct DELETE means the row is gone (the tracked entry and
/// navigation references must not survive to resurrect it or false-conflict a
/// later save), and a successful direct INSERT/UPDATE means the entity's
/// current values ARE the database state (a later SaveChanges must not
/// re-write or double-insert them).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DirectOperationTrackerCoherenceTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DirOp_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("DirOpTok_Test")]
    public class TokenRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
        [System.ComponentModel.DataAnnotations.Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    private static (SqliteConnection Keeper, Func<DbContext> OpenCtx) CreateDb()
    {
        var dbName = $"dirop_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DirOp_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);
                CREATE TABLE DirOpTok_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL, Token BLOB)
                """;
            cmd.ExecuteNonQuery();
        }
        return (keeper, () =>
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider());
        });
    }

    [Fact]
    public async Task Direct_delete_of_tracked_entity_detaches_it_from_the_tracker()
    {
        var (keeper, openCtx) = CreateDb();
        using var _ = keeper;
        using var ctx = openCtx();

        ctx.Add(new Row { Id = 1, Val = 10 });
        await ctx.SaveChangesAsync();

        var tracked = await ctx.Query<Row>().FirstAsync(r => r.Id == 1);
        await ctx.DeleteAsync(tracked);

        // The row is gone; a later save must not resurrect it from the tracked corpse.
        tracked.Val = 99;
        await ctx.SaveChangesAsync();

        using var verifyCtx = openCtx();
        Assert.Empty(await verifyCtx.Query<Row>().ToListAsync());
    }

    [Fact]
    public async Task Direct_delete_of_tracked_token_entity_lets_the_same_key_be_re_added()
    {
        var (keeper, openCtx) = CreateDb();
        using var _ = keeper;
        using var ctx = openCtx();

        ctx.Add(new TokenRow { Id = 1, Val = 10 });
        await ctx.SaveChangesAsync();

        var tracked = await ctx.Query<TokenRow>().FirstAsync(r => r.Id == 1);
        await ctx.DeleteAsync(tracked);

        // The key is free again: adding a fresh instance must insert exactly one row
        // (no identity-map conflict with the deleted corpse, no phantom DELETE).
        ctx.Add(new TokenRow { Id = 1, Val = 20 });
        await ctx.SaveChangesAsync();

        using var verifyCtx = openCtx();
        var row = await verifyCtx.Query<TokenRow>().FirstAsync(r => r.Id == 1);
        Assert.Equal(20, row.Val);
    }

    [Fact]
    public async Task Direct_insert_of_an_added_entity_is_not_inserted_twice_by_save()
    {
        var (keeper, openCtx) = CreateDb();
        using var _ = keeper;
        using var ctx = openCtx();

        var row = new Row { Id = 1, Val = 10 };
        ctx.Add(row);
        await ctx.InsertAsync(row);

        // The entity is persisted; SaveChanges must not attempt a second INSERT.
        await ctx.SaveChangesAsync();

        using var verifyCtx = openCtx();
        var rows = await verifyCtx.Query<Row>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(10, rows[0].Val);
    }

    [Fact]
    public async Task Direct_update_of_tracked_entity_is_accepted_so_save_writes_nothing()
    {
        var (keeper, openCtx) = CreateDb();
        using var _ = keeper;
        using var ctx = openCtx();

        ctx.Add(new Row { Id = 1, Val = 10 });
        await ctx.SaveChangesAsync();

        var tracked = await ctx.Query<Row>().FirstAsync(r => r.Id == 1);
        tracked.Val = 20;
        await ctx.UpdateAsync(tracked);

        // The direct update accepted Val=20 as current DB state; a concurrent writer
        // then changes the row. SaveChanges has nothing pending for this entity and
        // must not overwrite the concurrent value with a redundant UPDATE.
        using (var concurrentCtx = openCtx())
        {
            var other = await concurrentCtx.Query<Row>().FirstAsync(r => r.Id == 1);
            other.Val = 777;
            await concurrentCtx.SaveChangesAsync();
        }

        await ctx.SaveChangesAsync();

        using var verifyCtx = openCtx();
        var row = await verifyCtx.Query<Row>().FirstAsync(r => r.Id == 1);
        Assert.Equal(777, row.Val);
    }
}
