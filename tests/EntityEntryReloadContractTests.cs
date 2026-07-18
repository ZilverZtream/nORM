using System;
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
/// Pins the EF-parity <see cref="EntityEntry.Reload"/> / <see cref="EntityEntry.ReloadAsync"/> surface:
/// a fresh read of the entity's row overwrites its column values and resets the entry to Unchanged
/// (discarding pending edits); a vanished row detaches the entity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntityEntryReloadContractTests
{
    [Table("EerRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static void ExecSql(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static async Task<(SqliteConnection cn, DbContext ctx, Row row)> BootstrapAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, "CREATE TABLE EerRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);");
        await using (var seed = NewCtx(cn))
        {
            seed.Add(new Row { Id = 1, Name = "orig", Value = 10 });
            await seed.SaveChangesAsync();
        }
        var ctx = NewCtx(cn);
        var row = (await ctx.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1))!;
        return (cn, ctx, row);
    }

    [Fact]
    public async Task ReloadAsync_overwrites_local_edits_with_database_values_and_resets_state()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        // The row changed underneath us, and we have unsaved local edits.
        ExecSql(cn, "UPDATE EerRow SET Name = 'dbNew', Value = 42 WHERE Id = 1;");
        row.Name = "localEdit";
        row.Value = 999;

        await ctx.Entry(row).ReloadAsync();

        Assert.Equal("dbNew", row.Name);       // fresh DB values won, not the local edit
        Assert.Equal(42, row.Value);
        Assert.Equal(EntityState.Unchanged, ctx.Entry(row).State);
    }

    [Fact]
    public async Task Reload_sync_overwrites_local_edits_with_database_values()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        ExecSql(cn, "UPDATE EerRow SET Name = 'dbSync', Value = 7 WHERE Id = 1;");
        row.Name = "localEdit";

        ctx.Entry(row).Reload();

        Assert.Equal("dbSync", row.Name);
        Assert.Equal(7, row.Value);
    }

    [Fact]
    public async Task Reload_detaches_the_entity_when_its_row_no_longer_exists()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        ExecSql(cn, "DELETE FROM EerRow WHERE Id = 1;");

        await ctx.Entry(row).ReloadAsync();

        Assert.Throws<InvalidOperationException>(() => ctx.Entry(row)); // no longer tracked
    }

    [Fact]
    public async Task Reload_discards_pending_changes_so_a_later_save_persists_nothing()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        row.Name = "dirty";
        await ctx.Entry(row).ReloadAsync();   // discard the pending edit
        Assert.Equal(0, await ctx.SaveChangesAsync());

        await using var verify = NewCtx(cn);
        var reloaded = await verify.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1);
        Assert.Equal("orig", reloaded!.Name);
    }
}
