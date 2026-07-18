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
/// Pins the EF-parity disconnected-update surface: a public <see cref="EntityEntry.State"/> setter
/// that performs the corresponding tracker transition — Modified → full UPDATE, Deleted → DELETE (or
/// detach for a never-inserted Added entity), Unchanged → accept current values as the clean baseline,
/// Detached → stop tracking. Before this, State had an internal setter so the idiomatic
/// <c>ctx.Entry(e).State = EntityState.Modified</c> did not compile.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntityStateSetterContractTests
{
    [Table("EssRow")]
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

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE EssRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static async Task SeedAsync(SqliteConnection cn, Row row)
    {
        await using var seed = NewCtx(cn);
        seed.Add(row);
        await seed.SaveChangesAsync();
    }

    [Fact]
    public async Task Setting_State_Modified_on_an_attached_entity_updates_all_columns()
    {
        using var cn = OpenDb();
        await SeedAsync(cn, new Row { Id = 1, Name = "orig", Value = 10 });

        // Disconnected: a fresh instance the context never queried, attached then flagged Modified.
        await using (var ctx = NewCtx(cn))
        {
            var detached = new Row { Id = 1, Name = "changed", Value = 99 };
            ctx.Attach(detached);
            ctx.Entry(detached).State = EntityState.Modified;
            await ctx.SaveChangesAsync();
        }

        await using var verify = NewCtx(cn);
        var row = await verify.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1);
        Assert.NotNull(row);
        Assert.Equal("changed", row!.Name);
        Assert.Equal(99, row.Value);
    }

    [Fact]
    public async Task Setting_State_Deleted_removes_the_row()
    {
        using var cn = OpenDb();
        await SeedAsync(cn, new Row { Id = 1, Name = "x", Value = 1 });

        await using (var ctx = NewCtx(cn))
        {
            var e = new Row { Id = 1, Name = "x", Value = 1 };
            ctx.Attach(e);
            ctx.Entry(e).State = EntityState.Deleted;
            await ctx.SaveChangesAsync();
        }

        await using var verify = NewCtx(cn);
        Assert.Equal(0, await verify.Query<Row>().CountAsync());
    }

    [Fact]
    public async Task Setting_State_Detached_stops_tracking_the_entity()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var e = new Row { Id = 5, Name = "y", Value = 2 };
        ctx.Attach(e);
        ctx.Entry(e).State = EntityState.Detached;

        // Detached → no longer tracked, so Entry() throws and SaveChanges persists nothing.
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(e));
        Assert.Equal(0, await ctx.SaveChangesAsync());
        Assert.Equal(0, await ctx.Query<Row>().CountAsync());
    }

    [Fact]
    public async Task Setting_an_Added_entity_to_Deleted_detaches_it_without_inserting()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var e = new Row { Id = 7, Name = "z", Value = 3 };
        var entry = ctx.Add(e);              // Added
        entry.State = EntityState.Deleted;   // never inserted → just stop tracking

        Assert.Equal(0, await ctx.SaveChangesAsync());
        Assert.Equal(0, await ctx.Query<Row>().CountAsync());
    }

    [Fact]
    public async Task Setting_State_Unchanged_accepts_current_values_and_suppresses_the_update()
    {
        using var cn = OpenDb();
        await SeedAsync(cn, new Row { Id = 1, Name = "orig", Value = 10 });

        await using (var ctx = NewCtx(cn))
        {
            var e = await ctx.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1);
            e!.Name = "dirty";                          // a pending modification
            ctx.Entry(e).State = EntityState.Unchanged; // accept current as the clean baseline
            Assert.Equal(0, await ctx.SaveChangesAsync());
        }

        await using var verify = NewCtx(cn);
        var row = await verify.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1);
        Assert.Equal("orig", row!.Name);   // DB never received the pending change
    }
}
