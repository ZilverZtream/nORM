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
/// Pins the EF-parity <see cref="EntityEntry.CurrentValues"/> / <see cref="EntityEntry.OriginalValues"/>
/// property-bag surface: read/write by property name, original-vs-current after a mutation, SetValues,
/// ToObject cloning, and the read-only original key.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntityEntryPropertyValuesContractTests
{
    [Table("EepvRow")]
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

    private static async Task<(SqliteConnection cn, DbContext ctx, Row row)> BootstrapAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EepvRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
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
    public async Task CurrentValues_reads_the_live_entity_and_writes_persist()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        var entry = ctx.Entry(row);
        Assert.Equal("orig", entry.CurrentValues["Name"]);
        Assert.Equal(10, entry.CurrentValues["Value"]);

        entry.CurrentValues["Name"] = "written";
        Assert.Equal("written", row.Name);           // write reached the entity
        await ctx.SaveChangesAsync();

        await using var verify = NewCtx(cn);
        var reloaded = await verify.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1);
        Assert.Equal("written", reloaded!.Name);      // and the database
    }

    [Fact]
    public async Task OriginalValues_reflect_the_as_loaded_baseline_after_a_mutation()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        row.Name = "changed";
        row.Value = 999;

        var entry = ctx.Entry(row);
        Assert.Equal("orig", entry.OriginalValues["Name"]);
        Assert.Equal(10, entry.OriginalValues["Value"]);
        Assert.Equal("changed", entry.CurrentValues["Name"]);
        Assert.Equal(999, entry.CurrentValues["Value"]);
    }

    [Fact]
    public async Task SetValues_copies_all_values_from_another_instance()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        ctx.Entry(row).CurrentValues.SetValues(new Row { Id = 1, Name = "bulk", Value = 77 });

        Assert.Equal("bulk", row.Name);
        Assert.Equal(77, row.Value);
    }

    [Fact]
    public async Task ToObject_clones_current_values_into_a_detached_instance()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        var clone = (Row)ctx.Entry(row).CurrentValues.ToObject();

        Assert.NotSame(row, clone);
        Assert.Equal(row.Id, clone.Id);
        Assert.Equal(row.Name, clone.Name);
        Assert.Equal(row.Value, clone.Value);
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(clone)); // not tracked
    }

    [Fact]
    public async Task Original_key_value_is_read_only()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        var entry = ctx.Entry(row);
        Assert.Equal(1, entry.OriginalValues["Id"]);
        Assert.Throws<InvalidOperationException>(() => entry.OriginalValues["Id"] = 5);
    }

    [Fact]
    public async Task Properties_lists_every_mapped_column()
    {
        var (cn, ctx, row) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        var props = ctx.Entry(row).CurrentValues.Properties;
        Assert.Contains("Id", props);
        Assert.Contains("Name", props);
        Assert.Contains("Value", props);
    }
}
