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
/// Pins the EF-parity change-detection surface on <see cref="ChangeTracker"/>:
/// HasChanges() reports whether any tracked entity is pending, and DetectChanges() forces state
/// detection for tracked entities.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ChangeTrackerHasChangesContractTests
{
    [Table("CtcRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> BootstrapAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CtcRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var seed = NewCtx(cn);
        seed.Add(new Row { Id = 1, Name = "orig" });
        await seed.SaveChangesAsync();
        await seed.DisposeAsync();
        return (cn, NewCtx(cn));
    }

    [Fact]
    public async Task HasChanges_tracks_pending_modifications_and_clears_after_save()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        var row = (await ctx.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1))!;
        Assert.False(ctx.ChangeTracker.HasChanges());   // nothing pending yet

        row.Name = "changed";
        Assert.True(ctx.ChangeTracker.HasChanges());     // pending update detected

        await ctx.SaveChangesAsync();
        Assert.False(ctx.ChangeTracker.HasChanges());    // cleared after save
    }

    [Fact]
    public async Task HasChanges_is_true_after_Add()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        Assert.False(ctx.ChangeTracker.HasChanges());
        ctx.Add(new Row { Id = 2, Name = "new" });
        Assert.True(ctx.ChangeTracker.HasChanges());
    }

    [Fact]
    public async Task DetectChanges_updates_the_entry_state()
    {
        var (cn, ctx) = await BootstrapAsync();
        using var _cn = cn;
        await using var _ctx = ctx;

        var row = (await ctx.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1))!;
        row.Name = "changed";

        ctx.ChangeTracker.DetectChanges();

        Assert.Equal(EntityState.Modified, ctx.Entry(row).State);
    }
}
