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
/// Pins the EF-parity database lifecycle surface on <see cref="DatabaseFacade"/>:
/// EnsureDeletedAsync drops the mapped tables (inverse of EnsureCreatedAsync), and CanConnect(Async)
/// reports reachability.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DatabaseLifecycleContractTests
{
    [Table("DblRow")]
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

    [Fact]
    public async Task EnsureCreated_EnsureDeleted_round_trips()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = NewCtx(cn);

        Assert.True(await ctx.Database.EnsureCreatedAsync());    // created
        Assert.False(await ctx.Database.EnsureCreatedAsync());   // already exists
        Assert.True(await ctx.Database.EnsureDeletedAsync());    // dropped
        Assert.False(await ctx.Database.EnsureDeletedAsync());   // nothing left to drop
        Assert.True(await ctx.Database.EnsureCreatedAsync());    // recreated
    }

    [Fact]
    public async Task EnsureDeletedAsync_removes_the_table_and_its_data()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = NewCtx(cn);

        await ctx.Database.EnsureCreatedAsync();
        ctx.Add(new Row { Id = 1, Name = "x" });
        await ctx.SaveChangesAsync();
        Assert.Equal(1, await ctx.Query<Row>().CountAsync());

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();   // recreate an empty table

        Assert.Equal(0, await ctx.Query<Row>().CountAsync());
    }

    [Fact]
    public async Task CanConnect_is_true_for_a_reachable_database()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = NewCtx(cn);

        Assert.True(await ctx.Database.CanConnectAsync());
        Assert.True(ctx.Database.CanConnect());
    }
}
