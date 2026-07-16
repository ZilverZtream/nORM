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
/// Pins the AsAsyncEnumerable x change-tracking interaction, probed differentially: streamed
/// entities are change-tracked (a later edit + SaveChangesAsync persists), AsNoTracking
/// streams stay untracked, and streamed instances are identity-map canonical (the same
/// instance a list query returned). Guards the streaming neighbour of the fast-path tracking
/// kill, where the most common shapes silently returned untracked entities.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsyncStreamingTrackingContractTests
{
    [Table("AstRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static DbContext NewCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AstRow_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);" +
                "INSERT INTO AstRow_Test VALUES (1,10),(2,20),(3,30);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public async Task Streamed_entities_are_tracked_and_edits_persist()
    {
        await using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        Row? second = null;
        await foreach (var row in ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable())
        {
            if (row.Id == 2) second = row;
        }
        Assert.NotNull(second);

        var trackedCount = ctx.ChangeTracker.Entries.Count(e => e.Entity is Row);
        Console.WriteLine($"tracked after stream: {trackedCount}");

        second!.Val = 99;
        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Val FROM AstRow_Test WHERE Id = 2";
        var persisted = Convert.ToInt32(check.ExecuteScalar());
        Console.WriteLine($"persisted Val: {persisted}");
        Assert.Equal(99, persisted);
        Assert.Equal(3, trackedCount);
    }

    [Fact]
    public async Task No_tracking_streams_stay_untracked()
    {
        await using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        await foreach (var _ in ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().AsAsyncEnumerable()) { }
        Assert.Equal(0, ctx.ChangeTracker.Entries.Count(e => e.Entity is Row));
    }

    [Fact]
    public async Task Streamed_instances_are_identity_map_canonical()
    {
        await using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var listed = (await ctx.Query<Row>().Where(r => r.Id == 1).ToListAsync()).Single();
        Row? streamed = null;
        await foreach (var row in ctx.Query<Row>().AsAsyncEnumerable())
        {
            if (row.Id == 1) streamed = row;
        }
        Assert.Same(listed, streamed);
    }
}
