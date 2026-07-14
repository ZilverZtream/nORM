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
/// Update with a detached instance: over an UNTRACKED key it attaches as
/// Modified and persists the detached values; over a TRACKED key it throws an
/// identity conflict — the old behavior adopted Modified onto the tracked
/// entry, whose unchanged values produced no diff, and the detached values
/// silently vanished (lost update).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DetachedUpdateConflictTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DetUpd_Test")]
    public class DetUpdRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static SqliteConnection CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DetUpd_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Update_with_detached_instance_over_tracked_key_throws_identity_conflict()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new DetUpdRow { Id = 1, Val = 10 });
        await ctx.SaveChangesAsync();

        var tracked = await ctx.Query<DetUpdRow>().FirstAsync(r => r.Id == 1);
        Assert.Throws<InvalidOperationException>(() => ctx.Update(new DetUpdRow { Id = 1, Val = 99 }));

        // The supported path: mutate the tracked instance.
        tracked.Val = 99;
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        Assert.Equal(99, (await ctx.Query<DetUpdRow>().FirstAsync(r => r.Id == 1)).Val);
    }

    [Fact]
    public async Task Update_with_detached_instance_over_untracked_key_persists_the_detached_values()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new DetUpdRow { Id = 1, Val = 10 });
        await ctx.SaveChangesAsync();

        // Untrack the row so the detached update is a legitimate attach-as-modified.
        ctx.ChangeTracker.Clear();
        ctx.Update(new DetUpdRow { Id = 1, Val = 99 });
        await ctx.SaveChangesAsync();

        ctx.ChangeTracker.Clear();
        Assert.Equal(99, (await ctx.Query<DetUpdRow>().FirstAsync(r => r.Id == 1)).Val);
    }
}
