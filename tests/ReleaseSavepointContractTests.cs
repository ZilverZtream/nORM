using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contracts for <c>ReleaseSavepointAsync</c> (EF Core <c>IDbContextTransaction.ReleaseSavepointAsync</c>
/// parity). Releasing a savepoint KEEPS the work done since it was created (unlike a rollback) and removes it
/// as a rollback target. nORM also drops the savepoint's DB-generated-key snapshot on release.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReleaseSavepointContractTests
{
    [Table("SpRelWidget")]
    private class Widget
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpRelWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
        }, ownsConnection: false);
    }

    private static long Count(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM SpRelWidget";
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Releasing_a_savepoint_keeps_the_work_done_since_it()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.InsertAsync(new Widget { Name = "before" });
            await tx.CreateSavepointAsync("sp1");
            await ctx.InsertAsync(new Widget { Name = "after" });   // work done AFTER the savepoint
            await tx.ReleaseSavepointAsync("sp1");                  // release keeps 'after', unlike a rollback
            await tx.CommitAsync();
        }

        Assert.Equal(2, Count(cn));   // both rows persisted — release did not discard the post-savepoint insert
    }

    [Fact]
    public async Task A_released_savepoint_is_no_longer_a_rollback_target()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new Widget { Name = "before" });
        await tx.CreateSavepointAsync("sp1");
        await ctx.InsertAsync(new Widget { Name = "after" });
        await tx.ReleaseSavepointAsync("sp1");

        // The savepoint is gone; rolling back to it must fail rather than silently succeed.
        await Assert.ThrowsAnyAsync<Exception>(() => tx.RollbackToSavepointAsync("sp1"));

        await tx.RollbackAsync();
    }

    [Fact]
    public async Task Releasing_a_savepoint_with_a_blank_name_throws()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        await using var tx = await ctx.Database.BeginTransactionAsync();
        await tx.CreateSavepointAsync("sp1");
        await Assert.ThrowsAsync<ArgumentException>(() => tx.ReleaseSavepointAsync("  "));
        await tx.RollbackAsync();
    }
}
