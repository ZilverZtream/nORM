using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TX-1: Verifies that bulk operations participate in an ambient transaction
/// rather than creating their own independent transaction.
/// </summary>
public class BulkTransactionAtomicityTests
{
    private class Item
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Item(Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ─── BulkInsertAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task BulkInsertAsync_ExplicitTx_Rollback_LeavesNoRows()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        await ctx.BulkInsertAsync(new[] { new Item { Id = 1, Name = "A" } });

        await tx.RollbackAsync();

        var count = ctx.Query<Item>().Count();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task BulkInsertAsync_ExplicitTx_Commit_RowsPersisted()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        await ctx.BulkInsertAsync(new[] { new Item { Id = 1, Name = "A" } });

        await tx.CommitAsync();

        var count = ctx.Query<Item>().Count();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task BulkInsertAsync_NoAmbientTx_StillWorks()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await ctx.BulkInsertAsync(new[] { new Item { Id = 1, Name = "A" }, new Item { Id = 2, Name = "B" } });

        var count = ctx.Query<Item>().Count();
        Assert.Equal(2, count);
    }

    // ─── BulkDeleteAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task BulkDeleteAsync_ExplicitTx_Rollback_RowsStillPresent()
    {
        using var cn = CreateConnection();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Item(Id, Name) VALUES(1,'A'),(2,'B');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        var toDelete = new[] { new Item { Id = 1, Name = "A" }, new Item { Id = 2, Name = "B" } };
        await ctx.BulkDeleteAsync(toDelete);

        await tx.RollbackAsync();

        var count = ctx.Query<Item>().Count();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task BulkDeleteAsync_ExplicitTx_Commit_RowsDeleted()
    {
        using var cn = CreateConnection();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Item(Id, Name) VALUES(1,'A'),(2,'B');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        var toDelete = new[] { new Item { Id = 1, Name = "A" }, new Item { Id = 2, Name = "B" } };
        await ctx.BulkDeleteAsync(toDelete);

        await tx.CommitAsync();

        var count = ctx.Query<Item>().Count();
        Assert.Equal(0, count);
    }
}
