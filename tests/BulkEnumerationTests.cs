using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// PERF-1: Verifies that BulkInsertAsync/UpdateAsync/DeleteAsync materialize the IEnumerable
/// exactly once. Without the fix, a yield-return iterator is exhausted after the first
/// enumeration (ValidateBulkOperation) and the provider receives an empty sequence,
/// silently losing all writes.
/// </summary>
public class BulkEnumerationTests
{
    [Table("BulkEnumItem")]
    private class BulkEnumItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static SqliteConnection CreateSchema()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BulkEnumItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn)
        => new DbContext(cn, new SqliteProvider(), new DbContextOptions());

    private static async Task<int> CountRowsAsync(SqliteConnection cn)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM BulkEnumItem";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private static async Task SeedAsync(SqliteConnection cn, IEnumerable<BulkEnumItem> items)
    {
        foreach (var item in items)
        {
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO BulkEnumItem (Id, Name) VALUES (@id, @name)";
            cmd.Parameters.AddWithValue("@id", item.Id);
            cmd.Parameters.AddWithValue("@name", item.Name);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    // ── Yield-return iterators ────────────────────────────────────────────────

    private static IEnumerable<BulkEnumItem> YieldItems(int count)
    {
        for (int i = 1; i <= count; i++)
            yield return new BulkEnumItem { Id = i, Name = $"Item{i}" };
    }

    private static IEnumerable<BulkEnumItem> YieldItemsForUpdate(int count)
    {
        for (int i = 1; i <= count; i++)
            yield return new BulkEnumItem { Id = i, Name = $"Updated{i}" };
    }

    private static IEnumerable<BulkEnumItem> YieldItemsForDelete(int count)
    {
        for (int i = 1; i <= count; i++)
            yield return new BulkEnumItem { Id = i, Name = $"Item{i}" };
    }

    [Fact]
    public async Task BulkInsert_WithYieldIterator_InsertsAllRows()
    {
        using var cn = CreateSchema();
        using var ctx = CreateCtx(cn);

        int inserted = await ctx.BulkInsertAsync(YieldItems(5));

        Assert.Equal(5, inserted);
        Assert.Equal(5, await CountRowsAsync(cn));
    }

    [Fact]
    public async Task BulkUpdate_WithYieldIterator_UpdatesAllRows()
    {
        using var cn = CreateSchema();
        using var ctx = CreateCtx(cn);

        // Seed directly
        await SeedAsync(cn, YieldItems(5));

        // Update via yield iterator
        int updated = await ctx.BulkUpdateAsync(YieldItemsForUpdate(5));

        Assert.Equal(5, updated);

        // Verify names were actually changed
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM BulkEnumItem WHERE Name LIKE 'Updated%'";
        var updatedCount = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        Assert.Equal(5, updatedCount);
    }

    [Fact]
    public async Task BulkDelete_WithYieldIterator_DeletesAllRows()
    {
        using var cn = CreateSchema();
        using var ctx = CreateCtx(cn);

        await SeedAsync(cn, YieldItems(5));
        Assert.Equal(5, await CountRowsAsync(cn));

        int deleted = await ctx.BulkDeleteAsync(YieldItemsForDelete(5));

        Assert.Equal(5, deleted);
        Assert.Equal(0, await CountRowsAsync(cn));
    }

    [Fact]
    public async Task BulkInsert_WithSideEffectEnumerable_OnlyEnumeratesOnce()
    {
        using var cn = CreateSchema();
        using var ctx = CreateCtx(cn);

        int enumerationCount = 0;
        IEnumerable<BulkEnumItem> CountingIterator()
        {
            enumerationCount++;
            for (int i = 1; i <= 3; i++)
                yield return new BulkEnumItem { Id = i, Name = $"Item{i}" };
        }

        await ctx.BulkInsertAsync(CountingIterator());

        // Must enumerate exactly once regardless of internal validation steps
        Assert.Equal(1, enumerationCount);
        Assert.Equal(3, await CountRowsAsync(cn));
    }
}
