using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Npgsql;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that bulk operations participate in an ambient transaction
/// rather than creating their own independent transaction.
/// </summary>
[Xunit.Trait("Category", "Fast")]
[Xunit.Trait("Category", TestCategory.BulkProviderParity)]
public class BulkTransactionAtomicityTests
{
    private class Item
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("PgBulkAtomicRows")]
    private class PgBulkAtomicRow
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

    [Fact]
    [Xunit.Trait("Category", TestCategory.ProviderParity)]
    public async Task Postgres_BulkUpdate_BatchFailure_RollsBackEarlierBatches_Live()
    {
        var cs = LiveProviderEnvironment.GetConnectionString("postgres");
        if (string.IsNullOrEmpty(cs)) return;

        await using var cn = new NpgsqlConnection(cs);
        await cn.OpenAsync();

        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = """
                DROP TABLE IF EXISTS "PgBulkAtomicRows";
                CREATE TABLE "PgBulkAtomicRows" ("Id" INTEGER PRIMARY KEY, "Name" TEXT NOT NULL);
                INSERT INTO "PgBulkAtomicRows" ("Id", "Name")
                SELECT i, 'original' FROM generate_series(1, 11) AS s(i);
                """;
            await setup.ExecuteNonQueryAsync();
        }

        using var ctx = new DbContext(cn, new PostgresProvider(new SqliteParameterFactory()));
        var updates = Enumerable.Range(1, 11)
            .Select(i => new PgBulkAtomicRow
            {
                Id = i,
                Name = i == 11 ? null! : "updated"
            })
            .ToArray();

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.BulkUpdateAsync(updates));

        await using var probe = cn.CreateCommand();
        probe.CommandText = "SELECT COUNT(*) FROM \"PgBulkAtomicRows\" WHERE \"Name\" = 'updated'";
        var leakedUpdates = Convert.ToInt32(await probe.ExecuteScalarAsync());
        Assert.Equal(0, leakedUpdates);
    }
}
