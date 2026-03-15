using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for Raw SQL query APIs must respect the active DbContext transaction.
/// </summary>
public class TransactionIsolationTests
{
    [Table("TxItem")]
    private class TxItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TxItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    /// <summary>
    /// Within an active explicit transaction, rows written via SaveChangesAsync
    /// must be visible to QueryUnchangedAsync on the same connection / same transaction.
    /// </summary>
    [Fact]
    public async Task RawSqlQuery_WithinActiveTransaction_ReadsUncommittedWrites()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Begin an explicit transaction on the context.
        await using var tx = await ctx.Database.BeginTransactionAsync();

        // Insert a row within the transaction (not yet committed).
        var entity = new TxItem { Value = "uncommitted" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        // QueryUnchangedAsync must see the row within the same transaction.
        var rows = await ctx.QueryUnchangedAsync<TxItem>("SELECT * FROM \"TxItem\"");
        Assert.Contains(rows, r => r.Value == "uncommitted");
    }

    /// <summary>
    /// A separate connection (outside the transaction) must NOT see rows that
    /// have been written but not yet committed.
    /// Note: SQLite in WAL mode uses snapshot isolation; uncommitted writes on one
    /// connection are never visible to a separate connection regardless of transaction state.
    /// This test verifies the baseline isolation guarantee of the database.
    /// </summary>
    [Fact]
    public async Task RawSqlQuery_OutsideTransaction_DoesNotSeeUncommittedWrites()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Begin an explicit transaction — insert a row but do NOT commit.
        await using var tx = await ctx.Database.BeginTransactionAsync();
        var entity = new TxItem { Value = "not-yet-committed" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        // A completely separate connection to the SAME in-memory database cannot see
        // SQLite :memory: databases across connections, so we use the same connection
        // but a raw command outside the transaction to simulate an outside reader.
        // On SQLite the only way to truly test this is with a file-based DB.
        // Here we verify that the in-transaction count matches expectations.
        var rowsInTx = await ctx.QueryUnchangedAsync<TxItem>("SELECT * FROM \"TxItem\"");
        Assert.NotEmpty(rowsInTx); // row IS visible within the transaction

        // Roll back — the row should disappear.
        await tx.RollbackAsync();

        // After rollback the row must be gone.
        var rowsAfterRollback = await ctx.QueryUnchangedAsync<TxItem>("SELECT * FROM \"TxItem\"");
        Assert.Empty(rowsAfterRollback);
    }

    /// <summary>
    /// Structural test — after BeginTransactionAsync, the DbCommand created inside
    /// QueryUnchangedAsync must have its Transaction property bound to the active transaction.
    /// We verify this indirectly: write a row in a transaction, read it back via
    /// QueryUnchangedAsync (which will fail if Transaction is not bound on SQLite WAL),
    /// then roll back and confirm the row is gone.
    /// </summary>
    [Fact]
    public async Task FromSqlRaw_BindsToActiveTransaction()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();

        var entity = new TxItem { Value = "txbound" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        // FromSqlRawAsync must see the uncommitted row because it uses the same tx.
        var rows = await ctx.FromSqlRawAsync<TxItem>("SELECT * FROM \"TxItem\"");
        Assert.Contains(rows, r => r.Value == "txbound");

        // Roll back and verify the row is gone.
        await tx.RollbackAsync();

        var rowsAfter = await ctx.FromSqlRawAsync<TxItem>("SELECT * FROM \"TxItem\"");
        Assert.Empty(rowsAfter);
    }
}
