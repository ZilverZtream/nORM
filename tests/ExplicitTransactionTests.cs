using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies explicit transaction semantics for begin/commit and begin/rollback
/// using SQLite in-memory databases.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ExplicitTransactionTests
{
    [Table("TxItem")]
    private class TxItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) BuildContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── Begin / Commit ────────────────────────────────────────────────────────

    /// <summary>
    /// Rows inserted inside a committed explicit transaction must be visible
    /// after the transaction completes.
    /// </summary>
    [Fact]
    public async Task BeginAndCommit_InsertsAreVisible()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new TxItem { Name = "committed-row" });
        await tx.CommitAsync();

        var rows = await ctx.Query<TxItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("committed-row", rows[0].Name);
    }

    /// <summary>
    /// Multiple SaveChanges calls inside a single explicit transaction are
    /// treated as one atomic unit. All rows appear after commit.
    /// </summary>
    [Fact]
    public async Task ExplicitTransaction_MultipleInserts_AllCommitTogether()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new TxItem { Name = "row-1" });
        await ctx.InsertAsync(new TxItem { Name = "row-2" });
        await tx.CommitAsync();

        var rows = await ctx.Query<TxItem>().ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    // ── Begin / Rollback ──────────────────────────────────────────────────────

    /// <summary>
    /// Rows inserted inside a rolled-back explicit transaction must NOT
    /// appear after the rollback.
    /// </summary>
    [Fact]
    public async Task BeginAndRollback_InsertsAreNotVisible()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new TxItem { Name = "should-not-persist" });
        await tx.RollbackAsync();

        var rows = await ctx.Query<TxItem>().ToListAsync();
        Assert.Empty(rows);
    }

    /// <summary>
    /// After rolling back a transaction the context can begin a new transaction
    /// and commit successfully.
    /// </summary>
    [Fact]
    public async Task AfterRollback_NewTransactionSucceeds()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        // First transaction — rolled back.
        await using (var tx1 = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.InsertAsync(new TxItem { Name = "rollback-me" });
            await tx1.RollbackAsync();
        }

        // Second transaction — committed.
        await using (var tx2 = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.InsertAsync(new TxItem { Name = "keep-me" });
            await tx2.CommitAsync();
        }

        var rows = await ctx.Query<TxItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("keep-me", rows[0].Name);
    }

    // ── Double-begin guard ────────────────────────────────────────────────────

    /// <summary>
    /// Opening a second explicit transaction while the first is still active
    /// must throw InvalidOperationException.
    /// </summary>
    [Fact]
    public async Task BeginTransaction_WhileActiveTransaction_Throws()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        await using var tx = await ctx.Database.BeginTransactionAsync();

        await Assert.ThrowsAsync<NormUsageException>(() =>
            ctx.Database.BeginTransactionAsync());

        await tx.RollbackAsync();
    }
}
