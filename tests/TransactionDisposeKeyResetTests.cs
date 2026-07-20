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
/// Under a caller-controlled transaction, SaveChanges stamps DB-generated keys onto entities but leaves
/// them Added (AcceptChanges is skipped so they can be re-inserted if the transaction is undone). Disposing
/// an owned transaction WITHOUT committing rolls the inserted rows back in the database (ADO.NET contract) —
/// the canonical `await using (tx) { ...; Commit(); }` idiom takes this path whenever an exception or early
/// return skips the commit. The dispose must reset the stamped keys just like an explicit Rollback, or the
/// still-Added entities are silently dropped by the skip-already-inserted guard on the next SaveChanges.
/// A committed transaction must NOT reset (its rows are durable).
/// </summary>
[Trait("Category", "Fast")]
public class TransactionDisposeKeyResetTests
{
    [Table("TdOrder")]
    private class TdOrder
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static int RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TdOrder";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TdOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Async_dispose_without_commit_reinserts_still_added_entity_on_next_save()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new TdOrder { Name = "A" });
            await ctx.SaveChangesAsync();   // A inserted in the transaction, Id stamped, A stays Added
            // block exits WITHOUT CommitAsync (an exception or early return would do the same)
        }                                    // tx disposed -> DB rolls A back; A still Added with stamped key

        await ctx.SaveChangesAsync();        // must re-insert A (its key was reset on dispose)
        Assert.Equal(1, RowCount(cn));       // BUG before fix: 0 - A was silently dropped
    }

    [Fact]
    public async Task Sync_dispose_without_commit_reinserts_still_added_entity_on_next_save()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var tx = ctx.Database.BeginTransaction();
        ctx.Add(new TdOrder { Name = "A" });
        await ctx.SaveChangesAsync();
        tx.Dispose();                        // sync dispose, no commit -> DB rolls back

        await ctx.SaveChangesAsync();
        Assert.Equal(1, RowCount(cn));
    }

    [Fact]
    public async Task Committed_transaction_does_not_reset_or_duplicate_on_dispose()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new TdOrder { Name = "A" });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();          // commit inside the block; block-exit dispose must not reset
        }

        await ctx.SaveChangesAsync();        // nothing new should be inserted (A is durable, not re-added)
        Assert.Equal(1, RowCount(cn));       // exactly one row, no duplicate
    }
}
