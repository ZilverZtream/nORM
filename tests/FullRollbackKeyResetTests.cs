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
/// Under a caller-controlled transaction, SaveChanges stamps DB-generated keys onto entities but
/// leaves them Added (AcceptChanges is skipped). A full <c>tx.RollbackAsync()</c> undoes the inserted
/// rows in the DB, but the entities keep their stamped keys and Added state, so the next SaveChanges
/// silently drops them via the "skip already-inserted" guard. The rollback must reset those keys so
/// the still-Added entities are re-inserted. (Same class as the savepoint fix, different rollback path.)
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class FullRollbackKeyResetTests
{
    [Table("FrOrder")]
    private class FrOrder
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static int RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM FrOrder";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Full_rollback_reinserts_still_added_entity_on_next_save()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FrOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new FrOrder { Name = "A" });
        await ctx.SaveChangesAsync();   // A inserted in the transaction, Id stamped, A stays Added
        await tx.RollbackAsync();        // A's row is undone; A still Added with its stamped key

        await ctx.SaveChangesAsync();    // must re-insert A (its key was reset)

        Assert.Equal(1, RowCount(cn));   // BUG: 0 — A was silently dropped by the skip-already-inserted guard
    }
}
