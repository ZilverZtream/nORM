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
/// leaves them Added (AcceptChanges is skipped because the caller owns durability). If the caller
/// rolls back to a savepoint, the inserted row is gone from the DB but the entity keeps its stamped
/// key and Added state, so the "skip already-inserted" guard silently drops it from the next
/// SaveChanges — the row is lost with no error. The rollback must reset keys stamped since the
/// savepoint so the retry re-inserts them.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SavepointRollbackKeyResetTests
{
    [Table("SpOrder")]
    private class SpOrder
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static int RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM SpOrder";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task RollbackToSavepoint_reinserts_entity_saved_after_the_savepoint()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        ctx.Add(new SpOrder { Name = "A" });
        await ctx.SaveChangesAsync();          // A inserted, Id stamped, A stays Added

        await tx.CreateSavepointAsync("sp");

        var b = new SpOrder { Name = "B" };
        ctx.Add(b);
        await ctx.SaveChangesAsync();          // B inserted, Id stamped, B stays Added

        await tx.RollbackToSavepointAsync("sp"); // B's row is undone in the DB; B still Added with its key

        await ctx.SaveChangesAsync();          // must re-insert B (its key was reset)
        await tx.CommitAsync();

        Assert.Equal(2, RowCount(cn)); // BUG: 1 — B was silently dropped by the skip-already-inserted guard
    }
}
