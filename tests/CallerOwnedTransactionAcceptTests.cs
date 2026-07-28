using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Under a caller-owned transaction (<c>Database.BeginTransaction</c>) nORM defers ChangeTracker
/// acceptance out of SaveChanges to commit time, because the caller owns durability. Commit-time
/// acceptance must reconcile ALL flushed states — inserts, updates AND deletes. If a committed
/// delete stays tracked as <see cref="EntityState.Deleted"/>, the next SaveChanges silently
/// re-issues its DELETE (data loss); a committed update left <see cref="EntityState.Modified"/>
/// re-issues its UPDATE under <c>detectChanges:false</c> (lost update). These pin the delete/update
/// reconciliation so it matches EF Core, where a committed change is fully accepted.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CallerOwnedTransactionAcceptTests
{
    [Table("TxManual")]
    private class TxManual
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("TxOcc")]
    private class TxOcc
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        [Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    private static SqliteConnection OpenManualDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxManual (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static SqliteConnection OpenOccDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxOcc (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Token BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void RawInsertManual(SqliteConnection cn, int id, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO TxManual (Id, Name) VALUES ({id}, '{name}')";
        cmd.ExecuteNonQuery();
    }

    private static string? NameOfManual(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Name FROM TxManual WHERE Id = {id}";
        return cmd.ExecuteScalar() as string;
    }

    private static long CountManual(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TxManual";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static long CountOcc(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TxOcc";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task CallerOwned_delete_commit_then_unrelated_save_does_not_redelete_recreated_row()
    {
        using var cn = OpenManualDb();
        RawInsertManual(cn, 1, "v1");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var e = new TxManual { Id = 1, Name = "v1" };
        ctx.Attach(e);
        ctx.Remove(e);
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        }
        Assert.Equal(0L, CountManual(cn));   // delete committed

        // A different code path re-creates a row at Id=1 with important data.
        RawInsertManual(cn, 1, "IMPORTANT-NEW-DATA");

        // An UNRELATED save (inserts Id=2) must not re-emit the committed delete.
        ctx.Add(new TxManual { Id = 2, Name = "unrelated" });
        await ctx.SaveChangesAsync();

        Assert.Equal("IMPORTANT-NEW-DATA", NameOfManual(cn, 1));
    }

    [Fact]
    public async Task CallerOwned_deleteOcc_commit_then_unrelated_save_does_not_false_conflict()
    {
        using var cn = OpenOccDb();
        using (var s = cn.CreateCommand())
        {
            s.CommandText = "INSERT INTO TxOcc (Id, Name, Token) VALUES (1, 'occ', X'01')";
            s.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var e = new TxOcc { Id = 1, Name = "occ", Token = new byte[] { 0x01 } };
        ctx.Attach(e);
        ctx.Remove(e);
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        }
        Assert.Equal(0L, CountOcc(cn));   // delete committed

        // Unrelated insert must succeed — the committed delete must not poison the context.
        ctx.Add(new TxOcc { Id = 2, Name = "second", Token = new byte[] { 0x0A } });
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task CallerOwned_update_commit_then_detectFalse_save_does_not_reissue_stale_update()
    {
        using var cn = OpenManualDb();
        RawInsertManual(cn, 1, "orig");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var e = new TxManual { Id = 1, Name = "orig" };
        ctx.Attach(e);
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            e.Name = "mine";
            ctx.Update(e);
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();   // e must be accepted (Unchanged) after commit
        }

        // A concurrent external writer advances the row.
        using (var s = cn.CreateCommand())
        {
            s.CommandText = "UPDATE TxManual SET Name = 'external-newer' WHERE Id = 1";
            s.ExecuteNonQuery();
        }

        // A later save that skips change detection must NOT re-issue the committed UPDATE.
        await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal("external-newer", NameOfManual(cn, 1));
    }
}
