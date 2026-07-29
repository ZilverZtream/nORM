using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Caller-owned-transaction baseline machinery under rollback, and cross-mapping mixed-state atomicity.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SaveChangesCallerOwnedTransactionRollbackTests
{
    [Table("AItem")]
    private class AItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("BItem")]
    private class BItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("OccC")]
    private class OccC
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static int Count(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static string? Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar() as string;
    }

    // ============================================================================
    // T7 — Caller-owned tx: INSERT A (save1), MODIFY A (save2 -> in-place UPDATE), ROLLBACK, re-save.
    // The re-insert must carry the FINAL (modified) value, not the original inserted value.
    // ============================================================================
    [Fact]
    public async Task CallerOwnedTx_insert_then_modify_then_rollback_reinserts_final_value()
    {
        using var cn = Open("CREATE TABLE AItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<AItem>() };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var a = new AItem { Name = "v1" };
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(a);
            await ctx.SaveChangesAsync();       // insert v1, stays Added
            a.Name = "v2";
            await ctx.SaveChangesAsync();       // in-place UPDATE to v2 (dirty-inserted path)
            Assert.Equal("v2", Scalar(cn, "SELECT Name FROM AItem WHERE Id = " + a.Id));
            await tx.RollbackAsync();
        }

        Assert.Equal(0, Count(cn, "AItem"));

        await ctx.SaveChangesAsync();           // re-insert the still-Added entity
        Assert.Equal(1, Count(cn, "AItem"));
        Assert.Equal("v2", Scalar(cn, "SELECT Name FROM AItem"));   // FINAL value, not v1
    }

    // ============================================================================
    // T9 — Caller-owned tx: MODIFY loaded A->B (save1), A->C (save2), ROLLBACK, re-save.
    // The pending final edit (C) must re-apply against the reverted (A) row.
    // ============================================================================
    [Fact]
    public async Task CallerOwnedTx_repeated_modify_then_rollback_resaves_final_value()
    {
        using var cn = Open("CREATE TABLE AItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
        using (var seed = cn.CreateCommand()) { seed.CommandText = "INSERT INTO AItem (Name) VALUES ('A')"; seed.ExecuteNonQuery(); }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<AItem>() };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var a = await ctx.Query<AItem>().FirstAsync();

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            a.Name = "B"; ctx.Update(a); await ctx.SaveChangesAsync();
            a.Name = "C"; ctx.Update(a); await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        Assert.Equal("A", Scalar(cn, "SELECT Name FROM AItem"));   // reverted in DB

        // a.Name is still "C" in memory; the baseline must have reverted to "A" so the edit re-applies.
        await ctx.SaveChangesAsync();
        Assert.Equal("C", Scalar(cn, "SELECT Name FROM AItem"));
    }

    // ============================================================================
    // T8 — Cross-mapping mixed-state atomicity: an INSERT into one table plus a failing OCC UPDATE
    // in another table must roll back BOTH (the insert must not survive).
    // ============================================================================
    [Fact]
    public async Task Insert_plus_failing_occ_update_across_tables_rolls_back_the_insert()
    {
        using var cn = Open(
            "CREATE TABLE BItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE OccC (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL);");
        using (var seed = cn.CreateCommand()) { seed.CommandText = "INSERT INTO OccC (Payload, Token) VALUES ('p', X'01')"; seed.ExecuteNonQuery(); }

        var opts = new DbContextOptions { OnModelCreating = mb => { mb.Entity<BItem>(); mb.Entity<OccC>(); } };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var occ = await ctx.Query<OccC>().FirstAsync();

        // Make the OCC token stale so its UPDATE matches 0 rows.
        using (var ext = cn.CreateCommand()) { ext.CommandText = "UPDATE OccC SET Token = X'02' WHERE Id = 1"; ext.ExecuteNonQuery(); }

        ctx.Add(new BItem { Id = 5, Name = "inserted" });   // Added
        occ.Payload = "p-new"; ctx.Update(occ);             // Modified (will OCC-fail)

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());

        Assert.Equal(0, Count(cn, "BItem"));   // the insert must have rolled back too
        Assert.Equal("p", Scalar(cn, "SELECT Payload FROM OccC WHERE Id = 1"));  // unchanged
    }

    // ============================================================================
    // T8b — Same cross-mapping mixed-state failure, then verify the tracker is consistent:
    // after fixing the stale OCC row, a re-save persists BOTH the insert and the update exactly once.
    // ============================================================================
    [Fact]
    public async Task After_cross_mapping_failure_tracker_resaves_both_exactly_once()
    {
        using var cn = Open(
            "CREATE TABLE BItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE OccC (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL);");
        using (var seed = cn.CreateCommand()) { seed.CommandText = "INSERT INTO OccC (Payload, Token) VALUES ('p', X'01')"; seed.ExecuteNonQuery(); }

        var opts = new DbContextOptions { OnModelCreating = mb => { mb.Entity<BItem>(); mb.Entity<OccC>(); } };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var occ = await ctx.Query<OccC>().FirstAsync();
        using (var ext = cn.CreateCommand()) { ext.CommandText = "UPDATE OccC SET Token = X'02' WHERE Id = 1"; ext.ExecuteNonQuery(); }

        ctx.Add(new BItem { Id = 5, Name = "inserted" });
        occ.Payload = "p-new"; ctx.Update(occ);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());

        // Resolve the OCC conflict by aligning the tracked original token with the DB value.
        occ.Token = new byte[] { 0x02 };
        ctx.Entry(occ).OriginalValues["Token"] = new byte[] { 0x02 };

        await ctx.SaveChangesAsync();

        Assert.Equal(1, Count(cn, "BItem"));   // insert applied exactly once
        Assert.Equal("p-new", Scalar(cn, "SELECT Payload FROM OccC WHERE Id = 1"));  // update applied
    }

    // ============================================================================
    // T10 — Caller-owned tx: INSERT A and B (save), then DELETE B and rollback, then re-save.
    // Exercises InsertedInUncommittedTransaction across multiple entities under one tx rollback.
    // ============================================================================
    [Fact]
    public async Task CallerOwnedTx_multi_insert_partial_progress_rollback_reinserts_all()
    {
        using var cn = Open("CREATE TABLE BItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)");
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<BItem>() };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var a = new BItem { Id = 1, Name = "a" };
        var b = new BItem { Id = 2, Name = "b" };

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(a);
            await ctx.SaveChangesAsync();          // a inserted, stays Added
            ctx.Add(b);
            await ctx.SaveChangesAsync();          // b inserted, stays Added; a must NOT be re-inserted
            Assert.Equal(2, Count(cn, "BItem"));
            await tx.RollbackAsync();
        }

        Assert.Equal(0, Count(cn, "BItem"));

        await ctx.SaveChangesAsync();              // both must re-insert
        Assert.Equal(2, Count(cn, "BItem"));
    }
}
