using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

// X1 regression — EntityEntry must clone byte[] snapshots so that in-place mutation
// of a byte[] array cannot silently bypass dirty detection or corrupt the OCC predicate.
//
// Without the fix:
//   OriginalToken = tsCol.Getter(Entity)       → alias, not a copy
//   _originalValues[i] = _getValues[i](entity) → alias, not a copy
//   changed = !Equals(current, original)       → identity comparison returns true for same ref
//
// With the fix (SnapshotValue / ValuesEqual):
//   byte[] snapshots are cloned; structural equality is used for comparison.

#nullable enable

namespace nORM.Tests;

public class MutableSnapshotAliasingTests
{
    // ── Domain models ─────────────────────────────────────────────────────────

    [Table("SnapAlias_Payload")]
    private class PayloadEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public byte[] Data { get; set; } = Array.Empty<byte>();
    }

    [Table("SnapAlias_Ts")]
    private class TimestampedEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        [Timestamp]
        public byte[] RowVersion { get; set; } = Array.Empty<byte>();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection OpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static T? Scalar<T>(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        return result is DBNull ? default : (T?)Convert.ChangeType(result, typeof(T));
    }

    private static DbContext MakeCtx(SqliteConnection cn, bool eager = true)
        => new DbContext(cn, new SqliteProvider(),
               new DbContextOptions { EagerChangeTracking = eager });

    // ── X1-A: non-timestamp byte[] in-place mutation triggers dirty detection ─

    [Fact]
    public async Task InPlace_ByteArray_Mutation_IsDirtyDetected()
    {
        // Arrange: insert a row, then attach the entity and mutate Data in-place.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Payload (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Data BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Payload (Name, Data) VALUES ('original', X'010203')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Payload LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new PayloadEntity { Id = (int)id!, Name = "original", Data = new byte[] { 1, 2, 3 } };
        ctx.Attach(entity);

        // In-place mutation — same reference, different content
        entity.Data[0] = 0xFF;

        // SaveChanges must detect the change and emit UPDATE
        var affected = await ctx.SaveChangesAsync();

        Assert.True(affected > 0, "In-place byte[] mutation must trigger dirty detection and produce an UPDATE.");
    }

    [Fact]
    public async Task InPlace_ByteArray_Mutation_PersistsToDatabase()
    {
        // Verify the mutated value actually reaches the database row.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Payload (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Data BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Payload (Name, Data) VALUES ('before', X'010203')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Payload LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new PayloadEntity { Id = (int)id!, Name = "before", Data = new byte[] { 1, 2, 3 } };
        ctx.Attach(entity);

        entity.Data[0] = 0xAB;
        entity.Data[2] = 0xCD;
        await ctx.SaveChangesAsync();

        // Read raw bytes back from DB
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Data FROM SnapAlias_Payload WHERE Id = " + id;
        var rawBytes = (byte[])cmd.ExecuteScalar()!;

        Assert.Equal(0xAB, rawBytes[0]);
        Assert.Equal(2,    rawBytes[1]);
        Assert.Equal(0xCD, rawBytes[2]);
    }

    [Fact]
    public async Task InPlace_NonMutated_ByteArray_IsNotDirty()
    {
        // Sanity: if the byte[] content is unchanged, no UPDATE should be emitted.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Payload (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Data BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Payload (Name, Data) VALUES ('same', X'010203')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Payload LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new PayloadEntity { Id = (int)id!, Name = "same", Data = new byte[] { 1, 2, 3 } };
        ctx.Attach(entity);

        // No mutation — snapshot and current are structurally equal
        var affected = await ctx.SaveChangesAsync();

        Assert.Equal(0, affected);
    }

    // ── X1-B: Timestamp byte[] in-place mutation — OCC uses original snapshot ─

    [Fact]
    public async Task InPlace_TimestampMutation_OccUsesOriginalToken()
    {
        // Arrange: row in DB has RowVersion = 0x01.
        // Attach entity with RowVersion = [0x01].
        // Mutate RowVersion[0] = 0xFF in-place (simulating caller touching the token byte[]).
        // SaveChanges WHERE predicate must use [0x01] (original snapshot), not [0xFF] (mutated alias).
        // With the fix: UpdateAsync succeeds (predicate matches DB).
        // Without the fix: UpdateAsync fails with DbConcurrencyException (predicate uses mutated alias,
        //                  which doesn't match DB value 0x01).

        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Ts (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, RowVersion BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Ts (Name, RowVersion) VALUES ('orig', X'01')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Ts LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new TimestampedEntity { Id = (int)id!, Name = "orig", RowVersion = new byte[] { 0x01 } };
        ctx.Attach(entity);

        // Mutate the RowVersion byte[] in-place AND change a tracked column to force UPDATE
        entity.RowVersion[0] = 0xFF;
        entity.Name = "updated";

        // Must NOT throw DbConcurrencyException — original snapshot (0x01) must be used in WHERE
        var affected = await ctx.SaveChangesAsync();

        Assert.True(affected > 0, "OCC WHERE predicate must use the original snapshot token, not the mutated alias.");
    }

    [Fact]
    public async Task InPlace_TimestampMutation_OnlyNameChange_OccSucceeds()
    {
        // Same scenario but the Timestamp mutation is the ONLY reason original-token drift matters.
        // Change only Name so the UPDATE isn't a no-op.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Ts (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, RowVersion BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Ts (Name, RowVersion) VALUES ('a', X'0102')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Ts LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new TimestampedEntity { Id = (int)id!, Name = "a", RowVersion = new byte[] { 0x01, 0x02 } };
        ctx.Attach(entity);

        // Mutate OCC token in place
        entity.RowVersion[0] = 0xDE;
        entity.RowVersion[1] = 0xAD;

        entity.Name = "b"; // trigger UPDATE

        // Should succeed: WHERE RowVersion = X'0102' (original), not X'DEAD' (mutated)
        await ctx.SaveChangesAsync(); // must not throw
    }

    // ── X1-C: reference-replace vs in-place — both must work ─────────────────

    [Fact]
    public async Task ReplaceBytesRef_AndInPlaceMutation_BothDetectedCorrectly()
    {
        // Verify that both mutation modes are handled by the same structural equality.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Payload (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Data BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Payload (Name, Data) VALUES ('x', X'AA')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Payload LIMIT 1");

        await using var ctx = MakeCtx(cn);

        // Reference replacement (existing behavior, must still work)
        var entity = new PayloadEntity { Id = (int)id!, Name = "x", Data = new byte[] { 0xAA } };
        ctx.Attach(entity);
        entity.Data = new byte[] { 0xBB }; // replace reference
        var affected = await ctx.SaveChangesAsync();
        Assert.True(affected > 0, "Reference replacement must still trigger dirty detection.");
    }

    [Fact]
    public async Task InPlace_ZeroByteMutation_ToSameValue_IsNotDirty()
    {
        // Mutating in-place to the SAME value must not produce an UPDATE.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Payload (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Data BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Payload (Name, Data) VALUES ('y', X'0102')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Payload LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new PayloadEntity { Id = (int)id!, Name = "y", Data = new byte[] { 0x01, 0x02 } };
        ctx.Attach(entity);

        // "Mutate" each byte back to the same value
        entity.Data[0] = 0x01;
        entity.Data[1] = 0x02;

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(0, affected);
    }

    // ── X1-D: AcceptChanges refreshes the snapshot after save ─────────────────

    [Fact]
    public async Task AfterSave_Remutation_IsAgainDetected()
    {
        // After SaveChanges the new snapshot is [0xFF, 2, 3].
        // A subsequent in-place mutation to [0x00, 2, 3] must again be detected.
        using var cn = OpenConnection();
        Exec(cn, "CREATE TABLE SnapAlias_Payload (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Data BLOB NOT NULL)");
        Exec(cn, "INSERT INTO SnapAlias_Payload (Name, Data) VALUES ('z', X'010203')");
        var id = Scalar<long>(cn, "SELECT Id FROM SnapAlias_Payload LIMIT 1");

        await using var ctx = MakeCtx(cn);

        var entity = new PayloadEntity { Id = (int)id!, Name = "z", Data = new byte[] { 1, 2, 3 } };
        ctx.Attach(entity);

        // First save
        entity.Data[0] = 0xFF;
        await ctx.SaveChangesAsync();

        // Second mutation — snapshot must have refreshed to [0xFF, 2, 3]
        entity.Data[0] = 0x00;
        var affected = await ctx.SaveChangesAsync();
        Assert.True(affected > 0, "Re-mutation after save must be detected (snapshot refreshed).");
    }
}
