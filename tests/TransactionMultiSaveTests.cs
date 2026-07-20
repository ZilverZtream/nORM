using System.Collections.Generic;
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
/// A second SaveChanges inside one caller-owned transaction must not re-insert the rows an earlier
/// SaveChanges already wrote — the common "save parent, save child, commit" pattern. Under a caller-owned
/// transaction nORM keeps the inserted entities Added (so a rollback can re-insert them), which for a
/// client-assigned key used to cause the next save to re-run the INSERT and fail with a UNIQUE violation.
/// These tests pin the fix AND its mirror image: after any rollback (full, savepoint, ambient) the entities
/// whose rows were discarded must be re-inserted by the next save, never silently skipped.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TransactionMultiSaveTests
{
    [Table("ClientKeyItem")]
    private class ClientKeyItem
    {
        [Key] public int Id { get; set; }   // client-assigned, NOT database-generated
        public int Value { get; set; }
    }

    [Table("GenKeyItem")]
    private class GenKeyItem
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE ClientKeyItem (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);" +
            "CREATE TABLE GenKeyItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // Authoritative row count read straight from the database, bypassing the change tracker.
    private static int RawCount(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (int)(long)cmd.ExecuteScalar()!;
    }

    private static List<int> RawIds(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Id FROM {table} ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var ids = new List<int>();
        while (r.Read()) ids.Add(r.GetInt32(0));
        return ids;
    }

    [Fact]
    public async Task SecondSaveInOneTransaction_ClientKey_DoesNotReinsertAndPersistsAll()
    {
        var (cn, ctx) = Build();
        using var _ = cn;
        await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new ClientKeyItem { Id = 1, Value = 10 });
        await ctx.SaveChangesAsync();                        // flush #1
        ctx.Add(new ClientKeyItem { Id = 2, Value = 20 });
        await ctx.SaveChangesAsync();                        // flush #2 must NOT re-insert Id=1
        await tx.CommitAsync();

        Assert.Equal(new[] { 1, 2 }, RawIds(cn, "ClientKeyItem"));
    }

    [Fact]
    public async Task SecondSaveInOneTransaction_DbGeneratedKey_DoesNotReinsertAndPersistsAll()
    {
        var (cn, ctx) = Build();
        using var _ = cn;
        await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new GenKeyItem { Value = 10 });
        await ctx.SaveChangesAsync();
        ctx.Add(new GenKeyItem { Value = 20 });
        await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(2, RawCount(cn, "GenKeyItem"));
    }

    [Fact]
    public async Task CommitThenSaveOutsideTransaction_ClientKey_NoDuplicateAndBothPersist()
    {
        var (cn, ctx) = Build();
        using var _ = cn;
        await using var __ = ctx;

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new ClientKeyItem { Id = 1, Value = 10 });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();                          // entity 1 stays Added but is committed
        }

        // Outside any transaction: the still-Added entity 1 must not be re-inserted; entity 2 is new.
        ctx.Add(new ClientKeyItem { Id = 2, Value = 20 });
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { 1, 2 }, RawIds(cn, "ClientKeyItem"));
    }

    [Fact]
    public async Task FullRollbackThenSave_ClientKey_ReinsertsRatherThanSilentlyDropping()
    {
        var (cn, ctx) = Build();
        using var _ = cn;
        await using var __ = ctx;

        var item = new ClientKeyItem { Id = 1, Value = 10 };
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(item);
            await ctx.SaveChangesAsync();                    // inserted (uncommitted), flag set
            await tx.RollbackAsync();                        // row discarded; flag must be cleared
        }
        Assert.Empty(RawIds(cn, "ClientKeyItem"));

        // The still-Added entity must be RE-INSERTED by the next save, not skipped as "already inserted".
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { 1 }, RawIds(cn, "ClientKeyItem"));
    }

    [Fact]
    public async Task RollbackToSavepoint_ClientKey_DiscardsPostSavepointRowKeepsPriorRow()
    {
        var (cn, ctx) = Build();
        using var _ = cn;
        await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new ClientKeyItem { Id = 1, Value = 10 });
        await ctx.SaveChangesAsync();                        // before savepoint
        await tx.CreateSavepointAsync("sp");
        ctx.Add(new ClientKeyItem { Id = 2, Value = 20 });
        await ctx.SaveChangesAsync();                        // after savepoint
        await tx.RollbackToSavepointAsync("sp");             // discards row 2, keeps row 1
        await tx.CommitAsync();

        Assert.Equal(new[] { 1 }, RawIds(cn, "ClientKeyItem"));
    }

    [Fact]
    public async Task RollbackToSavepointThenSave_ClientKey_ReinsertsPostSavepointRow()
    {
        var (cn, ctx) = Build();
        using var _ = cn;
        await using var __ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new ClientKeyItem { Id = 1, Value = 10 });
        await ctx.SaveChangesAsync();
        await tx.CreateSavepointAsync("sp");
        ctx.Add(new ClientKeyItem { Id = 2, Value = 20 });
        await ctx.SaveChangesAsync();
        await tx.RollbackToSavepointAsync("sp");             // row 2 gone, its flag must be cleared
        await ctx.SaveChangesAsync();                        // must RE-INSERT entity 2, not skip it
        await tx.CommitAsync();

        Assert.Equal(new[] { 1, 2 }, RawIds(cn, "ClientKeyItem"));
    }
}
