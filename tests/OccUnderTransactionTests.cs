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
/// Optimistic-concurrency ([Timestamp]) writes under an EXPLICIT caller-owned transaction, where nORM defers
/// AcceptChanges until the caller commits. A second update or a delete of the same tracked entity inside one
/// transaction must compare against the token the (uncommitted) row now carries — not the pre-update token —
/// so it must NOT throw a false concurrency conflict. A full rollback must restore the pre-transaction token so
/// a re-update of the same tracked entity after the rollback still matches the reverted row.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class OccUnderTransactionTests
{
    [Table("OccTx")]
    private class OccTx
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE OccTx (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void Seed(SqliteConnection cn, string payload)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO OccTx (Payload, Token) VALUES ('{payload}', X'01')";
        cmd.ExecuteNonQuery();
    }

    private static string? PayloadInDb(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Payload FROM OccTx WHERE Id = {id}";
        return cmd.ExecuteScalar() as string;
    }

    private static long RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM OccTx";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Sequential_updates_of_loaded_entity_within_one_transaction_all_apply()
    {
        using var cn = OpenDb();
        Seed(cn, "original");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var row = await ctx.Query<OccTx>().Where(r => r.Id == 1).FirstAsync();

        await using var tx = await ctx.Database.BeginTransactionAsync();
        row.Payload = "v1"; ctx.Update(row); await ctx.SaveChangesAsync();
        row.Payload = "v2"; ctx.Update(row); await ctx.SaveChangesAsync();   // must NOT false-conflict
        await tx.CommitAsync();

        Assert.Equal("v2", PayloadInDb(cn, 1));
    }

    [Fact]
    public async Task Update_then_delete_of_loaded_entity_within_one_transaction_succeeds()
    {
        using var cn = OpenDb();
        Seed(cn, "original");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var row = await ctx.Query<OccTx>().Where(r => r.Id == 1).FirstAsync();

        await using var tx = await ctx.Database.BeginTransactionAsync();
        row.Payload = "updated"; ctx.Update(row); await ctx.SaveChangesAsync();
        ctx.Remove(row); await ctx.SaveChangesAsync();   // delete must match the refreshed token
        await tx.CommitAsync();

        Assert.Equal(0, RowCount(cn));
    }

    [Fact]
    public async Task Insert_then_second_save_of_occ_entity_within_one_transaction_persists_both()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();
        ctx.Add(new OccTx { Payload = "a" }); await ctx.SaveChangesAsync();
        ctx.Add(new OccTx { Payload = "b" }); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(2, RowCount(cn));
    }

    [Fact]
    public async Task Update_rolled_back_then_re_updating_same_entity_still_matches_the_reverted_row()
    {
        using var cn = OpenDb();
        Seed(cn, "original");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var row = await ctx.Query<OccTx>().Where(r => r.Id == 1).FirstAsync();

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            row.Payload = "v1"; ctx.Update(row); await ctx.SaveChangesAsync();
            await tx.RollbackAsync();   // DB reverts to 'original'; the token snapshot must revert too
        }

        // Re-update the same tracked entity without reloading: its token must match the reverted row.
        row.Payload = "v2"; ctx.Update(row); await ctx.SaveChangesAsync();

        Assert.Equal("v2", PayloadInDb(cn, 1));
    }
}
