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
/// The classic lost-update scenario: two contexts load the same row, both modify a field, both save.
/// With a [Timestamp] concurrency token, the second save must fail (DbConcurrencyException) rather
/// than silently overwrite the first writer's change. Verifies whether nORM's token actually changes
/// between updates on a provider without a native rowversion (SQLite).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class OptimisticConcurrencyLostUpdateTests
{
    [Table("OccRow")]
    private class OccRow
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
        cmd.CommandText = "CREATE TABLE OccRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static string PayloadInDb(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Payload FROM OccRow WHERE Id = {id}";
        return (string)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Second_concurrent_update_must_not_silently_overwrite()
    {
        using var cn = OpenDb();

        // Seed via raw SQL so no DbContext disposal closes the in-memory connection mid-test.
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText = "INSERT INTO OccRow (Payload, Token) VALUES ('original', X'01'); SELECT last_insert_rowid();";
            var id0 = Convert.ToInt32(seed.ExecuteScalar());
            Assert.True(id0 > 0);
        }
        int id = 1;

        // Two independent contexts each load the row (both see the same original token).
        using var ctxA = new DbContext(cn, new SqliteProvider());
        using var ctxB = new DbContext(cn, new SqliteProvider());

        var a = await ctxA.Query<OccRow>().Where(r => r.Id == id).FirstAsync();
        var b = await ctxB.Query<OccRow>().Where(r => r.Id == id).FirstAsync();

        // Writer A commits first.
        a.Payload = "written-by-A";
        ctxA.Update(a);
        await ctxA.SaveChangesAsync();

        // Writer B has a now-stale snapshot. Saving must NOT silently clobber A's write.
        b.Payload = "written-by-B";
        ctxB.Update(b);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctxB.SaveChangesAsync());

        // A's write survives.
        Assert.Equal("written-by-A", PayloadInDb(cn, id));
    }

    [Fact]
    public async Task Sequential_updates_refresh_the_token_and_all_succeed()
    {
        using var cn = OpenDb();
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText = "INSERT INTO OccRow (Payload, Token) VALUES ('original', X'01')";
            seed.ExecuteNonQuery();
        }
        int id = 1;

        using var ctx = new DbContext(cn, new SqliteProvider());
        var row = await ctx.Query<OccRow>().Where(r => r.Id == id).FirstAsync();

        // Each update stamps a new token; the snapshot must refresh so the next update's WHERE
        // matches. If it doesn't, the second update's stale WHERE affects 0 rows and throws.
        for (int i = 1; i <= 3; i++)
        {
            row.Payload = "v" + i;
            ctx.Update(row);
            await ctx.SaveChangesAsync();
        }

        Assert.Equal("v3", PayloadInDb(cn, id));
    }

    [Fact]
    public async Task Update_then_delete_same_context_succeeds()
    {
        using var cn = OpenDb();
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText = "INSERT INTO OccRow (Payload, Token) VALUES ('original', X'01')";
            seed.ExecuteNonQuery();
        }
        int id = 1;

        using var ctx = new DbContext(cn, new SqliteProvider());
        var row = await ctx.Query<OccRow>().Where(r => r.Id == id).FirstAsync();

        row.Payload = "updated";
        ctx.Update(row);
        await ctx.SaveChangesAsync();   // token stamped + snapshot refreshed

        ctx.Remove(row);
        await ctx.SaveChangesAsync();   // delete must match the refreshed token, not the original

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM OccRow";
        Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public async Task Direct_UpdateAsync_second_stale_write_must_not_silently_overwrite()
    {
        using var cn = OpenDb();
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText = "INSERT INTO OccRow (Payload, Token) VALUES ('original', X'01')";
            seed.ExecuteNonQuery();
        }
        int id = 1;

        using var ctxA = new DbContext(cn, new SqliteProvider());
        using var ctxB = new DbContext(cn, new SqliteProvider());

        var a = await ctxA.Query<OccRow>().Where(r => r.Id == id).FirstAsync();
        var b = await ctxB.Query<OccRow>().Where(r => r.Id == id).FirstAsync();

        a.Payload = "written-by-A";
        await ctxA.UpdateAsync(a);

        b.Payload = "written-by-B";
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctxB.UpdateAsync(b));

        Assert.Equal("written-by-A", PayloadInDb(cn, id));
    }

    [Fact]
    public async Task Bulk_UpdateAsync_second_stale_write_must_not_silently_overwrite()
    {
        using var cn = OpenDb();
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText = "INSERT INTO OccRow (Payload, Token) VALUES ('original', X'01')";
            seed.ExecuteNonQuery();
        }
        int id = 1;

        using var ctxA = new DbContext(cn, new SqliteProvider());
        using var ctxB = new DbContext(cn, new SqliteProvider());

        var a = await ctxA.Query<OccRow>().Where(r => r.Id == id).FirstAsync();
        var b = await ctxB.Query<OccRow>().Where(r => r.Id == id).FirstAsync();

        a.Payload = "written-by-A";
        await ctxA.BulkUpdateAsync(new[] { a });

        // Bulk uses partial-OCC semantics: the stale row is silently skipped (0 rows affected) rather
        // than throwing — but crucially it must NOT overwrite A's newer write.
        b.Payload = "written-by-B";
        var affected = await ctxB.BulkUpdateAsync(new[] { b });

        Assert.Equal(0, affected);
        Assert.Equal("written-by-A", PayloadInDb(cn, id));
    }
}
