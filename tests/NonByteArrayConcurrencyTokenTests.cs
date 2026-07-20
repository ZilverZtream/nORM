using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A <c>[Timestamp]</c> / <c>IsRowVersion()</c> concurrency token of an auto-manageable non-byte[] type
/// (Guid, int, uint, long, ulong) must protect against lost updates on providers without a native
/// rowversion (SQLite/PostgreSQL/MySQL) exactly as a byte[] token does: nORM stamps a fresh token value on
/// every UPDATE and compares the ORIGINAL snapshot token in the WHERE clause, so a second writer that loaded
/// the row before a first writer committed is rejected with <see cref="DbConcurrencyException"/> rather than
/// silently overwriting the committed change. Previously only byte[] tokens were nORM-managed; other token
/// types were compared in WHERE but never mutated, so the stored value stayed constant and every stale
/// writer matched — a silent lost update.
/// </summary>
[Trait("Category", TestCategory.Fast)]
[Trait("Category", TestCategory.AdversarialConcurrency)]
public class NonByteArrayConcurrencyTokenTests
{
    [Table("OccLongTok")]
    public class LongAccount
    {
        [Key] public int Id { get; set; }
        public int Balance { get; set; }
        [Timestamp] public long Version { get; set; }
    }

    [Table("OccGuidTok")]
    public class GuidAccount
    {
        [Key] public int Id { get; set; }
        public int Balance { get; set; }
        [Timestamp] public Guid Version { get; set; }
    }

    private static (SqliteConnection keeper, string cs) SharedDb(string ddl)
    {
        var cs = $"Data Source=file:occtok_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (keeper, cs);
    }

    private static DbContext Open(string cs)
    {
        var cn = new SqliteConnection(cs);
        cn.Open();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
    }

    [Fact]
    public async Task Long_token_rejects_a_stale_second_writer()
    {
        var (keeper, cs) = SharedDb(
            "CREATE TABLE OccLongTok (Id INTEGER PRIMARY KEY, Balance INTEGER NOT NULL, Version INTEGER NOT NULL DEFAULT 0);" +
            "INSERT INTO OccLongTok (Id, Balance, Version) VALUES (1, 100, 1);");
        using var _keeper = keeper;
        using var ctx1 = Open(cs);
        using var ctx2 = Open(cs);

        var a1 = await ctx1.Query<LongAccount>().FirstAsync();
        var a2 = await ctx2.Query<LongAccount>().FirstAsync();

        a1.Balance = 150;
        await ctx1.SaveChangesAsync();          // first writer wins, bumps the token

        a2.Balance = 200;                         // second writer is stale
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx2.SaveChangesAsync());

        using var verify = Open(cs);
        Assert.Equal(150, (await verify.Query<LongAccount>().FirstAsync()).Balance);
    }

    [Fact]
    public async Task Guid_token_rejects_a_stale_second_writer()
    {
        var (keeper, cs) = SharedDb(
            "CREATE TABLE OccGuidTok (Id INTEGER PRIMARY KEY, Balance INTEGER NOT NULL, Version TEXT NOT NULL);" +
            "INSERT INTO OccGuidTok (Id, Balance, Version) VALUES (1, 100, '11111111-1111-1111-1111-111111111111');");
        using var _keeper = keeper;
        using var ctx1 = Open(cs);
        using var ctx2 = Open(cs);

        var a1 = await ctx1.Query<GuidAccount>().FirstAsync();
        var a2 = await ctx2.Query<GuidAccount>().FirstAsync();

        a1.Balance = 150;
        await ctx1.SaveChangesAsync();

        a2.Balance = 200;
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx2.SaveChangesAsync());

        using var verify = Open(cs);
        Assert.Equal(150, (await verify.Query<GuidAccount>().FirstAsync()).Balance);
    }

    [Fact]
    public async Task Long_token_non_conflicting_sequential_updates_succeed_and_advance_the_token()
    {
        var (keeper, cs) = SharedDb(
            "CREATE TABLE OccLongTok (Id INTEGER PRIMARY KEY, Balance INTEGER NOT NULL, Version INTEGER NOT NULL DEFAULT 0);" +
            "INSERT INTO OccLongTok (Id, Balance, Version) VALUES (1, 100, 1);");
        using var _keeper = keeper;
        using var ctx = Open(cs);

        var a = await ctx.Query<LongAccount>().FirstAsync();
        a.Balance = 150;
        await ctx.SaveChangesAsync();     // token advances; entry re-accepts the new token
        a.Balance = 175;
        await ctx.SaveChangesAsync();     // must succeed against the advanced token, no false conflict

        using var verify = Open(cs);
        Assert.Equal(175, (await verify.Query<LongAccount>().FirstAsync()).Balance);
    }
}
