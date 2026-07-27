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
/// SQLite's native bulk update regenerates a client-managed concurrency token server-side
/// (<c>Version = randomblob(8)</c>) but the token is client-managed, so nORM must also advance the token on
/// the in-memory entity — otherwise a second bulk update of the SAME instances stages the now-stale token,
/// the OCC match finds no row, and the update is silently discarded (a lost update on a query that
/// "succeeds"). MySQL and PostgreSQL route client-managed tokens through the row-by-row path, which advances
/// the entity token; SQLite must stay consistent.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SqliteBulkUpdateTokenRefreshTests
{
    [Table("SbtAccount")]
    private sealed class Account
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Balance { get; set; }
        [Timestamp] public byte[] Version { get; set; } = Array.Empty<byte>();
    }

    private static DbContext Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SbtAccount (Id INTEGER PRIMARY KEY AUTOINCREMENT, Balance INTEGER NOT NULL, Version BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Repeated_bulk_updates_of_same_instances_each_apply()
    {
        await using var ctx = Create();

        var acct = new Account { Balance = 100 };
        ctx.Add(acct);
        await ctx.SaveChangesAsync(); // stamps the client-managed Version token

        acct.Balance = 150;
        var n1 = await ctx.BulkUpdateAsync(new[] { acct });
        Assert.Equal(1, n1);

        // Second update on the SAME instance: its token must have advanced to match the DB, so this applies.
        acct.Balance = 200;
        var n2 = await ctx.BulkUpdateAsync(new[] { acct });
        Assert.Equal(1, n2); // BUG: 0 — stale in-memory token no longer matches the regenerated DB token

        var final = await ctx.Query<Account>().FirstAsync(a => a.Id == acct.Id);
        Assert.Equal(200, final.Balance);
    }
}
