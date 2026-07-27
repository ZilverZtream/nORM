using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// MySQL's native bulk update stages rows into a temp table and joins the target on the key AND the
/// concurrency token (OCC). MySQL has no native rowversion, so the idiomatic token is a DB-generated
/// <c>DateTime</c> backed by <c>TIMESTAMP(6) ... ON UPDATE CURRENT_TIMESTAMP(6)</c>. Because a DateTime token
/// is not an auto-manageable token type, the update does NOT route through the row-by-row path; it takes the
/// temp-table path — which excluded the DB-generated token from staging (only keys were exempted from the
/// "skip DB-generated" filter), so the staged token was NULL and <c>T1.Version = T2.Version</c> matched zero
/// rows: every update was silently discarded with no exception (lost update). SQL Server and PostgreSQL
/// already stage the token; MySQL was the missing sibling.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderMySqlBulkUpdateTimestampTokenTests
{
    [Table("BulkOccTsAccount")]
    private sealed class Account
    {
        [Key] public int Id { get; set; }
        public int Balance { get; set; }
        public DateTime Version { get; set; }
    }

    private static DbContextOptions ModelOptions() => new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Account>().HasKey(a => a.Id);
            mb.Entity<Account>().Property(a => a.Version).IsRowVersion();
            mb.Entity<Account>().Property(a => a.Version).ValueGeneratedOnAddOrUpdate();
        }
    };

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Native_bulk_update_of_datetime_token_rows_updates_all_rows()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySql not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider, ModelOptions()))
        {
            await ExecuteAsync(ctx, "DROP TABLE IF EXISTS `BulkOccTsAccount`");
            await ExecuteAsync(ctx,
                "CREATE TABLE `BulkOccTsAccount` (" +
                "`Id` INT PRIMARY KEY, `Balance` INT NOT NULL, " +
                "`Version` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6))");
            await ExecuteAsync(ctx, "INSERT INTO `BulkOccTsAccount` (`Id`,`Balance`) VALUES (1,100),(2,200)");
            try
            {
                var accounts = await ctx.Query<Account>().OrderBy(a => a.Id).ToListAsync();
                foreach (var a in accounts) a.Balance += 50;

                var n = await ctx.BulkUpdateAsync(accounts);
                Assert.Equal(2, n);   // BUG: 0 — the un-staged (NULL) token joined nothing

                var after = (await ctx.Query<Account>().OrderBy(a => a.Id).ToListAsync())
                    .Select(a => a.Balance).ToArray();
                Assert.Equal(new[] { 150, 250 }, after);
            }
            finally
            {
                await ExecuteAsync(ctx, "DROP TABLE IF EXISTS `BulkOccTsAccount`");
            }
        }
    }
}
