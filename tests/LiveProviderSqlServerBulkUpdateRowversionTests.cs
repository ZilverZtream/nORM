using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SQL Server's native bulk update stages rows into a temp table and joins the target on the key AND the
/// rowversion (OCC). A DB-generated rowversion — [Timestamp] + [DatabaseGenerated(Computed)], which the
/// scaffolder emits on every ROWVERSION column — was excluded from staging (only keys were exempted from
/// the "skip DB-generated" filter), so the staged token was NULL and `T1.RowVersion = T2.RowVersion`
/// matched zero rows: every update was silently discarded with no exception (lost update).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderSqlServerBulkUpdateRowversionTests
{
    [Table("BulkOccAccount")]
    private sealed class Account
    {
        [Key] public int Id { get; set; }
        public int Balance { get; set; }
        [Timestamp, DatabaseGenerated(DatabaseGeneratedOption.Computed)] public byte[] RowVersion { get; set; } = Array.Empty<byte>();
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Native_bulk_update_of_rowversioned_rows_updates_all_rows()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SqlServer not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await ExecuteAsync(ctx, "IF OBJECT_ID(N'BulkOccAccount', N'U') IS NOT NULL DROP TABLE [BulkOccAccount];");
            await ExecuteAsync(ctx, "CREATE TABLE [BulkOccAccount] ([Id] INT PRIMARY KEY, [Balance] INT NOT NULL, [RowVersion] ROWVERSION)");
            await ExecuteAsync(ctx, "INSERT INTO [BulkOccAccount] ([Id],[Balance]) VALUES (1,100),(2,200)");
            try
            {
                var accounts = await ctx.Query<Account>().OrderBy(a => a.Id).ToListAsync();
                foreach (var a in accounts) a.Balance += 50;

                var n = await ctx.BulkUpdateAsync(accounts);
                Assert.Equal(2, n);   // BUG: 0 — the staged NULL rowversion joined nothing

                var after = (await ctx.Query<Account>().OrderBy(a => a.Id).ToListAsync())
                    .Select(a => a.Balance).ToArray();
                Assert.Equal(new[] { 150, 250 }, after);
            }
            finally
            {
                await ExecuteAsync(ctx, "IF OBJECT_ID(N'BulkOccAccount', N'U') IS NOT NULL DROP TABLE [BulkOccAccount];");
            }
        }
    }
}
