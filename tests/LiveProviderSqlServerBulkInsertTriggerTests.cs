using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// nORM's SQL Server bulk insert falls back to plain INSERTs for small batches (which fire AFTER INSERT
/// triggers) but uses SqlBulkCopy for larger ones. SqlBulkCopy suppresses triggers by default, so a
/// user-defined AFTER INSERT trigger (e.g. an audit trigger) ran for a 500-row insert but was silently
/// skipped for a 600-row one — a row-count-dependent loss of the trigger's side effects. Bulk insert must
/// fire triggers consistently regardless of batch size. Live-only: needs a real SQL Server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderSqlServerBulkInsertTriggerTests
{
    [Table("B4TrigMain")]
    private sealed class Main
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        return System.Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Large_bulk_insert_fires_after_insert_triggers()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SqlServer not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await ExecuteAsync(ctx, "IF OBJECT_ID('B4TrigAudit','U') IS NOT NULL DROP TABLE [B4TrigAudit];");
            await ExecuteAsync(ctx, "IF OBJECT_ID('B4TrigMain','U') IS NOT NULL DROP TABLE [B4TrigMain];");
            await ExecuteAsync(ctx, "CREATE TABLE [B4TrigMain] ([Id] INT PRIMARY KEY, [Name] NVARCHAR(100) NOT NULL)");
            await ExecuteAsync(ctx, "CREATE TABLE [B4TrigAudit] ([RowId] INT NOT NULL)");
            await ExecuteAsync(ctx, "CREATE TRIGGER [B4Trg] ON [B4TrigMain] AFTER INSERT AS INSERT INTO [B4TrigAudit] ([RowId]) SELECT [Id] FROM inserted;");
            try
            {
                // 600 rows > the 512 SqlBulkCopy threshold, so this takes the SqlBulkCopy path.
                var entities = Enumerable.Range(1, 600).Select(i => new Main { Id = i, Name = "n" + i }).ToArray();
                await ctx.BulkInsertAsync(entities);

                Assert.Equal(600L, await ScalarAsync(ctx, "SELECT COUNT(*) FROM [B4TrigMain]"));
                // BUG: 0 — SqlBulkCopy suppressed the audit trigger on the >512-row path.
                Assert.Equal(600L, await ScalarAsync(ctx, "SELECT COUNT(*) FROM [B4TrigAudit]"));
            }
            finally
            {
                await ExecuteAsync(ctx, "IF OBJECT_ID('B4TrigAudit','U') IS NOT NULL DROP TABLE [B4TrigAudit];");
                await ExecuteAsync(ctx, "IF OBJECT_ID('B4TrigMain','U') IS NOT NULL DROP TABLE [B4TrigMain];");
            }
        }
    }
}
