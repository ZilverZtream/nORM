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
/// Live-provider pin for the bulk-update decimal precision fix. The SQL Server
/// and MySQL bulk-update paths staged values into a temp table hardcoded to
/// DECIMAL(18,2), silently rounding any column with more than two decimal
/// places when it passed through BulkUpdateAsync. This test writes a
/// high-precision rate, bulk-updates it to another high-precision value, and
/// asserts the stored value is EXACT — a failing-first pin until the staging
/// type honours the column's real precision/scale.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class BulkUpdateDecimalPrecisionParityTests
{
    private const string Table = "BudpRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task BulkUpdate_preserves_high_precision_decimals(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                var e = mb.Entity<BudpRow>();
                e.Property(x => x.Rate).HasPrecision(28, 10);
                e.Property(x => x.Amount).HasPrecision(28, 10);
            }
        };
        await using (connection)
        using (var ctx = new DbContext(connection, provider, opts))
        {
            await SetupAsync(ctx, kind);
            try
            {
                var rows = new[]
                {
                    new BudpRow { Id = 1, Rate = 0.00125m,   Amount = 19.999m },
                    new BudpRow { Id = 2, Rate = 0.123456m,  Amount = 1234.5678m },
                };
                await ctx.BulkInsertAsync(rows);

                // Mutate to new high-precision values and bulk-update.
                rows[0].Rate = 0.00875m;   rows[0].Amount = 0.001m;
                rows[1].Rate = 0.999999m;  rows[1].Amount = 9999.9999m;
                await ctx.BulkUpdateAsync(rows);

                var read = (await ctx.Query<BudpRow>().OrderBy(r => r.Id).ToListAsync())
                    .ToArray();

                Assert.Equal(0.00875m, read[0].Rate);
                Assert.Equal(0.001m, read[0].Amount);
                Assert.Equal(0.999999m, read[1].Rate);
                Assert.Equal(9999.9999m, read[1].Amount);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(Table);
        var id = ctx.Provider.Escape(nameof(BudpRow.Id));
        var rate = ctx.Provider.Escape(nameof(BudpRow.Rate));
        var amount = ctx.Provider.Escape(nameof(BudpRow.Amount));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        // Wide precision/scale so the storage column itself never rounds — the
        // test isolates the staging-table type, not the destination column.
        var decType = kind == ProviderKind.Sqlite ? "TEXT" : "DECIMAL(28,10)";

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {rate} {decType} NOT NULL, {amount} {decType} NOT NULL)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            var table = ctx.Provider.Escape(Table);
            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table}"
                : $"DROP TABLE IF EXISTS {table}";
            await ExecuteAsync(ctx, drop);
        }
        catch
        {
            // best-effort cleanup only
        }
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(Table)]
    public sealed class BudpRow
    {
        [Key] public int Id { get; set; }
        public decimal Rate { get; set; }
        public decimal Amount { get; set; }
    }
}
