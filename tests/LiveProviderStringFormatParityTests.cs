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
/// Live-provider parity for the documented string.Format/interpolation subset:
/// constant templates, fixed decimal specs, and common date tokens.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderStringFormatParityTests
{
    private const string TableName = "StringFormatLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_format_and_interpolation_match_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAsync(ctx, kind);
            try
            {
                var rows = (await ctx.Query<StringFormatLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new
                    {
                        r.Id,
                        Pair = string.Format("{0}:{1}", r.Name, r.Score),
                        Money = string.Format("{0:F2}", r.Amount),
                        Day = string.Format("{0:yyyy-MM-dd}", r.Stamp),
                        Interpolated = $"{r.Name}-{r.Score}"
                    })
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(2, rows.Length);
                Assert.Equal("alpha:7", rows[0].Pair);
                Assert.Equal("12.50", rows[0].Money);
                Assert.Equal("2026-05-27", rows[0].Day);
                Assert.Equal("alpha-7", rows[0].Interpolated);
                Assert.Equal("beta:42", rows[1].Pair);
                Assert.Equal("3.25", rows[1].Money);
                Assert.Equal("2026-05-28", rows[1].Day);
                Assert.Equal("beta-42", rows[1].Interpolated);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(TableName);
        var id = ctx.Provider.Escape(nameof(StringFormatLiveRow.Id));
        var name = ctx.Provider.Escape(nameof(StringFormatLiveRow.Name));
        var score = ctx.Provider.Escape(nameof(StringFormatLiveRow.Score));
        var amount = ctx.Provider.Escape(nameof(StringFormatLiveRow.Amount));
        var stamp = ctx.Provider.Escape(nameof(StringFormatLiveRow.Stamp));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {name} {textType} NOT NULL, " +
            $"{score} {intType} NOT NULL, {amount} {DecimalCol(kind)} NOT NULL, {stamp} {DateTimeCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name},{score},{amount},{stamp}) VALUES " +
            "(1,'alpha',7,12.50,'2026-05-27 10:15:30')," +
            "(2,'beta',42,3.25,'2026-05-28 11:00:00')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, ctx.Provider.Escape(TableName)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static string DropTable(ProviderKind kind, string escapedTable)
        => kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{TableName}', N'U') IS NOT NULL DROP TABLE {escapedTable}"
            : $"DROP TABLE IF EXISTS {escapedTable}";

    private static string DecimalCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "DECIMAL(18,2)",
        ProviderKind.Postgres => "NUMERIC(18,2)",
        ProviderKind.MySql => "DECIMAL(18,2)",
        ProviderKind.Sqlite => "REAL",
        _ => throw new NotSupportedException()
    };

    private static string DateTimeCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "DATETIME2",
        ProviderKind.Postgres => "TIMESTAMP",
        ProviderKind.MySql => "DATETIME(6)",
        ProviderKind.Sqlite => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(TableName)]
    private sealed class StringFormatLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
        public decimal Amount { get; set; }
        public DateTime Stamp { get; set; }
    }
}
