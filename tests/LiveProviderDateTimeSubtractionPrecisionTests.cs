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
/// Live-provider proof that DateTime subtraction preserves sub-second deltas.
/// The LINQ support matrix documents fractional Total* semantics; integer
/// second provider hooks silently truncate common duration calculations.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderDateTimeSubtractionPrecisionTests
{
    private const string Table = "DateTimeSubPrecisionRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTime_subtraction_preserves_sub_second_deltas_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<DateTimeSubPrecisionRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Diff = r.End - r.Start, Seconds = (r.End - r.Start).TotalSeconds })
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(2, rows.Length);
                Assert.InRange((rows[0].Diff - TimeSpan.FromSeconds(1.5)).TotalMilliseconds, -1.0, 1.0);
                Assert.InRange(rows[0].Seconds - 1.5, -0.001, 0.001);
                Assert.InRange((rows[1].Diff - TimeSpan.FromMilliseconds(-250)).TotalMilliseconds, -1.0, 1.0);
                Assert.InRange(rows[1].Seconds - -0.25, -0.001, 0.001);
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
        var id = ctx.Provider.Escape(nameof(DateTimeSubPrecisionRow.Id));
        var start = ctx.Provider.Escape(nameof(DateTimeSubPrecisionRow.Start));
        var end = ctx.Provider.Escape(nameof(DateTimeSubPrecisionRow.End));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var dateTimeType = kind switch
        {
            ProviderKind.SqlServer => "DATETIME2(7)",
            ProviderKind.Postgres => "TIMESTAMP(6)",
            ProviderKind.MySql => "DATETIME(6)",
            ProviderKind.Sqlite => "TEXT",
            _ => "TIMESTAMP"
        };

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {start} {dateTimeType} NOT NULL, {end} {dateTimeType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{start},{end}) VALUES " +
            "(1,'2026-05-25 12:00:00.123456','2026-05-25 12:00:01.623456')," +
            "(2,'2026-05-25 12:00:00.500000','2026-05-25 12:00:00.250000')");
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
    private sealed class DateTimeSubPrecisionRow
    {
        [Key] public int Id { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }
}
