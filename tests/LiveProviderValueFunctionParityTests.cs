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
/// Live-provider parity for common provider-specific scalar translations:
/// string methods, Math methods, DateTime members/Add*, DateOnly parts, and
/// TimeOnly parts.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderValueFunctionParityTests
{
    private const string TableName = "ValueFuncLiveRow";

    [Table(TableName)]
    public sealed class ValueFuncLiveRow
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
        public double Number { get; set; }
        public DateTime Stamp { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly Clock { get; set; }
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({len})"
        : $"VARCHAR({len})";

    private static string DoubleCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "FLOAT",
        ProviderKind.Postgres => "DOUBLE PRECISION",
        ProviderKind.MySql => "DOUBLE",
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

    private static string DateCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "TEXT" : "DATE";

    private static string TimeCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "TIME(7)",
        ProviderKind.Postgres => "TIME",
        ProviderKind.MySql => "TIME(6)",
        ProviderKind.Sqlite => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(TableName);
        var id = ctx.Provider.Escape("Id");
        var text = ctx.Provider.Escape("Text");
        var number = ctx.Provider.Escape("Number");
        var stamp = ctx.Provider.Escape("Stamp");
        var day = ctx.Provider.Escape("Day");
        var clock = ctx.Provider.Escape("Clock");

        await ExecuteAsync(ctx, DropTable(kind, TableName, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {text} {VarCol(kind, 40)} NOT NULL, " +
            $"{number} {DoubleCol(kind)} NOT NULL, {stamp} {DateTimeCol(kind)} NOT NULL, " +
            $"{day} {DateCol(kind)} NOT NULL, {clock} {TimeCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{text},{number},{stamp},{day},{clock}) VALUES " +
            "(1,'alpha',9.0,'2026-05-27 10:15:30','2026-05-27','10:15:30')," +
            "(2,'bravo',3.6,'2026-01-02 03:04:05','2026-01-02','03:04:05')," +
            "(3,'charlie',-4.2,'2025-12-31 23:59:58','2025-12-31','23:59:58')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, TableName, ctx.Provider.Escape(TableName)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_math_and_temporal_value_functions_match_on_live_provider(ProviderKind kind)
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
                var upperIds = (await ctx.Query<ValueFuncLiveRow>()
                    .Where(r => r.Text.ToUpper() == "ALPHA")
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var stringIds = (await ctx.Query<ValueFuncLiveRow>()
                    .Where(r => r.Text.Contains("ar") || r.Text.StartsWith("ch") || r.Text.Substring(0, 1) == "b")
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var mathIds = (await ctx.Query<ValueFuncLiveRow>()
                    .Where(r => Math.Sqrt(r.Number) == 3.0 || Math.Abs(r.Number) > 4.0 || Math.Round(r.Number) == 4.0)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var dateTimeIds = (await ctx.Query<ValueFuncLiveRow>()
                    .Where(r => r.Stamp.Year == 2026 && r.Stamp.AddDays(1).Day >= 3)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var dateOnlyIds = (await ctx.Query<ValueFuncLiveRow>()
                    .Where(r => r.Day.Year == 2026 && r.Day.Month == 5 && r.Day.Day == 27)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var timeOnlyIds = (await ctx.Query<ValueFuncLiveRow>()
                    .Where(r => r.Clock.Hour == 23 || r.Clock.Minute == 15)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 1 }, upperIds);
                Assert.Equal(new[] { 2, 3 }, stringIds);
                Assert.Equal(new[] { 1, 2, 3 }, mathIds);
                Assert.Equal(new[] { 1, 2 }, dateTimeIds);
                Assert.Equal(new[] { 1 }, dateOnlyIds);
                Assert.Equal(new[] { 1, 3 }, timeOnlyIds);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }
}
