using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for raw SQL APIs. These tests prove the constrained
/// raw SQL contract against real engines: provider parameters, interpolated
/// parameterization, and name-based materialization with reordered columns.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderRawSqlParityTests
{
    private const string TableName = "RawSqlLiveItem";

    [Table(TableName)]
    public sealed class RawSqlLiveItem
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({len})"
        : $"VARCHAR({len})";

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
        var code = ctx.Provider.Escape("Code");
        var name = ctx.Provider.Escape("Name");
        var score = ctx.Provider.Escape("Score");

        await ExecuteAsync(ctx, DropTable(kind, TableName, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {code} {VarCol(kind, 20)} NOT NULL, " +
            $"{name} {VarCol(kind, 40)} NOT NULL, {score} {IntCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{code},{name},{score}) VALUES " +
            "(1,'A1','Alpha',10)," +
            "(2,'B2','Beta',20)," +
            "(3,'C3','Gamma',30)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, TableName, ctx.Provider.Escape(TableName)));
        }
        catch
        {
            // Best-effort cleanup; the test body reports operational failures.
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task FromSqlRaw_materializes_reordered_columns_with_provider_parameters_on_live_provider(ProviderKind kind)
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
                var table = ctx.Provider.Escape(TableName);
                var id = ctx.Provider.Escape("Id");
                var code = ctx.Provider.Escape("Code");
                var name = ctx.Provider.Escape("Name");
                var score = ctx.Provider.Escape("Score");
                var p0 = ctx.Provider.ParamPrefix + "p0";

                var rows = await ctx.FromSqlRawAsync<RawSqlLiveItem>(
                    $"SELECT {name}, {score}, {code}, {id} FROM {table} WHERE {score} >= {p0} ORDER BY {id}",
                    default,
                    20);

                Assert.Equal(new[] { 2, 3 }, rows.Select(r => r.Id).ToArray());
                Assert.Equal(new[] { "B2", "C3" }, rows.Select(r => r.Code).ToArray());
                Assert.Equal(new[] { "Beta", "Gamma" }, rows.Select(r => r.Name).ToArray());
                Assert.Equal(new[] { 20, 30 }, rows.Select(r => r.Score).ToArray());
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task FromSqlInterpolated_parameterizes_values_on_live_provider(ProviderKind kind)
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
                var table = ctx.Provider.Escape(TableName);
                var id = ctx.Provider.Escape("Id");
                var code = ctx.Provider.Escape("Code");
                var name = ctx.Provider.Escape("Name");
                var score = ctx.Provider.Escape("Score");

                var sql = FormattableStringFactory.Create(
                    $"SELECT {score}, {name}, {id}, {code} FROM {table} WHERE {code} = {{0}} ORDER BY {id}",
                    "B2");
                var rows = await ctx.FromSqlInterpolatedAsync<RawSqlLiveItem>(sql);

                Assert.Single(rows);
                Assert.Equal(2, rows[0].Id);
                Assert.Equal("B2", rows[0].Code);
                Assert.Equal("Beta", rows[0].Name);
                Assert.Equal(20, rows[0].Score);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task QueryUnchangedInterpolated_parameterizes_values_on_live_provider(ProviderKind kind)
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
                var table = ctx.Provider.Escape(TableName);
                var id = ctx.Provider.Escape("Id");
                var code = ctx.Provider.Escape("Code");
                var name = ctx.Provider.Escape("Name");
                var score = ctx.Provider.Escape("Score");

                var sql = FormattableStringFactory.Create(
                    $"SELECT {id}, {code}, {name}, {score} FROM {table} WHERE {name} = {{0}}",
                    "x' OR 1=1 --");
                var rows = await ctx.QueryUnchangedInterpolatedAsync<RawSqlLiveItem>(sql);

                Assert.Empty(rows);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }
}
