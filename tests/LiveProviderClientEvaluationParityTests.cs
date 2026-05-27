using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for the constrained client-evaluation projection tail.
/// The policy must reject by default, and Warn/Allow may only run after the
/// server-side filter/order/page shape has already been applied.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderClientEvaluationParityTests
{
    private const string TableName = "ClientEvalLiveUser";

    [Table(TableName)]
    public sealed class ClientEvalLiveUser
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
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
        var name = ctx.Provider.Escape("Name");

        await ExecuteAsync(ctx, DropTable(kind, TableName, table));
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 40)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name}) VALUES " +
            "(1,'ada')," +
            "(2,'grace')," +
            "(3,'linus')");
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
    public async Task Client_evaluation_policy_matches_contract_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var setup = new DbContext(connection, provider, null, ownsConnection: false))
        {
            await SetupAsync(setup, kind);
            try
            {
                using (var strict = new DbContext(connection, provider, null, ownsConnection: false))
                {
                    var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
                        strict.Query<ClientEvalLiveUser>()
                            .Select(u => new ClientEvalLiveUser { Id = u.Id, Name = FormatName(u.Name) })
                            .ToListAsync());

                    Assert.Contains("requires client-side evaluation", ex.Message, StringComparison.Ordinal);
                }

                var warnLogger = new CapturingLogger();
                using (var warn = new DbContext(connection, provider, new DbContextOptions
                {
                    ClientEvaluationPolicy = ClientEvaluationPolicy.Warn,
                    Logger = warnLogger
                }, ownsConnection: false))
                {
                    var secret = "sensitive-live";
                    var rows = await warn.Query<ClientEvalLiveUser>()
                        .Where(u => u.Id >= 2)
                        .OrderBy(u => u.Id)
                        .Take(1)
                        .Select(u => new ClientEvalLiveUser { Id = u.Id, Name = FormatName(u.Name) + secret })
                        .ToListAsync();

                    Assert.Single(rows);
                    Assert.Equal(2, rows[0].Id);
                    Assert.Equal("GRACEsensitive-live", rows[0].Name);

                    var warning = Assert.Single(warnLogger.Messages.Where(m => m.Contains("-- CLIENT-EVAL", StringComparison.Ordinal)));
                    Assert.DoesNotContain(secret, warning, StringComparison.Ordinal);
                }

                var allowLogger = new CapturingLogger();
                using (var allow = new DbContext(connection, provider, new DbContextOptions
                {
                    ClientEvaluationPolicy = ClientEvaluationPolicy.Allow,
                    Logger = allowLogger
                }, ownsConnection: false))
                {
                    var rows = await allow.Query<ClientEvalLiveUser>()
                        .Where(u => u.Id >= 2)
                        .OrderBy(u => u.Id)
                        .Take(1)
                        .Select(u => new ClientEvalLiveUser { Id = u.Id, Name = FormatName(u.Name) })
                        .ToListAsync();

                    Assert.Single(rows);
                    Assert.Equal(2, rows[0].Id);
                    Assert.Equal("GRACE", rows[0].Name);
                    Assert.DoesNotContain(allowLogger.Messages, m => m.Contains("-- CLIENT-EVAL", StringComparison.Ordinal));
                }
            }
            finally
            {
                await TeardownAsync(setup, kind);
            }
        }
    }

    private static string FormatName(string value) => value.ToUpperInvariant();

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Messages { get; } = new();

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Messages.Add(formatter(state, exception));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose()
            {
            }
        }
    }
}
