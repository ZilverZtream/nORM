using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Enterprise;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public sealed class LoggingRedactionTests
{
    [Fact]
    public void LogQuery_RedactsSqlLiteralsAndParameterValues()
    {
        var logger = new CapturingLogger();
        var parameters = new Dictionary<string, object>
        {
            ["@p0"] = "secret-password",
            ["@p1"] = 42
        };

        logger.LogQuery(
            "SELECT * FROM Users WHERE Name = 'secret-password' AND Age > @p1",
            parameters,
            TimeSpan.FromMilliseconds(1),
            1);

        var entry = Assert.Single(logger.Entries);
        Assert.DoesNotContain("secret-password", entry.Rendered, StringComparison.Ordinal);
        Assert.Contains("'[redacted]'", entry.Rendered, StringComparison.Ordinal);

        var state = Assert.IsAssignableFrom<IReadOnlyList<KeyValuePair<string, object?>>>(entry.State);
        var sql = Assert.Single(state, kv => kv.Key == "Sql").Value?.ToString();
        Assert.DoesNotContain("secret-password", sql, StringComparison.Ordinal);

        var loggedParameters = Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(
            Assert.Single(state, kv => kv.Key == "@Parameters").Value);
        Assert.Equal("[redacted]", loggedParameters["@p0"]);
        Assert.Equal("[redacted]", loggedParameters["@p1"]);
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_ScalarExecutedAsync_DoesNotLogScalarResult()
    {
        var logger = new CapturingLogger();
        var interceptor = new CapturingInterceptor(logger);
        using var command = new SqliteCommand("SELECT 'api-token-123'");

        await interceptor.ScalarExecutedAsync(command, null!, "api-token-123", TimeSpan.FromMilliseconds(1), CancellationToken.None);

        var entry = Assert.Single(logger.Entries);
        Assert.DoesNotContain("api-token-123", entry.Rendered, StringComparison.Ordinal);
        Assert.Contains("Executed scalar", entry.Rendered, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_CommandFailedAsync_RedactsCommandText()
    {
        var logger = new CapturingLogger();
        var interceptor = new CapturingInterceptor(logger);
        using var command = new SqliteCommand("SELECT * FROM Users WHERE Password = 'super-secret'");

        await interceptor.CommandFailedAsync(command, null!, new InvalidOperationException("boom"), CancellationToken.None);

        var entry = Assert.Single(logger.Entries);
        Assert.DoesNotContain("super-secret", entry.Rendered, StringComparison.Ordinal);
        Assert.Contains("[redacted]", entry.Rendered, StringComparison.Ordinal);
    }

    [Fact]
    public void Cli_connection_string_validation_treats_ef_sqlserver_provider_package_as_sqlserver()
    {
        var validated = nORM.Security.ConnectionStringValidator.Validate(
            "Server=localhost;Database=norm;User ID=sa;Password=secret;Encrypt=False",
            "Microsoft.EntityFrameworkCore.SqlServer");

        Assert.Contains("Encrypt=True", validated.ConnectionString, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("secret", validated.RedactedConnectionString, StringComparison.Ordinal);
        Assert.Contains("***", validated.RedactedConnectionString, StringComparison.Ordinal);
    }

    [Fact]
    public void Cli_connection_string_validation_preserves_explicit_sqlserver_trust_certificate_when_environment_selects_appsettings()
    {
        var previousEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        try
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Staging");

            var validated = nORM.Security.ConnectionStringValidator.Validate(
                "Server=localhost;Database=norm;User ID=sa;Password=secret;Encrypt=True;TrustServerCertificate=True",
                "sqlserver");

            Assert.Contains("TrustServerCertificate=True", validated.ConnectionString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("secret", validated.RedactedConnectionString, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", previousEnvironment);
        }
    }

    [Fact]
    public void Cli_connection_string_validation_keeps_execution_string_separate_from_redacted_diagnostics()
    {
        const string secret = "cli-secret-456";
        var validated = nORM.Security.ConnectionStringValidator.Validate(
            $"Host=localhost;Database=norm;Username=postgres;Password={secret}",
            "postgres");

        Assert.Contains(secret, validated.ConnectionString, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, validated.RedactedConnectionString, StringComparison.Ordinal);
        Assert.Contains("***", validated.RedactedConnectionString, StringComparison.Ordinal);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<LogEntry> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add(new LogEntry(formatter(state, exception), state!));
    }

    private sealed class CapturingInterceptor : BaseDbCommandInterceptor
    {
        public CapturingInterceptor(ILogger logger) : base(logger)
        {
        }
    }

    private sealed record LogEntry(string Rendered, object State);
}
