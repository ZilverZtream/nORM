using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class ClientEvaluationPolicyTests
{
    [Fact]
    public async Task ClientEvaluationPolicy_Default_rejects_projection_client_eval()
    {
        await using var connection = await OpenConnectionAsync();
        using var context = new DbContext(connection, new SqliteProvider());

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            context.Query<ClientEvalUser>()
                .Select(u => new ClientEvalUser { Id = u.Id, Name = FormatName(u.Name) })
                .ToListAsync());

        Assert.Contains("requires client-side evaluation", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ClientEvaluationPolicy_Warn_logs_and_applies_projection_after_server_query()
    {
        await using var connection = await OpenConnectionAsync();
        var logger = new CapturingLogger();
        var options = new DbContextOptions
        {
            ClientEvaluationPolicy = ClientEvaluationPolicy.Warn,
            Logger = logger
        };

        using var context = new DbContext(connection, new SqliteProvider(), options);
        var result = await context.Query<ClientEvalUser>()
            .Select(u => new ClientEvalUser { Id = u.Id, Name = FormatName(u.Name) })
            .ToListAsync();

        Assert.Equal(new[] { "ADA", "GRACE" }, result.Select(u => u.Name));
        Assert.Contains(logger.Messages, message => message.Contains("-- CLIENT-EVAL", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ClientEvaluationPolicy_Allow_applies_projection_without_client_eval_warning()
    {
        await using var connection = await OpenConnectionAsync();
        var logger = new CapturingLogger();
        var options = new DbContextOptions
        {
            ClientEvaluationPolicy = ClientEvaluationPolicy.Allow,
            Logger = logger
        };

        using var context = new DbContext(connection, new SqliteProvider(), options);
        var result = await context.Query<ClientEvalUser>()
            .Select(u => new ClientEvalUser { Id = u.Id, Name = FormatName(u.Name) })
            .ToListAsync();

        Assert.Equal(new[] { "ADA", "GRACE" }, result.Select(u => u.Name));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("-- CLIENT-EVAL", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ClientEvaluationPolicy_Throw_rejects_projection_client_eval()
    {
        await using var connection = await OpenConnectionAsync();
        var options = new DbContextOptions
        {
            ClientEvaluationPolicy = ClientEvaluationPolicy.Throw
        };

        using var context = new DbContext(connection, new SqliteProvider(), options);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            context.Query<ClientEvalUser>()
                .Select(u => new ClientEvalUser { Id = u.Id, Name = FormatName(u.Name) })
                .ToListAsync());

        Assert.Contains("requires client-side evaluation", ex.Message, StringComparison.Ordinal);
    }

    private static string FormatName(string value) => value.ToUpperInvariant();

    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE ClientEvalUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO ClientEvalUser (Id, Name) VALUES (1, 'ada'), (2, 'grace');
            """;
        await command.ExecuteNonQueryAsync();
        return connection;
    }

    private sealed class ClientEvalUser
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

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
