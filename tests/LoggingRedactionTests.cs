using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Xunit;

namespace nORM.Tests;

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

    private sealed record LogEntry(string Rendered, object State);
}
