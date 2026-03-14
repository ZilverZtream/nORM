using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CountFastPathCoverageTests
{
    [Table("CountFastUser")]
    private sealed class CountFastUser
    {
        [Key]
        public int Id { get; set; }
        public bool IsActive { get; set; }
        public string Category { get; set; } = string.Empty;
    }

    [Fact]
    public async Task CountAsync_bool_predicate_uses_simple_scalar_shape()
    {
        using var connection = CreateConnection();
        var interceptor = new CountCommandCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        await using var context = new DbContext(connection, new SqliteProvider(), options);
        var count = await context.Query<CountFastUser>().Where(user => user.IsActive).CountAsync();

        Assert.Equal(2, count);
        Assert.Contains("SELECT COUNT(*)", interceptor.LastCommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"IsActive\"", interceptor.LastCommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("= 1", interceptor.LastCommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(interceptor.LastParameters);
    }

    [Fact]
    public async Task CountAsync_equality_predicate_binds_runtime_parameter()
    {
        using var connection = CreateConnection();
        var interceptor = new CountCommandCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        await using var context = new DbContext(connection, new SqliteProvider(), options);

        int targetId = 2;
        var count = await context.Query<CountFastUser>().Where(user => user.Id == targetId).CountAsync();

        Assert.Equal(1, count);
        Assert.Contains("SELECT COUNT(*)", interceptor.LastCommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("@p0", interceptor.LastCommandText, StringComparison.Ordinal);
        Assert.True(interceptor.LastParameters.TryGetValue("@p0", out var value));
        Assert.Equal(2, Convert.ToInt32(value));
    }

    private static SqliteConnection CreateConnection()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText =
            "CREATE TABLE CountFastUser (Id INTEGER PRIMARY KEY, IsActive INTEGER NOT NULL, Category TEXT NOT NULL);" +
            "INSERT INTO CountFastUser VALUES (1, 1, 'A');" +
            "INSERT INTO CountFastUser VALUES (2, 0, 'B');" +
            "INSERT INTO CountFastUser VALUES (3, 1, 'A');";
        command.ExecuteNonQuery();

        return connection;
    }

    private sealed class CountCommandCaptureInterceptor : IDbCommandInterceptor
    {
        public string LastCommandText { get; private set; } = string.Empty;
        public Dictionary<string, object?> LastParameters { get; private set; } = new();

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            LastCommandText = command.CommandText;
            LastParameters = command.Parameters
                .Cast<DbParameter>()
                .ToDictionary(parameter => parameter.ParameterName, parameter => parameter.Value);
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct)
            => Task.CompletedTask;
    }
}
