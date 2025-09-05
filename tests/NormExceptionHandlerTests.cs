using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Core;
using Xunit;
using System.Linq;

namespace nORM.Tests;

public class NormExceptionHandlerTests
{
    [Fact]
    public async Task ExecuteWithExceptionHandling_returns_result_on_success()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        var result = await handler.ExecuteWithExceptionHandling(() => Task.FromResult(5), "TestOp");
        Assert.Equal(5, result);
    }

    [Fact]
    public async Task ExecuteWithExceptionHandling_wraps_timeout_exception()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        var context = new Dictionary<string, object>
        {
            ["Sql"] = "SELECT 1",
            ["Param0"] = 42
        };
        var ex = await Assert.ThrowsAsync<NormTimeoutException>(() =>
            handler.ExecuteWithExceptionHandling<int>(async () =>
            {
                await Task.Delay(10);
                throw new TimeoutException("timeout");
            }, "TestOp", context));
        Assert.Equal("SELECT 1", ex.SqlStatement);
        Assert.Equal(42, ex.Parameters!["Param0"]);
        Assert.Contains("Operation timed out", ex.Message);
    }

    [Fact]
    public async Task ExecuteWithExceptionHandling_wraps_unexpected_exception()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        var context = new Dictionary<string, object>
        {
            ["Sql"] = "SELECT 1",
            ["Param0"] = 42
        };
        var ex = await Assert.ThrowsAsync<NormException>(() =>
            handler.ExecuteWithExceptionHandling<int>(async () =>
            {
                await Task.Delay(10);
                throw new InvalidOperationException("boom");
            }, "TestOp", context));
        Assert.Equal("SELECT 1", ex.SqlStatement);
        Assert.Equal(42, ex.Parameters!["Param0"]);
        Assert.Contains("Unexpected error", ex.Message);
    }

    [Fact]
    public async Task ExecuteWithExceptionHandling_wraps_sql_exception()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        var context = new Dictionary<string, object>
        {
            ["Sql"] = "SELECT 1",
            ["Param0"] = 42
        };
        var ex = await Assert.ThrowsAsync<NormDatabaseException>(() =>
            handler.ExecuteWithExceptionHandling<int>(() => Task.FromException<int>(CreateSqlException()), "TestOp", context));
        Assert.Equal("SELECT 1", ex.SqlStatement);
        Assert.Equal(42, ex.Parameters!["Param0"]);
        Assert.Contains("Database operation failed", ex.Message);
    }

    private static SqlException CreateSqlException()
    {
        var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 9);
        var error = (SqlError)errorCtor.Invoke(new object?[] { 1, (byte)0, (byte)0, "server", "error", "proc", 0, (uint)0, null });
        var errorCollection = (SqlErrorCollection)Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
        typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(errorCollection, new object?[] { error });
        var exCtor = typeof(SqlException).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { typeof(string), typeof(SqlErrorCollection), typeof(Exception), typeof(Guid) },
            null)!;
        return (SqlException)exCtor.Invoke(new object?[] { "error", errorCollection, null, Guid.NewGuid() });
    }
}
