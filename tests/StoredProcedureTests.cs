using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Validates that all stored procedure overloads use Provider.StoredProcedureCommandType
/// rather than hardcoding CommandType.StoredProcedure.
/// </summary>
public class StoredProcedureTests
{
    public class Item
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    /// <summary>
    /// SQLite uses CommandType.Text for stored procedures (since it has none).
    /// Verify ExecuteStoredProcedureAsync works with SQLite by executing a SELECT.
    /// </summary>
    [Fact]
    public async Task ExecuteStoredProcedureAsync_UsesProviderCommandType_WorksWithSqlite()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(1,'Alpha');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        // SQLite SP command type is CommandType.Text, so passing a SELECT query should work
        var results = await ctx.ExecuteStoredProcedureAsync<Item>("SELECT Id, Name FROM Item");
        Assert.Single(results);
        Assert.Equal("Alpha", results[0].Name);
    }

    /// <summary>
    /// ExecuteStoredProcedureAsAsyncEnumerable already correctly uses Provider.StoredProcedureCommandType.
    /// Verify all three overloads produce consistent results on SQLite.
    /// </summary>
    [Fact]
    public async Task ExecuteStoredProcedureWithOutputAsync_UsesProviderCommandType_WorksWithSqlite()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(2,'Beta');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        // ExecuteStoredProcedureWithOutputAsync with no output params — should use CommandType.Text on SQLite
        var result = await ctx.ExecuteStoredProcedureWithOutputAsync<Item>("SELECT Id, Name FROM Item");
        Assert.Single(result.Results);
        Assert.Equal("Beta", result.Results[0].Name);
    }

    /// <summary>
    /// Verifies SqliteProvider.StoredProcedureCommandType returns CommandType.Text.
    /// This is the provider override that all SP methods should use.
    /// </summary>
    [Fact]
    public void SqliteProvider_StoredProcedureCommandType_IsText()
    {
        var provider = new SqliteProvider();
        Assert.Equal(CommandType.Text, provider.StoredProcedureCommandType);
    }

    /// <summary>
    /// Verifies SqlServerProvider.StoredProcedureCommandType returns CommandType.StoredProcedure.
    /// </summary>
    [Fact]
    public void SqlServerProvider_StoredProcedureCommandType_IsStoredProcedure()
    {
        var provider = new SqlServerProvider();
        Assert.Equal(CommandType.StoredProcedure, provider.StoredProcedureCommandType);
    }

    // ── SP1: Output parameter name validation ───────────────────────────────
    // NOTE: NormException : DbException, so DefaultExecutionStrategy catches NormUsageException
    // and wraps it in a new NormException. Tests unwrap via ContainsUsageException helper.

    private static bool ContainsUsageException(Exception? ex)
    {
        while (ex != null)
        {
            if (ex is NormUsageException) return true;
            ex = ex.InnerException;
        }
        return false;
    }

    /// <summary>
    /// SP1: Valid output parameter names (letters/digits/underscore) must be accepted.
    /// </summary>
    [Fact]
    public async Task SP1_ValidOutputParamName_IsAccepted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(1,'Alpha');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Valid names must not be rejected by nORM's validator.
        // (SQLite doesn't support output parameters at the driver level, so a driver
        // exception is expected after validation passes — that is not a test failure.)
        var ex = await Record.ExceptionAsync(() =>
            ctx.ExecuteStoredProcedureWithOutputAsync<Item>(
                "SELECT Id, Name FROM Item",
                outputParameters: new OutputParameter("myParam", DbType.Int32)));

        Assert.False(ContainsUsageException(ex),
            "A valid output param name must not be rejected by the nORM validator.");
    }

    /// <summary>
    /// SP1: Output parameter name with a space must be rejected early (NormUsageException).
    /// Before the fix, IsSafeIdentifier accepted spaces, causing a late provider exception.
    /// </summary>
    [Fact]
    public async Task SP1_OutputParamName_WithSpace_ThrowsNormUsageException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = await Record.ExceptionAsync(() =>
            ctx.ExecuteStoredProcedureWithOutputAsync<Item>(
                "SELECT Id, Name FROM Item",
                outputParameters: new OutputParameter("bad name", DbType.Int32)));

        Assert.True(ContainsUsageException(ex), $"Expected NormUsageException in chain; got {ex?.GetType().Name}: {ex?.Message}");
    }

    /// <summary>
    /// SP1: Output parameter name with a dot must be rejected early (NormUsageException).
    /// "a.b" passes IsSafeIdentifier (dot-separated parts) but is not a valid param identifier.
    /// </summary>
    [Fact]
    public async Task SP1_OutputParamName_WithDot_ThrowsNormUsageException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = await Record.ExceptionAsync(() =>
            ctx.ExecuteStoredProcedureWithOutputAsync<Item>(
                "SELECT Id, Name FROM Item",
                outputParameters: new OutputParameter("schema.param", DbType.Int32)));

        Assert.True(ContainsUsageException(ex), $"Expected NormUsageException in chain; got {ex?.GetType().Name}: {ex?.Message}");
    }

    /// <summary>
    /// SP1: Empty output parameter name must be rejected.
    /// </summary>
    [Fact]
    public async Task SP1_OutputParamName_Empty_ThrowsNormUsageException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = await Record.ExceptionAsync(() =>
            ctx.ExecuteStoredProcedureWithOutputAsync<Item>(
                "SELECT Id, Name FROM Item",
                outputParameters: new OutputParameter("", DbType.Int32)));

        Assert.True(ContainsUsageException(ex), $"Expected NormUsageException in chain; got {ex?.GetType().Name}: {ex?.Message}");
    }

    /// <summary>
    /// SP1: Output parameter name starting with a digit must be rejected.
    /// </summary>
    [Fact]
    public async Task SP1_OutputParamName_StartsWithDigit_ThrowsNormUsageException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = await Record.ExceptionAsync(() =>
            ctx.ExecuteStoredProcedureWithOutputAsync<Item>(
                "SELECT Id, Name FROM Item",
                outputParameters: new OutputParameter("1badStart", DbType.Int32)));

        Assert.True(ContainsUsageException(ex), $"Expected NormUsageException in chain; got {ex?.GetType().Name}: {ex?.Message}");
    }

    /// <summary>
    /// All three SP overloads in DbContext.cs must use ctx._p.StoredProcedureCommandType.
    /// This test validates the async-enumerable overload (which was correct before) still works.
    /// </summary>
    [Fact]
    public async Task AsyncEnumerableVariant_UsesProviderCommandType_WorksWithSqlite()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(3,'Gamma');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = new List<Item>();
        await foreach (var item in ctx.ExecuteStoredProcedureAsAsyncEnumerable<Item>("SELECT Id, Name FROM Item"))
            results.Add(item);

        Assert.Single(results);
        Assert.Equal("Gamma", results[0].Name);
    }
}
