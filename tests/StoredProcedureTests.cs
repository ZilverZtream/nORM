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
[Xunit.Trait("Category", "Fast")]
public class StoredProcedureTests
{
    [Xunit.Trait("Category", "Fast")]
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

    [Fact]
    public async Task ExecuteStoredProcedureAsync_ReorderedColumns_PopulatesCorrectProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(5,'Delta');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.ExecuteStoredProcedureAsync<Item>("SELECT Name, Id FROM Item");

        Assert.Single(results);
        Assert.Equal(5, results[0].Id);
        Assert.Equal("Delta", results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BindsDictionaryParameters()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(7,'Eta'),(8,'Theta');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new Dictionary<string, object?> { ["id"] = 8 };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Id = @id",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Theta", results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureNonQueryAsync_UsesProviderCommandType_WorksWithSqlite()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(12,'Mu');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new Dictionary<string, object?> { ["id"] = 12, ["name"] = "Nu" };

        var affected = await ctx.ExecuteStoredProcedureNonQueryAsync(
            "UPDATE Item SET Name = @name WHERE Id = @id",
            parameters: parameters);

        Assert.Equal(1, affected);
        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT Name FROM Item WHERE Id = 12";
        Assert.Equal("Nu", verify.ExecuteScalar());
    }

    [Fact]
    public async Task ExecuteStoredProcedureNonQueryAsync_RejectsStackedTextCommand()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await Assert.ThrowsAsync<NormException>(() =>
            ctx.ExecuteStoredProcedureNonQueryAsync("UPDATE Item SET Name = 'x'; DROP TABLE Item"));
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BindsPrefixedDictionaryParameters()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(9,'Iota');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new Dictionary<string, object?> { ["@id"] = 9 };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Id = @id",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Iota", results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BindsStronglyTypedDictionaryParameters()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(10,'Kappa'),(11,'Lambda');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new Dictionary<string, string> { ["name"] = "Lambda" };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Name = @name",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal(11, results[0].Id);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BindsDbParameterDictionaryValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(12,'Mu'),(13,'Nu');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new Dictionary<string, object?>
        {
            ["id"] = new SqliteParameter { Value = 13 }
        };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Id = @id",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Nu", results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_DictionaryKeyOverridesDbParameterName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(14,'Xi'),(15,'Omicron');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new Dictionary<string, object?>
        {
            ["id"] = new SqliteParameter("@wrong", 15)
        };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Id = @id",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Omicron", results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_BindsDbParameterObjectProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(16,'Pi'),(17,'Rho');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new
        {
            id = new SqliteParameter { Value = 17 }
        };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Id = @id",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Rho", results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_ObjectPropertyNameOverridesDbParameterName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(18,'Sigma'),(19,'Tau');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var parameters = new
        {
            id = new SqliteParameter("@wrong", 19)
        };

        var results = await ctx.ExecuteStoredProcedureAsync<Item>(
            "SELECT Id, Name FROM Item WHERE Id = @id",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Tau", results[0].Name);
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

    [Fact]
    public async Task ExecuteStoredProcedureWithOutputAsync_ReorderedColumns_PopulatesCorrectProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(6,'Epsilon');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await ctx.ExecuteStoredProcedureWithOutputAsync<Item>("SELECT Name, Id FROM Item");

        Assert.Single(result.Results);
        Assert.Equal(6, result.Results[0].Id);
        Assert.Equal("Epsilon", result.Results[0].Name);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_SqliteTextProvider_RejectsNonReadOnlyCommandText()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(1,'Alpha');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = await Record.ExceptionAsync(() =>
            ctx.ExecuteStoredProcedureAsync<Item>("DELETE FROM Item"));

        Assert.True(ContainsUsageException(ex), $"Expected NormUsageException in chain; got {ex?.GetType().Name}: {ex?.Message}");
    }

    [Theory]
    [InlineData("bad proc")]
    [InlineData("dbo.GetUsers; DROP TABLE Item")]
    [InlineData("EXEC dbo.GetUsers")]
    public void StoredProcedureProvider_RejectsCommandTextNames(string procedureName)
    {
        var ex = Assert.Throws<NormUsageException>(() =>
            DbContext.ValidateStoredProcedureCommandText(
                procedureName,
                new SqlServerProvider(),
                parameters: null));

        Assert.Contains("Stored procedure names", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    [Fact]
    public async Task SP1_OutputParamDirection_Input_ThrowsNormUsageException()
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
                outputParameters: new OutputParameter("badDirection", DbType.Int32, null, ParameterDirection.Input)));

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

    [Fact]
    public async Task AsyncEnumerableVariant_ReorderedColumns_PopulatesCorrectProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item(Id INTEGER, Name TEXT); INSERT INTO Item VALUES(7,'Zeta');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = new List<Item>();
        await foreach (var item in ctx.ExecuteStoredProcedureAsAsyncEnumerable<Item>("SELECT Name, Id FROM Item"))
            results.Add(item);

        Assert.Single(results);
        Assert.Equal(7, results[0].Id);
        Assert.Equal("Zeta", results[0].Name);
    }
}
