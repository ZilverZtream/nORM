using System.Reflection;
using nORM.Providers;
using nORM.Versioning;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that the TemporalManager DDL validator accepts SQL Server IF OBJECT_ID
/// bootstrap SQL in addition to the existing CREATE/ALTER/DROP allowlist.
/// </summary>
public class TemporalDdlValidatorTests
{
    private static bool CallIsValidDdl(string ddl)
    {
        var method = typeof(TemporalManager).GetMethod(
            "IsValidDdl",
            BindingFlags.Static | BindingFlags.NonPublic)!;
        return (bool)method.Invoke(null, new object[] { ddl })!;
    }

    /// <summary>
    /// SQL Server bootstrap DDL starts with "IF OBJECT_ID" — must pass the validator.
    /// Previously rejected, causing temporal initialization to fail on SQL Server.
    /// </summary>
    [Fact]
    public void IsValidDdl_IfObjectId_ReturnsTrue()
    {
        const string sql = "IF OBJECT_ID(N'__NormTemporalTags', N'U') IS NULL\n" +
                           "CREATE TABLE [__NormTemporalTags] ([TagName] NVARCHAR(200) NOT NULL PRIMARY KEY, " +
                           "[Timestamp] DATETIME2 NOT NULL)";
        Assert.True(CallIsValidDdl(sql));
    }

    /// <summary>
    /// Baseline — CREATE TABLE must still pass as before.
    /// </summary>
    [Fact]
    public void IsValidDdl_CreateTable_ReturnsTrue()
    {
        const string sql = "CREATE TABLE IF NOT EXISTS \"__NormTemporalTags\" (\"TagName\" TEXT PRIMARY KEY, \"Timestamp\" TEXT)";
        Assert.True(CallIsValidDdl(sql));
    }

    /// <summary>
    /// ALTER TABLE must still pass.
    /// </summary>
    [Fact]
    public void IsValidDdl_AlterTable_ReturnsTrue()
    {
        const string sql = "ALTER TABLE [Orders] ADD COLUMN [ArchivedAt] DATETIME2 NULL";
        Assert.True(CallIsValidDdl(sql));
    }

    /// <summary>
    /// Security guard — DROP TABLE must be rejected. No temporal provider generates
    /// DROP TABLE as part of bootstrap; only DROP TRIGGER/FUNCTION/PROCEDURE are
    /// legitimate temporal DDL. Allowing DROP TABLE would let arbitrary table
    /// destruction pass through the DDL validator.
    /// </summary>
    [Fact]
    public void IsValidDdl_DropTable_ReturnsFalse()
    {
        const string sql = "DROP TABLE IF EXISTS \"__NormTemporalTags\"";
        Assert.False(CallIsValidDdl(sql));
    }

    /// <summary>
    /// Security guard — SELECT must still be rejected.
    /// </summary>
    [Fact]
    public void IsValidDdl_SelectStatement_ReturnsFalse()
    {
        const string sql = "SELECT * FROM __NormTemporalTags";
        Assert.False(CallIsValidDdl(sql));
    }

    /// <summary>
    /// Security guard — UPDATE must still be rejected.
    /// </summary>
    [Fact]
    public void IsValidDdl_UpdateStatement_ReturnsFalse()
    {
        const string sql = "UPDATE __NormTemporalTags SET Timestamp = '2024-01-01' WHERE TagName = 'x'";
        Assert.False(CallIsValidDdl(sql));
    }

    /// <summary>
    /// End-to-end check — SqlServerProvider.GetCreateTagsTableSql() returns DDL
    /// that begins with "IF" and is accepted by the validator.
    /// </summary>
    [Fact]
    public void SqlServerGetCreateTagsTableSql_PassesIsValidDdl()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetCreateTagsTableSql().Trim();

        // Must start with IF (SQL Server conditional guard)
        Assert.True(sql.StartsWith("IF", System.StringComparison.OrdinalIgnoreCase),
            $"Expected SQL Server DDL to start with 'IF', but got: {sql[..System.Math.Min(50, sql.Length)]}");

        // Must pass the DDL validator
        Assert.True(CallIsValidDdl(sql),
            "SqlServerProvider.GetCreateTagsTableSql() was rejected by IsValidDdl");
    }
}
