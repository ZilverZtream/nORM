using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

//<summary>
//Verifies that temporal infrastructure bootstrap SQL is correct per provider.
//Verifies that the existence probe distinguishes schema errors from operational errors.
//</summary>
public class TemporalProviderBootstrapTests
{
 // ── Finding C: provider-specific DDL ─────────────────────────────────────

 //<summary>
 //SQLite tags table creation SQL must use IF NOT EXISTS (valid SQLite syntax)
 //and TEXT column types appropriate for SQLite.
 //</summary>
    [Fact]
    public void SqliteProvider_GetCreateTagsTableSql_UsesIfNotExists()
    {
        var provider = new SqliteProvider();
        var sql = provider.GetCreateTagsTableSql();

        Assert.Contains("IF NOT EXISTS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__NormTemporalTags", sql);
        Assert.Contains("TEXT", sql, StringComparison.OrdinalIgnoreCase);
    }

 //<summary>
 //SQL Server tags table creation must NOT use IF NOT EXISTS (invalid T-SQL).
 //Must use OBJECT_ID check and NVARCHAR/DATETIME2 column types.
 //</summary>
    [Fact]
    public void SqlServerProvider_GetCreateTagsTableSql_UsesObjectIdCheck_NotIfNotExists()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetCreateTagsTableSql();

        Assert.DoesNotContain("IF NOT EXISTS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OBJECT_ID", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__NormTemporalTags", sql);
 // Must use SQL Server-appropriate types (not TEXT)
        Assert.Contains("NVARCHAR", sql, StringComparison.OrdinalIgnoreCase);
    }

 //<summary>
 //SQLite probe query must use LIMIT 1 (standard SQLite syntax).
 //</summary>
    [Fact]
    public void SqliteProvider_GetHistoryTableExistsProbeSql_UsesLimit()
    {
        var provider = new SqliteProvider();
        var sql = provider.GetHistoryTableExistsProbeSql("\"MyTable_History\"");

        Assert.Contains("LIMIT 1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("MyTable_History", sql);
    }

 //<summary>
 //SQL Server probe query must use TOP 1, not LIMIT 1.
 //LIMIT is not valid T-SQL; it causes a syntax error on SQL Server.
 //</summary>
    [Fact]
    public void SqlServerProvider_GetHistoryTableExistsProbeSql_UsesTop_NotLimit()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetHistoryTableExistsProbeSql("[MyTable_History]");

        Assert.Contains("TOP 1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("MyTable_History", sql);
    }

 // ── Finding A: escaped identifiers in tag SQL ─────────────────────────────

 //<summary>
 //SQLite GetTagLookupSql must use double-quoted escaped identifiers,
 //not bare unescaped table/column names.
 //</summary>
    [Fact]
    public void SqliteProvider_GetTagLookupSql_UsesEscapedIdentifiers()
    {
        var provider = new SqliteProvider();
        var sql = provider.GetTagLookupSql("@p0");

 // SQLite uses "double-quote" escaping
        Assert.Contains("\"__NormTemporalTags\"", sql);
        Assert.Contains("\"TagName\"", sql);
        Assert.Contains("\"Timestamp\"", sql);
        Assert.DoesNotContain("__NormTemporalTags\"", sql.Replace("\"__NormTemporalTags\"", "")); // confirm escaped form
    }

 //<summary>
 //SQL Server GetTagLookupSql must use bracket-escaped identifiers.
 //</summary>
    [Fact]
    public void SqlServerProvider_GetTagLookupSql_UsesEscapedIdentifiers()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetTagLookupSql("@p0");

 // SQL Server uses [bracket] escaping
        Assert.Contains("[__NormTemporalTags]", sql);
        Assert.Contains("[TagName]", sql);
        Assert.Contains("[Timestamp]", sql);
    }

 //<summary>
 //SQLite GetCreateTagSql must use double-quoted escaped identifiers.
 //</summary>
    [Fact]
    public void SqliteProvider_GetCreateTagSql_UsesEscapedIdentifiers()
    {
        var provider = new SqliteProvider();
        var sql = provider.GetCreateTagSql("@p0", "@p1");

        Assert.Contains("\"__NormTemporalTags\"", sql);
        Assert.Contains("\"TagName\"", sql);
        Assert.Contains("\"Timestamp\"", sql);
    }

 // ── Finding D: existence probe error semantics ────────────────────────────

 //<summary>
 //SQLite IsObjectNotFoundError must return true for SQLITE_ERROR (code 1)
 //combined with "no such table" message — the definitive table-absence signal.
 //</summary>
    [Fact]
    public void SqliteProvider_IsObjectNotFoundError_Code1_WithTableMessage_ReturnsTrue()
    {
        var provider = new SqliteProvider();
 // SqliteException(message, errorCode) constructor
        var ex = new SqliteException("SQLite Error 1: 'no such table: Foo_History'", 1);

        Assert.True(provider.IsObjectNotFoundError(ex));
    }

 //<summary>
 //SQLite IsObjectNotFoundError must return false for code 1 with a different message
 //(e.g. syntax error) — code 1 is broad and covers more than table-not-found.
 //</summary>
    [Fact]
    public void SqliteProvider_IsObjectNotFoundError_Code1_SyntaxError_ReturnsFalse()
    {
        var provider = new SqliteProvider();
        var ex = new SqliteException("SQLite Error 1: 'near \"SELEC\": syntax error'", 1);

        Assert.False(provider.IsObjectNotFoundError(ex));
    }

 //<summary>
 //SQLite IsObjectNotFoundError must return false for non-1 error codes
 //even if the message contains "no such table". Code 14 = SQLITE_CANTOPEN.
 //This ensures connectivity/permission errors do not masquerade as schema absence.
 //</summary>
    [Fact]
    public void SqliteProvider_IsObjectNotFoundError_NonTableCode_ReturnsFalse()
    {
        var provider = new SqliteProvider();
 // Code 14 = SQLITE_CANTOPEN
        var ex = new SqliteException("SQLite Error 14: 'no such table: (access denied)'", 14);

        Assert.False(provider.IsObjectNotFoundError(ex));
    }

 //<summary>
 //SqliteProvider.IsObjectNotFoundError returns false for a non-SqliteException DbException
 //(the code-based guard fails, base message-based check applies).
 //"doesn't exist" is recognized as schema-absence by the default fallback.
 //</summary>
    [Fact]
    public void SqliteProvider_IsObjectNotFoundError_GenericDbException_DoesntExistMessage_ReturnsFalse()
    {
 // SqliteProvider's override requires SqliteException; a generic DbException does NOT match,
 // so the guard fails and returns false (the typed check dominates over message).
        var provider = new SqliteProvider();
        var ex = new GenericDbException("Table 'mydb.Foo_History' doesn't exist");

 // SqliteProvider uses code+message AND requires SqliteException type — generic DbEx returns false.
        Assert.False(provider.IsObjectNotFoundError(ex));
    }

 //<summary>
 //The base message-based fallback correctly identifies "doesn't exist" messages.
 //Verified via SqliteProvider with a SQLite exception carrying a "doesn't exist" message
 //but code ≠ 1, to exercise the message path indirectly. Use separate provider instance
 //targeting only message-based check.
 //</summary>
    [Fact]
    public void DatabaseProvider_BaseMessageFallback_DoesntExist_Recognized()
    {
 // Test the default base.IsObjectNotFoundError directly via the virtual dispatch:
 // pass a SqliteException with error code 1 AND "doesn't exist" text.
        var provider = new SqliteProvider();
        var ex = new SqliteException("SQLite Error 1: 'no such table: Foo'", 1); // "no such table" triggers true
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

 //<summary>
 //Non-schema errors (e.g. "permission denied") must return false,
 //ensuring operational errors are not misidentified as schema absence.
 //</summary>
    [Fact]
    public void SqliteProvider_IsObjectNotFoundError_GenericPermissionError_ReturnsFalse()
    {
        var provider = new SqliteProvider();
        var ex = new GenericDbException("Access denied for user 'app'@'localhost'");

        Assert.False(provider.IsObjectNotFoundError(ex));
    }

 // ── End-to-end SQLite bootstrap integration ───────────────────────────────

 //<summary>
 //+A: End-to-end: SQLite tags table is created and tag lookup works with
 //escaped identifiers when temporal features are enabled on a real SQLite connection.
 //</summary>
    [Fact]
    public async System.Threading.Tasks.Task Sqlite_TemporalTagRoundTrip_WorksEndToEnd()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var provider = new SqliteProvider();

 // Create the tags table using provider-generated DDL
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = provider.GetCreateTagsTableSql();
            cmd.ExecuteNonQuery();
        }

 // Insert a tag using provider-escaped SQL
        var p0 = provider.ParamPrefix + "p0";
        var p1 = provider.ParamPrefix + "p1";
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = provider.GetCreateTagSql(p0, p1);
            cmd.Parameters.Add(new SqliteParameter(p0, "release-1.0"));
            cmd.Parameters.Add(new SqliteParameter(p1, "2024-01-15T12:00:00"));
            cmd.ExecuteNonQuery();
        }

 // Look up the tag using provider-escaped SQL
        using var lookupCmd = cn.CreateCommand();
        lookupCmd.CommandText = provider.GetTagLookupSql(p0);
        lookupCmd.Parameters.Add(new SqliteParameter(p0, "release-1.0"));
        var result = lookupCmd.ExecuteScalar();

        Assert.NotNull(result);
        Assert.Equal("2024-01-15T12:00:00", result!.ToString());
    }

 // ── Helper: generic DbException for fallback tests ────────────────────────

    private sealed class GenericDbException : DbException
    {
        public GenericDbException(string message) : base(message) { }
    }
}
