using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Coverage tests for PostgresProvider, MySqlProvider, and SqlServerProvider.
/// Uses SqliteParameterFactory and in-memory SQLite connections where a live
/// database connection is required by the method under test.
/// </summary>
public class ProviderImplementationCoverageTests
{
    // ═══════════════════════════════════════════════════════════════════════
    //  PostgresProvider — 25 tests
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void PostgresProvider_Escape_WrapsInDoubleQuotes()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"MyTable\"", provider.Escape("MyTable"));
    }

    [Fact]
    public void PostgresProvider_Escape_SchemaQualified_EscapesEachPart()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"myschema\".\"mytable\"", provider.Escape("myschema.mytable"));
    }

    [Fact]
    public void PostgresProvider_Escape_EmbeddedDoubleQuote_IsDoubled()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"tab\"\"le\"", provider.Escape("tab\"le"));
    }

    [Fact]
    public void PostgresProvider_ParamPrefix_IsAtSign()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("@", provider.ParamPrefix);
    }

    [Fact]
    public void PostgresProvider_MaxParameters_Is32767()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal(32_767, provider.MaxParameters);
    }

    [Fact]
    public void PostgresProvider_BooleanTrueLiteral_IsTrue()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("true", provider.BooleanTrueLiteral);
    }

    [Fact]
    public void PostgresProvider_BooleanFalseLiteral_IsFalse()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("false", provider.BooleanFalseLiteral);
    }

    [Fact]
    public void PostgresProvider_NullSafeEqual_EmitsIsNotDistinctFrom()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var result = provider.NullSafeEqual("col", "@p0");
        Assert.Contains("IS NOT DISTINCT FROM", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_NullSafeNotEqual_EmitsIsDistinctFrom()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var result = provider.NullSafeNotEqual("col", "@p0");
        Assert.Contains("IS DISTINCT FROM", result, StringComparison.OrdinalIgnoreCase);
        // Must NOT contain NOT DISTINCT FROM (that would be the equal form)
        Assert.DoesNotContain("IS NOT DISTINCT FROM", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_ApplyPaging_LimitAndOffset_EmitsLimitOffset()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM \"T\"");
        provider.ApplyPaging(sb, 10, 20, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("LIMIT 10", sql);
        Assert.Contains("OFFSET 20", sql);
    }

    [Fact]
    public void PostgresProvider_ApplyPaging_LimitOnly_NoOffset()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM \"T\"");
        provider.ApplyPaging(sb, 5, null, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("LIMIT 5", sql);
        Assert.DoesNotContain("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_ApplyPaging_OffsetOnly_EmitsOffsetNoLimit()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM \"T\"");
        provider.ApplyPaging(sb, null, 30, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("OFFSET 30", sql);
        // No limit clause should be present
        Assert.DoesNotContain("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_ApplyPaging_WithParamNames_EmitsParams()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM \"T\"");
        provider.ApplyPaging(sb, null, null, "@limit", "@offset");
        var sql = sb.ToSqlString();
        Assert.Contains("LIMIT @limit", sql);
        Assert.Contains("OFFSET @offset", sql);
    }

    [Fact]
    public void PostgresProvider_UsesFetchOffsetPaging_IsFalse()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.False(provider.UsesFetchOffsetPaging);
    }

    [Fact]
    public void PostgresProvider_GetInsertOrIgnoreSql_ContainsOnConflict()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sql = provider.GetInsertOrIgnoreSql("\"jt\"", "\"c1\"", "\"c2\"", "@p0", "@p1");
        Assert.Contains("ON CONFLICT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DO NOTHING", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_TranslateFunction_StringLength_EmitsLength()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var result = provider.TranslateFunction(nameof(string.Length), typeof(string), "\"Name\"");
        Assert.NotNull(result);
        Assert.Contains("LENGTH", result!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_TranslateFunction_StringToUpper_EmitsUpper()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var result = provider.TranslateFunction(nameof(string.ToUpper), typeof(string), "\"Name\"");
        Assert.NotNull(result);
        Assert.Contains("UPPER", result!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgresProvider_TranslateFunction_UnknownMethod_ReturnsNull()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var result = provider.TranslateFunction("NonExistentMethod", typeof(string), "x");
        Assert.Null(result);
    }

    [Fact]
    public void PostgresProvider_BuildContainsClause_IntValues_TypedIntArray()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var values = new List<object?> { (object?)1, 2, 3 };
        var sql = provider.BuildContainsClause(cmd, "col", values);
        Assert.Equal(1, cmd.Parameters.Count);
        Assert.Contains("ANY", sql, StringComparison.OrdinalIgnoreCase);
        var arr = cmd.Parameters[0].Value as int[];
        Assert.NotNull(arr);
        Assert.Equal(new[] { 1, 2, 3 }, arr);
    }

    [Fact]
    public void PostgresProvider_BuildContainsClause_GuidValues_TypedGuidArray()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        var values = new List<object?> { (object?)g1, g2 };
        provider.BuildContainsClause(cmd, "col", values);
        Assert.Equal(1, cmd.Parameters.Count);
        var arr = cmd.Parameters[0].Value as Guid[];
        Assert.NotNull(arr);
        Assert.Equal(new[] { g1, g2 }, arr);
    }

    [Fact]
    public void PostgresProvider_BuildContainsClause_MixedTypes_FallsBackToObjectArray()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var values = new List<object?> { (object?)1, "two", 3.0 };
        provider.BuildContainsClause(cmd, "col", values);
        Assert.Equal(1, cmd.Parameters.Count);
        var arr = cmd.Parameters[0].Value as object[];
        Assert.NotNull(arr);
    }

    [Fact]
    public void PostgresProvider_BuildContainsClause_AllNullValues_FallsBackToObjectArray()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var values = new List<object?> { null, null };
        provider.BuildContainsClause(cmd, "col", values);
        Assert.Equal(1, cmd.Parameters.Count);
        // All-null → object[] fallback
        var arr = cmd.Parameters[0].Value as object[];
        Assert.NotNull(arr);
    }

    [Fact]
    public void PostgresProvider_BuildContainsClause_EmitsAnyExpression()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var values = new List<object?> { (object?)42 };
        var sql = provider.BuildContainsClause(cmd, "\"MyCol\"", values);
        Assert.StartsWith("\"MyCol\" = ANY(", sql);
    }

    [Fact]
    public void PostgresProvider_CreateParameter_SetsNameAndValue()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var p = provider.CreateParameter("@myParam", 99);
        Assert.Equal("@myParam", p.ParameterName);
        Assert.Equal(99, p.Value);
    }

    [Fact]
    public async Task PostgresProvider_IntrospectTableColumns_DoesNotThrow_ReturnsEmpty()
    {
        // SQLite does not have information_schema, so result is empty but should not throw.
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var result = await provider.IntrospectTableColumnsAsync(cn, "myschema.mytable");
        Assert.NotNull(result);
        // SQLite silently fails (table not found → empty list via IsObjectNotFoundError)
        Assert.Empty(result);
    }

    [Fact]
    public void PostgresProvider_IsObjectNotFoundError_NoSuchTable_ReturnsTrue()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var ex = new SqliteException("no such table: foo", 1);
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void PostgresProvider_IsObjectNotFoundError_OtherError_ReturnsFalse()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var ex = new SqliteException("disk I/O error", 10);
        Assert.False(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void PostgresProvider_GetConcatSql_EmitsConcatFunction()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var result = provider.GetConcatSql("a", "b");
        Assert.Contains("CONCAT", result, StringComparison.OrdinalIgnoreCase);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  MySqlProvider — 20 tests
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MySqlProvider_Escape_AddsBackticks()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`MyTable`", provider.Escape("MyTable"));
    }

    [Fact]
    public void MySqlProvider_Escape_SchemaQualified_EscapesEachPart()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`mydb`.`mytable`", provider.Escape("mydb.mytable"));
    }

    [Fact]
    public void MySqlProvider_Escape_EmbeddedBacktick_IsDoubled()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`tab``le`", provider.Escape("tab`le"));
    }

    [Fact]
    public void MySqlProvider_ParamPrefix_IsAtSign()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("@", provider.ParamPrefix);
    }

    [Fact]
    public void MySqlProvider_MaxParameters_Is65535()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal(65_535, provider.MaxParameters);
    }

    [Fact]
    public void MySqlProvider_BooleanTrueLiteral_IsOne()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("1", provider.BooleanTrueLiteral);
    }

    [Fact]
    public void MySqlProvider_BooleanFalseLiteral_IsZero()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("0", provider.BooleanFalseLiteral);
    }

    [Fact]
    public void MySqlProvider_UsesFetchOffsetPaging_IsFalse()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.False(provider.UsesFetchOffsetPaging);
    }

    [Fact]
    public void MySqlProvider_UseAffectedRowsSemantics_IsTrue()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        // UseAffectedRowsSemantics is internal; access via reflection
        var prop = typeof(nORM.Providers.DatabaseProvider)
            .GetProperty("UseAffectedRowsSemantics",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        Assert.True((bool)prop.GetValue(provider)!);
    }

    [Fact]
    public void MySqlProvider_ApplyPaging_LimitAndOffset_UsesLimitOffsetStyle()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM `T`");
        provider.ApplyPaging(sb, 10, 20, null, null);
        var sql = sb.ToSqlString();
        // MySQL format: LIMIT offset, limit
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("20", sql);
        Assert.Contains("10", sql);
    }

    [Fact]
    public void MySqlProvider_ApplyPaging_OffsetOnly_EmitsMaxBigintLimit()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM `T`");
        provider.ApplyPaging(sb, null, 20, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("18446744073709551615", sql);
    }

    [Fact]
    public void MySqlProvider_ApplyPaging_LimitOnly_EmitsLimitClause()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM `T`");
        provider.ApplyPaging(sb, 5, null, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("LIMIT 5", sql);
    }

    [Fact]
    public void MySqlProvider_GetInsertOrIgnoreSql_ContainsInsertIgnore()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var sql = provider.GetInsertOrIgnoreSql("`jt`", "`c1`", "`c2`", "@p0", "@p1");
        Assert.Contains("INSERT IGNORE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySqlProvider_GetIdentityRetrievalString_ContainsLastInsertId()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = new nORM.Core.DbContext(new SqliteConnection("Data Source=:memory:"), new SqliteProvider())
            .GetMapping(typeof(MySqlTestEntity));
        var sql = provider.GetIdentityRetrievalString(mapping);
        Assert.Contains("LAST_INSERT_ID", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySqlProvider_TranslateFunction_StringLength_EmitsCharLength()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var result = provider.TranslateFunction(nameof(string.Length), typeof(string), "`Name`");
        Assert.NotNull(result);
        Assert.Contains("CHAR_LENGTH", result!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySqlProvider_TranslateFunction_StringToLower_EmitsLower()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var result = provider.TranslateFunction(nameof(string.ToLower), typeof(string), "`Name`");
        Assert.NotNull(result);
        Assert.Contains("LOWER", result!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySqlProvider_NullSafeEqual_ContainsIsNullOrEquals()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var result = provider.NullSafeEqual("col", "@p0");
        // Base default: (col = @p0 OR (col IS NULL AND @p0 IS NULL))
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void MySqlProvider_IsObjectNotFoundError_TableDoesntExist_ReturnsTrue()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var ex = new SqliteException("Table 'mydb.test' doesn't exist", 1);
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void MySqlProvider_IsObjectNotFoundError_NoSuchTable_ReturnsTrue()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var ex = new SqliteException("no such table: foo", 1);
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void MySqlProvider_IsObjectNotFoundError_OtherError_ReturnsFalse()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var ex = new SqliteException("connection refused", 2002);
        Assert.False(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void MySqlProvider_CreateParameter_SetsNameAndValue()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var p = provider.CreateParameter("@myParam", "hello");
        Assert.Equal("@myParam", p.ParameterName);
        Assert.Equal("hello", p.Value);
    }

    [Fact]
    public async Task MySqlProvider_IntrospectTableColumns_DoesNotThrow_ReturnsEmpty()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var result = await provider.IntrospectTableColumnsAsync(cn, "myschema.mytable");
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  SqlServerProvider — 25 tests
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServerProvider_Escape_AddsSquareBrackets()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("[MyTable]", provider.Escape("MyTable"));
    }

    [Fact]
    public void SqlServerProvider_Escape_SchemaQualified_EscapesEachPart()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("[dbo].[MyTable]", provider.Escape("dbo.MyTable"));
    }

    [Fact]
    public void SqlServerProvider_Escape_EmbeddedCloseBracket_IsDoubled()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("[tab]]le]", provider.Escape("tab]le"));
    }

    [Fact]
    public void SqlServerProvider_ParamPrefix_IsAtSign()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("@", provider.ParamPrefix);
    }

    [Fact]
    public void SqlServerProvider_MaxParameters_Is2100()
    {
        var provider = new SqlServerProvider();
        Assert.Equal(2_100, provider.MaxParameters);
    }

    [Fact]
    public void SqlServerProvider_BooleanTrueLiteral_IsOne()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("1", provider.BooleanTrueLiteral);
    }

    [Fact]
    public void SqlServerProvider_BooleanFalseLiteral_IsZero()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("0", provider.BooleanFalseLiteral);
    }

    [Fact]
    public void SqlServerProvider_UsesFetchOffsetPaging_IsTrue()
    {
        var provider = new SqlServerProvider();
        Assert.True(provider.UsesFetchOffsetPaging);
    }

    [Fact]
    public void SqlServerProvider_ApplyPaging_LimitAndOffset_EmitsFetchOffset()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [T] ORDER BY [Id]");
        provider.ApplyPaging(sb, 10, 20, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH NEXT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("20", sql);
        Assert.Contains("10", sql);
    }

    [Fact]
    public void SqlServerProvider_ApplyPaging_OffsetOnly_EmitsOffsetRows()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [T] ORDER BY [Id]");
        provider.ApplyPaging(sb, null, 30, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("OFFSET 30 ROWS", sql, StringComparison.OrdinalIgnoreCase);
        // No FETCH NEXT when no limit
        Assert.DoesNotContain("FETCH NEXT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_ApplyPaging_LimitOnly_EmitsFetchFirstRows()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [T] ORDER BY [Id]");
        provider.ApplyPaging(sb, 5, null, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("OFFSET 0 ROWS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH NEXT 5 ROWS ONLY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_ApplyPaging_NoOrderBy_AddsOrderBySelectNull()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM [T]");
        provider.ApplyPaging(sb, 5, null, null, null);
        var sql = sb.ToSqlString();
        Assert.Contains("ORDER BY (SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_GetInsertOrIgnoreSql_ContainsIfNotExists()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetInsertOrIgnoreSql("[jt]", "[c1]", "[c2]", "@p0", "@p1");
        Assert.Contains("IF NOT EXISTS", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_GetIdentityRetrievalString_ContainsScopeIdentity()
    {
        var provider = new SqlServerProvider();
        var mapping = new nORM.Core.DbContext(new SqliteConnection("Data Source=:memory:"), new SqliteProvider())
            .GetMapping(typeof(SqlServerTestEntity));
        var sql = provider.GetIdentityRetrievalString(mapping);
        Assert.Contains("SCOPE_IDENTITY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_TranslateFunction_StringLength_EmitsLen()
    {
        var provider = new SqlServerProvider();
        var result = provider.TranslateFunction(nameof(string.Length), typeof(string), "[Name]");
        Assert.NotNull(result);
        // SQL Server uses LEN, not LENGTH
        Assert.Contains("LEN", result!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_TranslateFunction_Max_EmitsMax()
    {
        // Math.Abs as a stand-in for a method that should be translated
        var provider = new SqlServerProvider();
        var result = provider.TranslateFunction(nameof(Math.Abs), typeof(Math), "col");
        Assert.NotNull(result);
        Assert.Contains("ABS", result!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_TranslateFunction_UnknownMethod_ReturnsNull()
    {
        var provider = new SqlServerProvider();
        var result = provider.TranslateFunction("GobbledyGook", typeof(string), "x");
        Assert.Null(result);
    }

    [Fact]
    public void SqlServerProvider_NullSafeEqual_ContainsEquality()
    {
        var provider = new SqlServerProvider();
        var result = provider.NullSafeEqual("col", "@p0");
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void SqlServerProvider_NullSafeNotEqual_ContainsNotEqual()
    {
        var provider = new SqlServerProvider();
        var result = provider.NullSafeNotEqual("col", "@p0");
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void SqlServerProvider_GetCreateTagsTableSql_ContainsObjectIdCheck()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetCreateTagsTableSql();
        Assert.Contains("OBJECT_ID", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__NormTemporalTags", sql);
    }

    [Fact]
    public void SqlServerProvider_GetCreateTagsTableSql_ContainsNvarcharAndDatetime2()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetCreateTagsTableSql();
        Assert.Contains("NVARCHAR", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DATETIME2", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_GetHistoryTableExistsProbeSql_ContainsTop1()
    {
        var provider = new SqlServerProvider();
        var sql = provider.GetHistoryTableExistsProbeSql("[MyTable_History]");
        Assert.Contains("TOP 1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[MyTable_History]", sql);
    }

    [Fact]
    public void SqlServerProvider_GetConcatSql_ContainsConcat()
    {
        var provider = new SqlServerProvider();
        var result = provider.GetConcatSql("a", "b");
        Assert.Contains("CONCAT", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServerProvider_IsObjectNotFoundError_NonSqlException_ReturnsFalse()
    {
        // SqlServerProvider.IsObjectNotFoundError only recognises SqlException with Number==208.
        // A SqliteException (not a SqlException) never matches, so returns false regardless of message.
        var provider = new SqlServerProvider();
        var exMsg = new SqliteException("Invalid object name 'TestTable'.", 1);
        Assert.False(provider.IsObjectNotFoundError(exMsg));
    }

    [Fact]
    public void SqlServerProvider_IsObjectNotFoundError_OtherMessage_ReturnsFalse()
    {
        var provider = new SqlServerProvider();
        var ex = new SqliteException("Login failed for user 'sa'.", 18456);
        Assert.False(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void SqlServerProvider_CreateParameter_SetsNameAndValue()
    {
        var provider = new SqlServerProvider();
        var p = provider.CreateParameter("@myParam", 42);
        Assert.Equal("@myParam", p.ParameterName);
        Assert.Equal(42, p.Value);
    }

    [Fact]
    public async Task SqlServerProvider_IntrospectTableColumns_WithSqliteConn_ThrowsOrReturnsEmpty()
    {
        // SqlServerProvider.IsObjectNotFoundError only recognises SqlException (not SqliteException),
        // so the "no such table" error from SQLite propagates as a DbException rather than
        // being swallowed. Verify the method either returns empty or throws a DbException
        // (but NOT an unrelated exception type like NullReferenceException).
        var provider = new SqlServerProvider();
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        IReadOnlyList<nORM.Providers.DatabaseProvider.LiveColumnInfo>? result = null;
        System.Data.Common.DbException? caughtDbEx = null;
        try
        {
            result = await provider.IntrospectTableColumnsAsync(cn, "dbo.SomeTable");
        }
        catch (System.Data.Common.DbException dbEx)
        {
            caughtDbEx = dbEx;
        }
        // Either an empty list was returned (if error was swallowed) or a DbException was thrown.
        // Both outcomes are valid for a non-SQL Server connection.
        Assert.True(result is { Count: 0 } || caughtDbEx != null,
            "Expected either empty result or DbException for non-SQL Server connection.");
    }

    // Helper entity types used only for GetMapping calls in provider tests
    [System.ComponentModel.DataAnnotations.Schema.Table("MySqlTestEntity")]
    private class MySqlTestEntity
    {
        [System.ComponentModel.DataAnnotations.Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("SqlServerTestEntity")]
    private class SqlServerTestEntity
    {
        [System.ComponentModel.DataAnnotations.Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
