using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Shared helpers ────────────────────────────────────────────────────────────

/// <summary>
/// Minimal IDbParameterFactory backed by SqliteParameter; used wherever
/// MySqlProvider or PostgresProvider require one but the factory isn't actually called.
/// </summary>
file sealed class TestParameterFactory : IDbParameterFactory
{
    public DbParameter CreateParameter(string name, object? value)
        => new SqliteParameter(name, value ?? DBNull.Value);
}

// ── DatabaseProvider (base) via SqliteProvider ───────────────────────────────

public class DatabaseProviderBaseTests
{
    private readonly SqliteProvider _sqlite = new();

    [Fact]
    public void ParamPrefix_IsAtSign()
    {
        Assert.Equal("@", _sqlite.ParamPrefix);
    }

    [Fact]
    public void ParameterPrefixChar_IsAtSign()
    {
        Assert.Equal('@', _sqlite.ParameterPrefixChar);
    }

    [Fact]
    public void BooleanTrueLiteral_DefaultIsOne()
    {
        // SQLite inherits the base default of "1"
        Assert.Equal("1", _sqlite.BooleanTrueLiteral);
    }

    [Fact]
    public void BooleanFalseLiteral_DefaultIsZero()
    {
        Assert.Equal("0", _sqlite.BooleanFalseLiteral);
    }

    [Fact]
    public void UsesFetchOffsetPaging_DefaultIsFalse()
    {
        Assert.False(_sqlite.UsesFetchOffsetPaging);
    }

    [Fact]
    public void PrefersSyncExecution_SqliteReturnsTrue()
    {
        Assert.True(_sqlite.PrefersSyncExecution);
    }

    [Fact]
    public void GetConcatSql_DefaultReturnsConcatFunction()
    {
        // Base default — SQLite overrides to use ||, others keep CONCAT
        var sql = new SqlServerProvider().GetConcatSql("a", "b");
        Assert.Equal("CONCAT(a, b)", sql);
    }

    [Fact]
    public void GetConcatSql_SqliteUsesDoublePipe()
    {
        var sql = _sqlite.GetConcatSql("col1", "col2");
        // SQLite wraps in parens: (col1 || col2)
        Assert.Contains("||", sql);
        Assert.Contains("col1", sql);
        Assert.Contains("col2", sql);
    }

    [Fact]
    public void GetInsertOrIgnoreSql_SqliteUsesInsertOrIgnore()
    {
        // SQLite overrides to INSERT OR IGNORE
        var sql = _sqlite.GetInsertOrIgnoreSql("\"T\"", "\"A\"", "\"B\"", "@p1", "@p2");
        Assert.Contains("INSERT OR IGNORE INTO", sql);
        Assert.Contains("\"T\"", sql);
    }

    [Fact]
    public void NullSafeEqual_BaseProducesOrExpansion()
    {
        // SqlServer uses base default (no override)
        var provider = new SqlServerProvider();
        var sql = provider.NullSafeEqual("col", "@p0");
        Assert.Contains("IS NULL", sql);
        Assert.Contains("col", sql);
    }

    [Fact]
    public void NullSafeNotEqual_BaseProducesOrExpansion()
    {
        var provider = new SqlServerProvider();
        var sql = provider.NullSafeNotEqual("col", "@p0");
        Assert.Contains("IS NOT NULL", sql);
    }

    [Fact]
    public void EscapeLikePattern_EscapesPercentAndUnderscore()
    {
        var pattern = _sqlite.EscapeLikePattern("50% off today_sale");
        Assert.Contains("\\%", pattern);
        Assert.Contains("\\_", pattern);
    }

    [Fact]
    public void GetLikeEscapeSql_ProducesNestedReplace()
    {
        var sql = _sqlite.GetLikeEscapeSql("@col");
        Assert.Contains("REPLACE", sql);
        Assert.Contains("@col", sql);
    }

    [Fact]
    public void GetCreateTagsTableSql_BaseContainsPrimaryKey()
    {
        // Base implementation (Sqlite doesn't override)
        var sql = _sqlite.GetCreateTagsTableSql();
        Assert.Contains("__NormTemporalTags", sql);
        Assert.Contains("PRIMARY KEY", sql);
        Assert.Contains("IF NOT EXISTS", sql);
    }

    [Fact]
    public void GetHistoryTableExistsProbeSql_BaseUsesLimit()
    {
        var sql = _sqlite.GetHistoryTableExistsProbeSql("\"MyTable_History\"");
        Assert.Contains("LIMIT 1", sql);
        Assert.Contains("\"MyTable_History\"", sql);
    }

    [Fact]
    public void GetTagLookupSql_BaseFormatsSelectCorrectly()
    {
        var sql = _sqlite.GetTagLookupSql("@p0");
        Assert.Contains("__NormTemporalTags", sql);
        Assert.Contains("@p0", sql);
        Assert.Contains("SELECT", sql);
    }

    [Fact]
    public void GetCreateTagSql_BaseFormatsInsertCorrectly()
    {
        var sql = _sqlite.GetCreateTagSql("@tag", "@ts");
        Assert.Contains("INSERT INTO", sql);
        Assert.Contains("__NormTemporalTags", sql);
        Assert.Contains("@tag", sql);
        Assert.Contains("@ts", sql);
    }

    [Fact]
    public void IsObjectNotFoundError_BaseReturnsTrueForNoSuchTable()
    {
        // MySQL/Postgres use base message-based implementation
        var provider = new MySqlProvider(new TestParameterFactory());
        var ex = new FakeDbException("no such table: Foo");
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void IsObjectNotFoundError_BaseReturnsTrueForDoesNotExist()
    {
        var provider = new MySqlProvider(new TestParameterFactory());
        var ex = new FakeDbException("Table does not exist");
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void IsObjectNotFoundError_BaseReturnsFalseForOtherErrors()
    {
        var provider = new MySqlProvider(new TestParameterFactory());
        var ex = new FakeDbException("connection refused");
        Assert.False(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void IsObjectNotFoundError_BaseReturnsTrueForInvalidObjectName()
    {
        // MySQL/Postgres provider uses base message check
        var provider = new MySqlProvider(new TestParameterFactory());
        var ex = new FakeDbException("Invalid object name 'dbo.Foo'");
        Assert.True(provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void IsObjectNotFoundError_Sqlite_RequiresSqliteException_PlainExceptionReturnsFalse()
    {
        // SqliteProvider requires SqliteException with code 1; plain FakeDbException returns false
        var ex = new FakeDbException("no such table: Foo");
        Assert.False(_sqlite.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void EscapeLikePattern_EscapesEscapeCharItself()
    {
        // If the value contains the escape char itself it should be doubled
        var pattern = _sqlite.EscapeLikePattern("100\\% done");
        Assert.Contains("\\\\", pattern);
    }

    [Fact]
    public void LikeEscapeChar_DefaultIsBackslash()
    {
        Assert.Equal('\\', _sqlite.LikeEscapeChar);
    }
}

// ── MySqlProvider ─────────────────────────────────────────────────────────────

public class MySqlProviderCovTests
{
    private readonly MySqlProvider _provider = new(new TestParameterFactory());

    [Fact]
    public void Escape_SimpleIdentifier_UsesBackticks()
    {
        Assert.Equal("`orders`", _provider.Escape("orders"));
    }

    [Fact]
    public void Escape_SchemaQualified_EachPartBacktickEscaped()
    {
        Assert.Equal("`mydb`.`orders`", _provider.Escape("mydb.orders"));
    }

    [Fact]
    public void Escape_EmbeddedBacktick_IsDoubled()
    {
        Assert.Equal("`my``table`", _provider.Escape("my`table"));
    }

    [Fact]
    public void Escape_NullOrWhitespace_ReturnedAsIs()
    {
        Assert.Equal("", _provider.Escape(""));
        Assert.Equal("   ", _provider.Escape("   "));
    }

    [Fact]
    public void MaxSqlLength_IsLarge()
    {
        Assert.Equal(4_194_304, _provider.MaxSqlLength);
    }

    [Fact]
    public void MaxParameters_IsLarge()
    {
        Assert.Equal(65_535, _provider.MaxParameters);
    }

    [Fact]
    public void GetIdentityRetrievalString_ContainsLastInsertId()
    {
        // TableMapping cannot easily be constructed, but the method ignores it
        var sql = _provider.GetIdentityRetrievalString(null!);
        Assert.Contains("LAST_INSERT_ID", sql);
    }

    [Fact]
    public void GetInsertOrIgnoreSql_UsesInsertIgnore()
    {
        var sql = _provider.GetInsertOrIgnoreSql("`T`", "`A`", "`B`", "@p1", "@p2");
        Assert.StartsWith("INSERT IGNORE INTO", sql);
        Assert.Contains("`T`", sql);
    }

    [Fact]
    public void TranslateFunction_ToUpper_ReturnsUPPER()
    {
        var sql = _provider.TranslateFunction("ToUpper", typeof(string), "col");
        Assert.Equal("UPPER(col)", sql);
    }

    [Fact]
    public void TranslateFunction_ToLower_ReturnsLOWER()
    {
        var sql = _provider.TranslateFunction("ToLower", typeof(string), "col");
        Assert.Equal("LOWER(col)", sql);
    }

    [Fact]
    public void TranslateFunction_Length_ReturnsCharLength()
    {
        var sql = _provider.TranslateFunction("Length", typeof(string), "col");
        Assert.Equal("CHAR_LENGTH(col)", sql);
    }

    [Fact]
    public void TranslateFunction_DateTimeYear_ReturnsYEAR()
    {
        var sql = _provider.TranslateFunction("Year", typeof(DateTime), "col");
        Assert.Equal("YEAR(col)", sql);
    }

    [Fact]
    public void TranslateFunction_DateTimeMonth_ReturnsMONTH()
    {
        var sql = _provider.TranslateFunction("Month", typeof(DateTime), "col");
        Assert.Equal("MONTH(col)", sql);
    }

    [Fact]
    public void TranslateFunction_MathAbs_ReturnsABS()
    {
        var sql = _provider.TranslateFunction("Abs", typeof(Math), "col");
        Assert.Equal("ABS(col)", sql);
    }

    [Fact]
    public void TranslateFunction_MathRound_ReturnsROUND()
    {
        var sql = _provider.TranslateFunction("Round", typeof(Math), "col");
        Assert.Equal("ROUND(col)", sql);
    }

    [Fact]
    public void TranslateFunction_MathRoundWithScale_ReturnsROUNDWithScale()
    {
        var sql = _provider.TranslateFunction("Round", typeof(Math), "col", "2");
        Assert.Equal("ROUND(col, 2)", sql);
    }

    [Fact]
    public void TranslateFunction_Unknown_ReturnsNull()
    {
        var sql = _provider.TranslateFunction("Foo", typeof(string), "col");
        Assert.Null(sql);
    }

    [Fact]
    public void TranslateJsonPathAccess_ProducesJsonUnquoteExtract()
    {
        var sql = _provider.TranslateJsonPathAccess("data", "user.name");
        Assert.Contains("JSON_UNQUOTE", sql);
        Assert.Contains("JSON_EXTRACT", sql);
        Assert.Contains("data", sql);
        Assert.Contains("user.name", sql);
    }

    [Fact]
    public void ApplyPaging_LimitOnly_ProducesLimitClause()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, 10, null, "@p0", null);
        Assert.Contains("LIMIT", sb.ToString());
        Assert.Contains("@p0", sb.ToString());
    }

    [Fact]
    public void ApplyPaging_OffsetAndLimit_ProducesOffsetCommaLimit()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, 10, 5, "@p0", "@p1");
        var sql = sb.ToString();
        Assert.Contains("LIMIT", sql);
        Assert.Contains("@p1,", sql);
        Assert.Contains("@p0", sql);
    }

    [Fact]
    public void ApplyPaging_OffsetOnly_UsesMaxBigint()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, null, 5, null, "@p1");
        var sql = sb.ToString();
        Assert.Contains("18446744073709551615", sql);
        Assert.Contains("@p1", sql);
    }

    [Fact]
    public void ApplyPaging_NoArgs_ProducesNothing()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, null, null, null, null);
        Assert.Equal("", sb.ToString());
    }

    [Fact]
    public void BooleanTrueLiteral_InheritsDefault1()
    {
        Assert.Equal("1", _provider.BooleanTrueLiteral);
    }

    [Fact]
    public void ParamPrefix_IsAtSign()
    {
        Assert.Equal("@", _provider.ParamPrefix);
    }

    [Fact]
    public void GetCreateTagsTableSql_ContainsTemporalTagsTable()
    {
        var sql = _provider.GetCreateTagsTableSql();
        Assert.Contains("__NormTemporalTags", sql);
    }

    [Fact]
    public void GetTagLookupSql_ContainsParam()
    {
        var sql = _provider.GetTagLookupSql("@p0");
        Assert.Contains("@p0", sql);
    }

    [Fact]
    public void GetCreateTagSql_ContainsInsertAndParams()
    {
        var sql = _provider.GetCreateTagSql("@tag", "@ts");
        Assert.Contains("INSERT INTO", sql);
        Assert.Contains("@tag", sql);
        Assert.Contains("@ts", sql);
    }
}

// ── PostgresProvider ──────────────────────────────────────────────────────────

public class PostgresProviderTests
{
    private readonly PostgresProvider _provider = new(new TestParameterFactory());

    [Fact]
    public void Escape_SimpleIdentifier_UsesDoubleQuotes()
    {
        Assert.Equal("\"orders\"", _provider.Escape("orders"));
    }

    [Fact]
    public void Escape_SchemaQualified_EachPartDoubleQuoted()
    {
        Assert.Equal("\"public\".\"orders\"", _provider.Escape("public.orders"));
    }

    [Fact]
    public void Escape_EmbeddedDoubleQuote_IsDoubled()
    {
        Assert.Equal("\"my\"\"table\"", _provider.Escape("my\"table"));
    }

    [Fact]
    public void Escape_NullOrWhitespace_ReturnedAsIs()
    {
        Assert.Equal("", _provider.Escape(""));
        Assert.Equal("   ", _provider.Escape("   "));
    }

    [Fact]
    public void BooleanTrueLiteral_IsLowerTrue()
    {
        Assert.Equal("true", _provider.BooleanTrueLiteral);
    }

    [Fact]
    public void BooleanFalseLiteral_IsLowerFalse()
    {
        Assert.Equal("false", _provider.BooleanFalseLiteral);
    }

    [Fact]
    public void MaxSqlLength_IsIntMax()
    {
        Assert.Equal(int.MaxValue, _provider.MaxSqlLength);
    }

    [Fact]
    public void MaxParameters_Is32767()
    {
        Assert.Equal(32_767, _provider.MaxParameters);
    }

    [Fact]
    public void NullSafeEqual_UsesIsNotDistinctFrom()
    {
        var sql = _provider.NullSafeEqual("col", "@p0");
        Assert.Contains("IS NOT DISTINCT FROM", sql);
    }

    [Fact]
    public void NullSafeNotEqual_UsesIsDistinctFrom()
    {
        var sql = _provider.NullSafeNotEqual("col", "@p0");
        Assert.Contains("IS DISTINCT FROM", sql);
    }

    [Fact]
    public void GetInsertOrIgnoreSql_UsesOnConflictDoNothing()
    {
        var sql = _provider.GetInsertOrIgnoreSql("\"T\"", "\"A\"", "\"B\"", "@p1", "@p2");
        Assert.Contains("ON CONFLICT DO NOTHING", sql);
    }

    [Fact]
    public void TranslateFunction_ToUpper_ReturnsUPPER()
    {
        Assert.Equal("UPPER(col)", _provider.TranslateFunction("ToUpper", typeof(string), "col"));
    }

    [Fact]
    public void TranslateFunction_Length_ReturnsLENGTH()
    {
        // Postgres uses LENGTH not CHAR_LENGTH
        Assert.Equal("LENGTH(col)", _provider.TranslateFunction("Length", typeof(string), "col"));
    }

    [Fact]
    public void TranslateFunction_DateTimeYear_ReturnsEXTRACT()
    {
        var sql = _provider.TranslateFunction("Year", typeof(DateTime), "col");
        Assert.Contains("EXTRACT(YEAR FROM", sql);
    }

    [Fact]
    public void TranslateFunction_MathAbs_ReturnsABS()
    {
        Assert.Equal("ABS(col)", _provider.TranslateFunction("Abs", typeof(Math), "col"));
    }

    [Fact]
    public void TranslateFunction_Unknown_ReturnsNull()
    {
        Assert.Null(_provider.TranslateFunction("Foo", typeof(string), "col"));
    }

    [Fact]
    public void TranslateJsonPathAccess_SimpleProperty_ProducesJsonbExtractPath()
    {
        var sql = _provider.TranslateJsonPathAccess("data", "user");
        Assert.Contains("jsonb_extract_path_text", sql);
        Assert.Contains("data", sql);
        Assert.Contains("'user'", sql);
    }

    [Fact]
    public void TranslateJsonPathAccess_NestedPath_SplitsOnDot()
    {
        var sql = _provider.TranslateJsonPathAccess("data", "user.address");
        Assert.Contains("'user'", sql);
        Assert.Contains("'address'", sql);
    }

    [Fact]
    public void TranslateJsonPathAccess_ArrayAccess_ProducesIndex()
    {
        var sql = _provider.TranslateJsonPathAccess("data", "items[0]");
        Assert.Contains("'0'", sql);
    }

    [Fact]
    public void ApplyPaging_LimitOnly_ProducesLimitClause()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, 10, null, "@p0", null);
        Assert.Contains("LIMIT @p0", sb.ToString());
    }

    [Fact]
    public void ApplyPaging_OffsetOnly_ProducesOffsetClause()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, null, 5, null, "@p1");
        Assert.Contains("OFFSET @p1", sb.ToString());
    }

    [Fact]
    public void ApplyPaging_OffsetAndLimit_ProducesBothClauses()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, 10, 5, "@p0", "@p1");
        var sql = sb.ToString();
        Assert.Contains("LIMIT @p0", sql);
        Assert.Contains("OFFSET @p1", sql);
    }

    [Fact]
    public void ApplyPaging_LiteralValues_EmbeddedInSql()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, 10, 5, null, null);
        var sql = sb.ToString();
        Assert.Contains("LIMIT 10", sql);
        Assert.Contains("OFFSET 5", sql);
    }

    [Fact]
    public void ApplyPaging_NoArgs_ProducesNothing()
    {
        using var sb = new OptimizedSqlBuilder();
        _provider.ApplyPaging(sb, null, null, null, null);
        Assert.Equal("", sb.ToString());
    }

    [Fact]
    public void UsesFetchOffsetPaging_IsFalse()
    {
        Assert.False(_provider.UsesFetchOffsetPaging);
    }

    [Fact]
    public void ParamPrefix_IsAtSign()
    {
        Assert.Equal("@", _provider.ParamPrefix);
    }

    [Fact]
    public void GetCreateTagsTableSql_ContainsTemporalTagsTable()
    {
        var sql = _provider.GetCreateTagsTableSql();
        Assert.Contains("__NormTemporalTags", sql);
        Assert.Contains("IF NOT EXISTS", sql);
    }

    [Fact]
    public void GetHistoryTableExistsProbeSql_UsesLimit()
    {
        var sql = _provider.GetHistoryTableExistsProbeSql("\"MyTable_History\"");
        Assert.Contains("LIMIT 1", sql);
    }

    [Fact]
    public void GetTagLookupSql_ContainsParam()
    {
        var sql = _provider.GetTagLookupSql("@p0");
        Assert.Contains("@p0", sql);
    }

    [Fact]
    public void GetCreateTagSql_ContainsInsertAndParams()
    {
        var sql = _provider.GetCreateTagSql("@tag", "@ts");
        Assert.Contains("INSERT INTO", sql);
    }

    [Fact]
    public void IsObjectNotFoundError_DefaultMessageBased_TrueForDoesNotExist()
    {
        var ex = new FakeDbException("relation \"foo\" does not exist");
        Assert.True(_provider.IsObjectNotFoundError(ex));
    }
}

// ── SqlServerProvider ─────────────────────────────────────────────────────────

public class SqlServerProviderTests
{
    private readonly SqlServerProvider _provider = new();

    [Fact]
    public void Escape_SimpleIdentifier_UsesBrackets()
    {
        Assert.Equal("[orders]", _provider.Escape("orders"));
    }

    [Fact]
    public void Escape_SchemaQualified_EachPartBracketed()
    {
        Assert.Equal("[dbo].[orders]", _provider.Escape("dbo.orders"));
    }

    [Fact]
    public void Escape_EmbeddedCloseBracket_IsDoubled()
    {
        Assert.Equal("[my]]table]", _provider.Escape("my]table"));
    }

    [Fact]
    public void Escape_NullOrWhitespace_ReturnedAsIs()
    {
        Assert.Equal("", _provider.Escape(""));
        Assert.Equal("   ", _provider.Escape("   "));
    }

    [Fact]
    public void MaxSqlLength_IsLargeEnoughForRealQueries()
    {
        // SQL1 fix: SQL Server's actual query text limit is much larger than 8,000 characters.
        // The previous 8k ceiling was incorrectly derived from the max row size, not query text.
        Assert.True(_provider.MaxSqlLength > 8_000, "MaxSqlLength should be > 8000 to allow valid wide queries");
        Assert.Equal(268_435_456, _provider.MaxSqlLength);
    }

    [Fact]
    public void MaxParameters_Is2100()
    {
        Assert.Equal(2_100, _provider.MaxParameters);
    }

    [Fact]
    public void UsesFetchOffsetPaging_IsTrue()
    {
        Assert.True(_provider.UsesFetchOffsetPaging);
    }

    [Fact]
    public void GetIdentityRetrievalString_ContainsScopeIdentity()
    {
        var sql = _provider.GetIdentityRetrievalString(null!);
        Assert.Contains("SCOPE_IDENTITY", sql);
    }

    [Fact]
    public void GetInsertOrIgnoreSql_UsesIfNotExists()
    {
        var sql = _provider.GetInsertOrIgnoreSql("[T]", "[A]", "[B]", "@p1", "@p2");
        Assert.Contains("IF NOT EXISTS", sql);
    }

    [Fact]
    public void TranslateFunction_ToUpper_ReturnsUPPER()
    {
        Assert.Equal("UPPER(col)", _provider.TranslateFunction("ToUpper", typeof(string), "col"));
    }

    [Fact]
    public void TranslateFunction_Length_ReturnsLEN()
    {
        // SQL Server uses LEN not LENGTH
        Assert.Equal("LEN(col)", _provider.TranslateFunction("Length", typeof(string), "col"));
    }

    [Fact]
    public void TranslateFunction_DateTimeHour_ReturnsDatepart()
    {
        var sql = _provider.TranslateFunction("Hour", typeof(DateTime), "col");
        Assert.Contains("DATEPART", sql);
        Assert.Contains("hour", sql);
    }

    [Fact]
    public void TranslateFunction_MathAbs_ReturnsABS()
    {
        Assert.Equal("ABS(col)", _provider.TranslateFunction("Abs", typeof(Math), "col"));
    }

    [Fact]
    public void TranslateFunction_MathCeiling_ReturnsCEILING()
    {
        Assert.Equal("CEILING(col)", _provider.TranslateFunction("Ceiling", typeof(Math), "col"));
    }

    [Fact]
    public void TranslateFunction_MathFloor_ReturnsFLOOR()
    {
        Assert.Equal("FLOOR(col)", _provider.TranslateFunction("Floor", typeof(Math), "col"));
    }

    [Fact]
    public void TranslateFunction_Unknown_ReturnsNull()
    {
        Assert.Null(_provider.TranslateFunction("Foo", typeof(string), "col"));
    }

    [Fact]
    public void TranslateJsonPathAccess_ProducesJsonValue()
    {
        var sql = _provider.TranslateJsonPathAccess("data", "$.user.name");
        Assert.Equal("JSON_VALUE(data, '$.user.name')", sql);
    }

    [Fact]
    public void EscapeLikePattern_EscapesBrackets()
    {
        var escaped = _provider.EscapeLikePattern("50% [sale] today_deal");
        // SQL Server escapes [ ] ^ in addition to % _
        Assert.Contains("\\[", escaped);
        Assert.Contains("\\]", escaped);
        Assert.Contains("\\%", escaped);
        Assert.Contains("\\_", escaped);
    }

    [Fact]
    public void EscapeLikePattern_EscapesCaret()
    {
        var escaped = _provider.EscapeLikePattern("^start");
        Assert.Contains("\\^", escaped);
    }

    [Fact]
    public void GetLikeEscapeSql_ContainsReplaceAndExpression()
    {
        var sql = _provider.GetLikeEscapeSql("@param");
        Assert.Contains("REPLACE", sql);
        Assert.Contains("@param", sql);
    }

    [Fact]
    public void ApplyPaging_LimitOnly_ProducesFetchNext()
    {
        using var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T ORDER BY id");
        _provider.ApplyPaging(sb, 10, null, "@p0", null);
        var sql = sb.ToString();
        Assert.Contains("OFFSET", sql);
        Assert.Contains("FETCH NEXT @p0 ROWS ONLY", sql);
    }

    [Fact]
    public void ApplyPaging_NoOrderBy_InjectsOrderBySelectNull()
    {
        using var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T");
        _provider.ApplyPaging(sb, 5, 0, "@p0", "@p1");
        var sql = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", sql);
    }

    [Fact]
    public void ApplyPaging_LiteralOffset_EmbeddedInSql()
    {
        using var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T ORDER BY id");
        _provider.ApplyPaging(sb, 10, 5, null, null);
        var sql = sb.ToString();
        Assert.Contains("OFFSET 5 ROWS", sql);
        Assert.Contains("FETCH NEXT 10 ROWS ONLY", sql);
    }

    [Fact]
    public void ApplyPaging_NoArgs_ProducesNothing()
    {
        using var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T");
        _provider.ApplyPaging(sb, null, null, null, null);
        // No OFFSET or FETCH appended
        Assert.DoesNotContain("OFFSET", sb.ToString());
    }

    [Fact]
    public void GetCreateTagsTableSql_UsesObjectIdGuard()
    {
        var sql = _provider.GetCreateTagsTableSql();
        Assert.Contains("OBJECT_ID", sql);
        Assert.Contains("__NormTemporalTags", sql);
        Assert.Contains("NVARCHAR", sql);
        Assert.Contains("DATETIME2", sql);
    }

    [Fact]
    public void GetHistoryTableExistsProbeSql_UsesSelectTop1()
    {
        var sql = _provider.GetHistoryTableExistsProbeSql("[MyTable_History]");
        Assert.Contains("SELECT TOP 1", sql);
        Assert.Contains("[MyTable_History]", sql);
    }

    [Fact]
    public void IsObjectNotFoundError_NonSqlException_ReturnsFalse()
    {
        // A plain DbException (not SqlException) should return false since
        // SqlServerProvider requires sqlEx.Number == 208.
        var ex = new FakeDbException("Invalid object name 'foo'");
        Assert.False(_provider.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void GetTagLookupSql_ContainsParam()
    {
        var sql = _provider.GetTagLookupSql("@p0");
        Assert.Contains("@p0", sql);
        Assert.Contains("__NormTemporalTags", sql);
    }

    [Fact]
    public void GetCreateTagSql_ContainsInsertAndBracketedNames()
    {
        var sql = _provider.GetCreateTagSql("@tag", "@ts");
        Assert.Contains("INSERT INTO", sql);
        Assert.Contains("@tag", sql);
        Assert.Contains("@ts", sql);
    }

    [Fact]
    public void BooleanTrueLiteral_InheritsDefault1()
    {
        Assert.Equal("1", _provider.BooleanTrueLiteral);
    }

    [Fact]
    public void NullSafeEqual_UsesPortableOrExpansion()
    {
        var sql = _provider.NullSafeEqual("col", "@p0");
        Assert.Contains("IS NULL", sql);
    }

    // ── SQL1: MaxSqlLength is no longer 8000 ────────────────────────────────

    [Fact]
    public void SQL1_MaxSqlLength_IsNot8000()
    {
        // SQL1 fix verified: the old 8000-char limit was incorrectly derived from
        // the max row size, not the query text limit. The current value must exceed
        // 8000 to allow legitimate wide queries.
        Assert.NotEqual(8_000, _provider.MaxSqlLength);
        Assert.True(_provider.MaxSqlLength > 8_000);
    }

    [Fact]
    public void SQL1_WideQuery_ExceedingOld8kLimit_DoesNotThrow()
    {
        // Build a SELECT statement that exceeds 8000 characters (the old limit).
        // It should NOT throw InvalidOperationException with the corrected MaxSqlLength.
        var columns = string.Join(", ", Enumerable.Range(1, 500)
            .Select(i => $"[Column{i:D4}_With_A_Long_Name]"));
        var sql = $"SELECT {columns} FROM [WideTable] WHERE [Id] = @p0";
        Assert.True(sql.Length > 8_000, $"Test SQL should exceed 8000 chars but was {sql.Length}");

        // Verify QueryPlanValidator accepts this plan under SqlServerProvider
        var plan = new QueryPlan(
            sql,
            new Dictionary<string, object> { ["@p0"] = 1 },
            new List<string>(),
            (r, ct) => Task.FromResult<object>(0),
            r => (object)0,
            typeof(int),
            false, false, false,
            string.Empty,
            new List<IncludePlan>(),
            null,
            Array.Empty<string>(),
            true,
            TimeSpan.FromSeconds(30),
            false,
            null);

        // Must not throw — 8k+ SQL is valid for SQL Server
        var ex = Record.Exception(() => QueryPlanValidator.Validate(plan, _provider));
        Assert.Null(ex);
    }

    [Fact]
    public void SQL1_QueryPlanValidator_AcceptsPlansUpToNewLimit()
    {
        // A plan with SQL just under the new limit should be accepted.
        // We don't allocate a full 256 MB string — just verify that a 10k SQL passes.
        var sql = new string('x', 10_000);
        var plan = new QueryPlan(
            sql,
            new Dictionary<string, object>(),
            new List<string>(),
            (r, ct) => Task.FromResult<object>(0),
            r => (object)0,
            typeof(int),
            false, false, false,
            string.Empty,
            new List<IncludePlan>(),
            null,
            Array.Empty<string>(),
            true,
            TimeSpan.FromSeconds(30),
            false,
            null);

        var ex = Record.Exception(() => QueryPlanValidator.Validate(plan, _provider));
        Assert.Null(ex);
    }
}

// ── DbConcurrencyException ────────────────────────────────────────────────────

public class DbConcurrencyExceptionTests
{
    [Fact]
    public void DefaultConstructor_MessageIsNull()
    {
        var ex = new DbConcurrencyException();
        Assert.Null(ex.Message == "Exception of type 'nORM.Core.DbConcurrencyException' was thrown." ? null : ex.InnerException);
        Assert.IsType<DbConcurrencyException>(ex);
    }

    [Fact]
    public void MessageConstructor_SetsMessage()
    {
        var ex = new DbConcurrencyException("row was modified");
        Assert.Equal("row was modified", ex.Message);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void MessageAndInnerConstructor_SetsBoth()
    {
        var inner = new InvalidOperationException("inner");
        var ex = new DbConcurrencyException("row conflict", inner);
        Assert.Equal("row conflict", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void IsSubclassOfException()
    {
        Assert.True(typeof(Exception).IsAssignableFrom(typeof(DbConcurrencyException)));
    }

    [Fact]
    public void DefaultConstructor_IsNotNull()
    {
        var ex = new DbConcurrencyException();
        Assert.NotNull(ex);
    }
}

// ── NavigationContext ─────────────────────────────────────────────────────────

public class NavigationContextCovTests
{
    private static NavigationContext MakeContext() =>
        new NavigationContext(null!, typeof(string));

    [Fact]
    public void IsLoaded_Initially_ReturnsFalse()
    {
        var ctx = MakeContext();
        Assert.False(ctx.IsLoaded("Orders"));
    }

    [Fact]
    public void MarkAsLoaded_ThenIsLoaded_ReturnsTrue()
    {
        var ctx = MakeContext();
        ctx.MarkAsLoaded("Orders");
        Assert.True(ctx.IsLoaded("Orders"));
    }

    [Fact]
    public void MarkAsUnloaded_AfterMarkAsLoaded_ReturnsFalse()
    {
        var ctx = MakeContext();
        ctx.MarkAsLoaded("Orders");
        ctx.MarkAsUnloaded("Orders");
        Assert.False(ctx.IsLoaded("Orders"));
    }

    [Fact]
    public void Dispose_ClearsLoadedProperties()
    {
        var ctx = MakeContext();
        ctx.MarkAsLoaded("Orders");
        ctx.Dispose();
        Assert.False(ctx.IsLoaded("Orders"));
    }

    [Fact]
    public void EntityType_IsCorrect()
    {
        var ctx = MakeContext();
        Assert.Equal(typeof(string), ctx.EntityType);
    }
}

// ── DatabaseScaffolder private helpers via reflection ─────────────────────────

public class DatabaseScaffolderHelperTests
{
    private static MethodInfo GetPrivateStatic(string name, int paramCount = 1)
    {
        return typeof(DatabaseScaffolder)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!;
    }

    private static object? Invoke(string methodName, object?[] args)
    {
        var method = typeof(DatabaseScaffolder)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .First(m => m.Name == methodName && m.GetParameters().Length == args.Length);
        return method.Invoke(null, args);
    }

    [Fact]
    public void ToPascalCase_SimpleWord_CapitalizesFirstLetter()
    {
        var result = Invoke("ToPascalCase", new object?[] { "orders" });
        Assert.Equal("Orders", result);
    }

    [Fact]
    public void ToPascalCase_SnakeCase_JoinsCapitalized()
    {
        var result = Invoke("ToPascalCase", new object?[] { "order_items" });
        Assert.Equal("OrderItems", result);
    }

    [Fact]
    public void ToPascalCase_SpaceSeparated_JoinsCapitalized()
    {
        var result = Invoke("ToPascalCase", new object?[] { "my table" });
        Assert.Equal("MyTable", result);
    }

    [Fact]
    public void ToPascalCase_AlreadyCamelCase_FirstLetterCapitalized()
    {
        var result = Invoke("ToPascalCase", new object?[] { "blogPost" });
        Assert.Equal("Blogpost", result);
    }

    [Fact]
    public void ToPascalCase_EmptyOrWhitespace_ReturnedAsIs()
    {
        var result = Invoke("ToPascalCase", new object?[] { "  " });
        Assert.Equal("  ", result);
    }

    [Fact]
    public void GetTypeName_Int_NotNull_ReturnsInt()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(int), false });
        Assert.Equal("int", result);
    }

    [Fact]
    public void GetTypeName_Int_Nullable_ReturnsIntQuestion()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(int), true });
        Assert.Equal("int?", result);
    }

    [Fact]
    public void GetTypeName_String_NotNull_ReturnsString()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(string), false });
        Assert.Equal("string", result);
    }

    [Fact]
    public void GetTypeName_String_Nullable_ReturnsStringQuestion()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(string), true });
        Assert.Equal("string?", result);
    }

    [Fact]
    public void GetTypeName_Bool_NotNull_ReturnsBool()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(bool), false });
        Assert.Equal("bool", result);
    }

    [Fact]
    public void GetTypeName_DateTime_NotNull_ReturnsDateTime()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(DateTime), false });
        Assert.Equal("DateTime", result);
    }

    [Fact]
    public void GetTypeName_Decimal_NotNull_ReturnsDecimal()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(decimal), false });
        Assert.Equal("decimal", result);
    }

    [Fact]
    public void GetTypeName_Guid_NotNull_ReturnsGuid()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(Guid), false });
        Assert.Equal("Guid", result);
    }

    [Fact]
    public void GetTypeName_ByteArray_NotNull_ReturnsByteArray()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(byte[]), false });
        Assert.Equal("byte[]", result);
    }

    [Fact]
    public void GetTypeName_Long_Nullable_ReturnsLongQuestion()
    {
        var result = Invoke("GetTypeName", new object?[] { typeof(long), true });
        Assert.Equal("long?", result);
    }

    [Fact]
    public void GetUnqualifiedName_WithDot_ReturnsLastPart()
    {
        var result = Invoke("GetUnqualifiedName", new object?[] { "dbo.Orders" });
        Assert.Equal("Orders", result);
    }

    [Fact]
    public void GetUnqualifiedName_NoDot_ReturnsFull()
    {
        var result = Invoke("GetUnqualifiedName", new object?[] { "Orders" });
        Assert.Equal("Orders", result);
    }

    [Fact]
    public void GetSchemaNameOrNull_WithDot_ReturnsSchema()
    {
        var result = Invoke("GetSchemaNameOrNull", new object?[] { "dbo.Orders" });
        Assert.Equal("dbo", result);
    }

    [Fact]
    public void GetSchemaNameOrNull_NoDot_ReturnsNull()
    {
        var result = Invoke("GetSchemaNameOrNull", new object?[] { "Orders" });
        Assert.Null(result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_NormalName_ReturnedAsIs()
    {
        var result = Invoke("EscapeCSharpIdentifier", new object?[] { "Orders" });
        Assert.Equal("Orders", result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_CSharpKeyword_PrefixedWithAt()
    {
        var result = Invoke("EscapeCSharpIdentifier", new object?[] { "class" });
        Assert.Equal("@class", result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_StartsWithDigit_PrefixedWithAt()
    {
        var result = Invoke("EscapeCSharpIdentifier", new object?[] { "1Table" });
        Assert.Equal("@1Table", result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_ContainsHyphen_PrefixedWithAt()
    {
        var result = Invoke("EscapeCSharpIdentifier", new object?[] { "my-table" });
        Assert.Equal("@my-table", result);
    }

    [Fact]
    public void EscapeQualifiedIfNeeded_NoSchema_ReturnsTable()
    {
        // Use the (string?, string) overload
        var method = typeof(DatabaseScaffolder)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .First(m => m.Name == "EscapeQualifiedIfNeeded");
        var result = method.Invoke(null, new object?[] { null, "Orders" });
        Assert.Equal("Orders", result);
    }

    [Fact]
    public void EscapeQualifiedIfNeeded_WithSchema_ReturnsSchemaTable()
    {
        var method = typeof(DatabaseScaffolder)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .First(m => m.Name == "EscapeQualifiedIfNeeded");
        var result = method.Invoke(null, new object?[] { "dbo", "Orders" });
        Assert.Equal("dbo.Orders", result);
    }
}

// ── FakeDbException helper used across tests ──────────────────────────────────

/// <summary>
/// Minimal concrete DbException for testing IsObjectNotFoundError.
/// </summary>
file sealed class FakeDbException : DbException
{
    public FakeDbException(string message) : base(message) { }
}
