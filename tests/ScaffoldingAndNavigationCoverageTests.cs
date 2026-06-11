// Tests for DatabaseScaffolder private helpers.

#nullable enable

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

// Entity types used by scaffolding coverage tests.

[Table("SchemaWidget", Schema = "main")]
[Xunit.Trait("Category", "Fast")]
public class SanSchemaWidget
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Table("SAN_ComputedGenerated")]
public class SanComputedGenerated
{
    [Key]
    public int Id { get; set; }

    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
    public int Total { get; set; }
}

[Table("SAN_RowVersionGenerated")]
public class SanRowVersionGenerated
{
    [Key]
    public int Id { get; set; }

    [Timestamp]
    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
    public byte[] RowVersion { get; set; } = Array.Empty<byte>();
}

[Table("SAN_ReadOnlyReport")]
[ReadOnlyEntity]
public class SanReadOnlyReport
{
    public string ExternalId { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
}

[Table("SchemaParent", Schema = "aux")]
[Xunit.Trait("Category", "Fast")]
public class SanSchemaParent
{
    [Key]
    public int Id { get; set; }
    public List<SanSchemaChild> Children { get; set; } = new();
}

[Table("SchemaChild", Schema = "aux")]
[Xunit.Trait("Category", "Fast")]
public class SanSchemaChild
{
    [Key]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public int Amount { get; set; }
}

[Xunit.Trait("Category", "Fast")]
public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void ToPascalCase_UnderscoreSeparated_ReturnsPascalCase()
    {
        var result = InvokeToPascalCase("user_name");
        Assert.Equal("UserName", result);
    }

    [Fact]
    public void ToPascalCase_AllUpperWithUnderscore_ReturnsFirstLetterUpper()
    {
        var result = InvokeToPascalCase("CUSTOMER_ID");
        // Each segment: C+ustomer = "Customer", I+d = "Id"
        Assert.Equal("CustomerId", result);
    }

    [Theory]
    [InlineData("length(Name) VIRTUAL", "length(Name)", false)]
    [InlineData("length(Name) STORED", "length(Name)", true)]
    [InlineData("([Total]+[Tax]) PERSISTED", "[Total]+[Tax]", true)]
    [InlineData("GENERATED ALWAYS AS (Price * Quantity) STORED", "Price * Quantity", true)]
    [InlineData("GENERATED ALWAYS AS (Price * Quantity) VIRTUAL", "Price * Quantity", false)]
    [InlineData("' STORED' VIRTUAL", "' STORED'", false)]
    [InlineData("GENERATED ALWAYS AS (' STORED') VIRTUAL", "' STORED'", false)]
    [InlineData("CONCAT(Name, ' PERSISTED')", "CONCAT(Name, ' PERSISTED')", false)]
    [InlineData("PERSISTED", "PERSISTED", false)]
    public void NormalizeScaffoldComputedSql_StripsStorageTokensAndPreservesStoredFlag(
        string raw,
        string expectedSql,
        bool expectedStored)
    {
        var (sql, stored) = InvokeNormalizeScaffoldComputedSql(raw);

        Assert.Equal(expectedSql, sql);
        Assert.Equal(expectedStored, stored);
    }

    [Theory]
    [InlineData("(length(\"Paren)Name\") + 1)", "length(\"Paren)Name\") + 1")]
    [InlineData("([Paren)Name] + 1) PERSISTED", "[Paren)Name] + 1")]
    [InlineData("(length(`Paren)Name`) + 1) STORED", "length(`Paren)Name`) + 1")]
    public void NormalizeScaffoldComputedSql_StripsOuterParenthesesAroundQuotedIdentifiers(
        string raw,
        string expectedSql)
    {
        var (sql, _) = InvokeNormalizeScaffoldComputedSql(raw);

        Assert.Equal(expectedSql, sql);
    }

    [Fact]
    public void DynamicExtractSqliteGeneratedColumns_UsesSharedQuoteAwareParser()
    {
        var columns = InvokeDynamicExtractSqliteGeneratedColumns("""
            CREATE TABLE "Metrics" (
                "Note" TEXT DEFAULT 'GENERATED ALWAYS AS (ignored) STORED',
                "Total" INTEGER GENERATED ALWAYS AS (([Quantity] * [Price])) PERSISTED,
                "Virtual" TEXT GENERATED ALWAYS AS ('PERSISTED') VIRTUAL
            )
            """);

        Assert.False(columns.ContainsKey("Note"));
        Assert.Equal("[Quantity] * [Price]", columns["Total"].Sql);
        Assert.True(columns["Total"].Stored);
        Assert.Equal("'PERSISTED'", columns["Virtual"].Sql);
        Assert.False(columns["Virtual"].Stored);
    }

    [Theory]
    [InlineData("CHECK ((length(\"Paren)Name\") > 0))", "length(\"Paren)Name\") > 0")]
    [InlineData("CHECK (([Paren)Name] > 0))", "[Paren)Name] > 0")]
    [InlineData("CHECK (CHECKSUM([Name]) > 0)", "CHECKSUM([Name]) > 0")]
    [InlineData("CHECKSUM([Name]) > 0", "CHECKSUM([Name]) > 0")]
    public void NormalizeScaffoldCheckSql_StripsOuterParenthesesAroundQuotedIdentifiers(
        string raw,
        string expectedSql)
    {
        var sql = InvokeNormalizeScaffoldCheckSql(raw);

        Assert.Equal(expectedSql, sql);
    }

    [Theory]
    [InlineData("'active'::text")]
    [InlineData("'draft'::character varying")]
    [InlineData("'{}'::jsonb")]
    [InlineData("'00000000-0000-0000-0000-000000000000'::uuid")]
    [InlineData("42::integer")]
    [InlineData("3.14::numeric(10, 2)")]
    [InlineData("true::boolean")]
    [InlineData("now()::timestamp without time zone")]
    [InlineData("CURRENT_TIMESTAMP::timestamp with time zone")]
    [InlineData("now() AT TIME ZONE 'utc'")]
    [InlineData("CURRENT_TIMESTAMP(6) AT TIME ZONE 'utc'::text")]
    [InlineData("timezone('utc'::text, now())")]
    public void NormalizeDefaultSql_StaticAndDynamic_AcceptSafePostgresCasts(string raw)
    {
        var staticResult = InvokeTryNormalizeScaffoldDefaultSql(raw);
        var dynamicResult = InvokeTryNormalizeDynamicDefaultSql(raw);

        Assert.True(staticResult.Normalized);
        Assert.Equal(raw, staticResult.Sql);
        Assert.True(dynamicResult.Normalized);
        Assert.Equal(raw, dynamicResult.Sql);
    }

    [Theory]
    [InlineData("'active'::text; DROP TABLE Users")]
    [InlineData("'active'::text -- comment")]
    [InlineData("0::integer /* comment */")]
    [InlineData("now()::timestamp without time zone; DELETE FROM Users")]
    [InlineData("now() AT TIME ZONE current_user")]
    [InlineData("timezone('utc', unsafe())")]
    [InlineData("timezone('utc', now()); DROP TABLE Users")]
    [InlineData("'active'::\"quoted\"")]
    public void NormalizeDefaultSql_StaticAndDynamic_RejectUnsafePostgresCasts(string raw)
    {
        Assert.False(InvokeTryNormalizeScaffoldDefaultSql(raw).Normalized);
        Assert.False(InvokeTryNormalizeDynamicDefaultSql(raw).Normalized);
    }

    [Theory]
    [InlineData("IDENTITY(1000,25)", true, 1000L, 25L)]
    [InlineData(" IDENTITY ( -5 , 2 ) NOT FOR REPLICATION", true, -5L, 2L)]
    [InlineData("GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)", false, 0L, 0L)]
    [InlineData("sequence(1000,25)", false, 0L, 0L)]
    [InlineData("IDENTITY_COMMENT(1000,25)", false, 0L, 0L)]
    public void TryParseIdentityOptions_OnlyAcceptsSqlServerIdentityMetadata(
        string detail,
        bool expectedParsed,
        long expectedSeed,
        long expectedIncrement)
    {
        var (parsed, seed, increment) = InvokeTryParseIdentityOptions(detail);

        Assert.Equal(expectedParsed, parsed);
        Assert.Equal(expectedSeed, seed);
        Assert.Equal(expectedIncrement, increment);
    }

    [Fact]
    public void ToPascalCase_SpaceSeparated_ReturnsPascalCase()
    {
        var result = InvokeToPascalCase("first name");
        Assert.Equal("FirstName", result);
    }

    [Fact]
    public void ToPascalCase_SingleLower_CapitalizesFirst()
    {
        var result = InvokeToPascalCase("id");
        Assert.Equal("Id", result);
    }

    [Fact]
    public void ToPascalCase_AlreadyPascal_RoundsTrip()
    {
        // "User" → one segment → "User"
        var result = InvokeToPascalCase("User");
        Assert.Equal("User", result);
    }

    [Fact]
    public void ToPascalCase_WhitespaceOnly_ReturnsInput()
    {
        // Implementation: IsNullOrWhiteSpace → return name as-is
        var result = InvokeToPascalCase("   ");
        Assert.Equal("   ", result);
    }

    [Fact]
    public void ToPascalCase_MultipleUnderscores_RemovesAll()
    {
        var result = InvokeToPascalCase("order_line_item");
        Assert.Equal("OrderLineItem", result);
    }

    [Fact]
    public void ToPascalCase_SingleChar_CapitalizesIt()
    {
        var result = InvokeToPascalCase("x");
        Assert.Equal("X", result);
    }

    [Fact]
    public void ToPascalCase_CamelCase_PreservesInnerWordCapital()
    {
        var result = InvokeToPascalCase("blogPost");
        Assert.Equal("BlogPost", result);
    }

    [Fact]
    public void ToPascalCase_InvalidSeparators_BecomeWordBoundaries()
    {
        var result = InvokeToPascalCase("bad-table.name");
        Assert.Equal("BadTableName", result);
    }

    // ── EscapeCSharpIdentifier ──────────────────────────────────────────────

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_class_PrependsAt()
    {
        Assert.Equal("@class", InvokeEscapeCSharpIdentifier("class"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_string_PrependsAt()
    {
        Assert.Equal("@string", InvokeEscapeCSharpIdentifier("string"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_int_PrependsAt()
    {
        Assert.Equal("@int", InvokeEscapeCSharpIdentifier("int"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_bool_PrependsAt()
    {
        Assert.Equal("@bool", InvokeEscapeCSharpIdentifier("bool"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_return_PrependsAt()
    {
        Assert.Equal("@return", InvokeEscapeCSharpIdentifier("return"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_ValidIdentifier_ReturnsUnchanged()
    {
        Assert.Equal("MyField", InvokeEscapeCSharpIdentifier("MyField"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_StartsWithDigit_PrefixesUnderscore()
    {
        Assert.Equal("_123abc", InvokeEscapeCSharpIdentifier("123abc"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_ContainsHyphen_ReplacesWithUnderscore()
    {
        // Hyphens are invalid in C# identifiers
        var result = InvokeEscapeCSharpIdentifier("my-field");
        Assert.Equal("my_field", result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_EmptyString_ReturnsFallbackIdentifier()
    {
        Assert.Equal("_", InvokeEscapeCSharpIdentifier(""));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Underscore_IsValid()
    {
        // Underscore alone is a valid C# identifier
        Assert.Equal("_", InvokeEscapeCSharpIdentifier("_"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_var_PrependsAt()
    {
        Assert.Equal("@var", InvokeEscapeCSharpIdentifier("var"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_AlreadyEscapedIdentifier_ReturnsUnchanged()
    {
        Assert.Equal("@class", InvokeEscapeCSharpIdentifier("@class"));
        Assert.Equal("@record", InvokeEscapeCSharpIdentifier("@record"));
        Assert.Equal("@class", InvokeDynamicEscapeCSharpIdentifier("@class"));
        Assert.Equal("@record", InvokeDynamicEscapeCSharpIdentifier("@record"));
    }

    [Fact]
    public void BuildColumnPropertyNameMap_Reserves_Enclosing_Entity_Name()
    {
        var method = GetMethod(
            "BuildColumnPropertyNameMap",
            new[]
            {
                typeof(IReadOnlyDictionary<string, IReadOnlyList<string>>),
                typeof(IReadOnlyDictionary<string, string>),
                typeof(bool)
            });
        var orderedColumns = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["User"] = new[] { "User", "Id" }
        };
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["User"] = "User"
        };

        var result = (IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>)method.Invoke(
            null,
            new object[] { orderedColumns, entityByTable, false })!;

        Assert.Equal("User2", result["User"]["User"]);
        Assert.Equal("Id", result["User"]["Id"]);
    }

    [Fact]
    public void EscapeCSharpIdentifier_InvalidVerbatimIdentifier_IsSanitized()
    {
        Assert.Equal("_bad_name", InvokeEscapeCSharpIdentifier("@bad-name"));
        Assert.Equal("_bad_name", InvokeDynamicEscapeCSharpIdentifier("@bad-name"));
    }

    // ── GetTypeName ─────────────────────────────────────────────────────────

    [Fact]
    public void GetTypeName_Int_NotNull_ReturnsInt()
    {
        Assert.Equal("int", InvokeGetTypeName(typeof(int), false));
    }

    [Fact]
    public void GetTypeName_Int_AllowNull_ReturnsIntQuestion()
    {
        Assert.Equal("int?", InvokeGetTypeName(typeof(int), true));
    }

    [Fact]
    public void GetTypeName_String_NotNull_ReturnsString()
    {
        Assert.Equal("string", InvokeGetTypeName(typeof(string), false));
    }

    [Fact]
    public void GetTypeName_String_AllowNull_ReturnsStringQuestion()
    {
        Assert.Equal("string?", InvokeGetTypeName(typeof(string), true));
    }

    [Fact]
    public void GetTypeName_Bool_NotNull_ReturnsBool()
    {
        Assert.Equal("bool", InvokeGetTypeName(typeof(bool), false));
    }

    [Fact]
    public void GetTypeName_Bool_AllowNull_ReturnsBoolQuestion()
    {
        Assert.Equal("bool?", InvokeGetTypeName(typeof(bool), true));
    }

    [Fact]
    public void GetTypeName_DateTime_NotNull_ReturnsDateTime()
    {
        Assert.Equal("DateTime", InvokeGetTypeName(typeof(DateTime), false));
    }

    [Fact]
    public void GetTypeName_DateTime_AllowNull_ReturnsDateTimeQuestion()
    {
        Assert.Equal("DateTime?", InvokeGetTypeName(typeof(DateTime), true));
    }

    [Fact]
    public void GetTypeName_Guid_NotNull_ReturnsGuid()
    {
        Assert.Equal("Guid", InvokeGetTypeName(typeof(Guid), false));
    }

    [Fact]
    public void GetTypeName_Guid_AllowNull_ReturnsGuidQuestion()
    {
        Assert.Equal("Guid?", InvokeGetTypeName(typeof(Guid), true));
    }

    [Fact]
    public void GetTypeName_Long_NotNull_ReturnsLong()
    {
        Assert.Equal("long", InvokeGetTypeName(typeof(long), false));
    }

    [Fact]
    public void GetTypeName_Decimal_NotNull_ReturnsDecimal()
    {
        Assert.Equal("decimal", InvokeGetTypeName(typeof(decimal), false));
    }

    [Theory]
    [InlineData("decimal(28,6)", true, 28, 6)]
    [InlineData("decimal(28, 6)", true, 28, 6)]
    [InlineData("numeric(19,4)", true, 19, 4)]
    [InlineData("numeric(10)", true, 10, null)]
    [InlineData("numeric ( 19 , 4 )", true, 19, 4)]
    [InlineData("DOMAIN (public.price_amount -> numeric(12, 3))", true, 12, 3)]
    [InlineData("varchar(20)", false, 0, null)]
    [InlineData("mydecimal(18,2)", false, 0, null)]
    [InlineData("decimal(18,,2)", false, 0, null)]
    [InlineData("decimal(18,)", false, 0, null)]
    [InlineData("decimal(,2)", false, 0, null)]
    [InlineData("decimal(4,5)", false, 0, null)]
    public void TryParseDecimalPrecision_ParsesProviderNumericDeclarations(string typeName, bool expected, int expectedPrecision, int? expectedScale)
    {
        var method = GetMethod("TryParseDecimalPrecision", new[] { typeof(string), typeof(int).MakeByRefType(), typeof(int?).MakeByRefType() });
        var args = new object?[] { typeName, 0, null };

        var result = (bool)method.Invoke(null, args)!;

        Assert.Equal(expected, result);
        Assert.Equal(expectedPrecision, (int)args[1]!);
        Assert.Equal(expectedScale, args[2] is int scale ? scale : null);
    }

    [Theory]
    [InlineData("decimal(18,2)", 18, 2)]
    [InlineData("decimal(18)", 18, null)]
    [InlineData("numeric(18,0)", 18, 0)]
    [InlineData("decimal(18,,2)", null, null)]
    [InlineData("decimal(18,)", null, null)]
    [InlineData("decimal(,2)", null, null)]
    [InlineData("decimal(4,5)", null, null)]
    public void GetRoutineParameterPrecisionScale_RejectsMalformedPrecisionScale(string typeName, int? expectedPrecision, int? expectedScale)
    {
        var method = GetMethod("GetRoutineParameterPrecisionScale", new[] { typeof(string) });

        var result = ((byte? Precision, byte? Scale))method.Invoke(null, new object?[] { typeName })!;
        var actualPrecision = result.Precision.HasValue ? (int?)result.Precision.Value : null;
        var actualScale = result.Scale.HasValue ? (int?)result.Scale.Value : null;

        Assert.Equal(expectedPrecision, actualPrecision);
        Assert.Equal(expectedScale, actualScale);
    }

    [Fact]
    public void GetTypeName_Double_NotNull_ReturnsDouble()
    {
        Assert.Equal("double", InvokeGetTypeName(typeof(double), false));
    }

    [Fact]
    public void GetTypeName_Float_NotNull_ReturnsFloat()
    {
        Assert.Equal("float", InvokeGetTypeName(typeof(float), false));
    }

    [Fact]
    public void GetTypeName_Short_NotNull_ReturnsFallbackName()
    {
        // short IS in the switch (mapped to "short")
        var result = InvokeGetTypeName(typeof(short), false);
        Assert.Equal("short", result);
    }

    [Fact]
    public void GetTypeName_ByteArray_NotNull_ReturnsByteArray()
    {
        Assert.Equal("byte[]", InvokeGetTypeName(typeof(byte[]), false));
    }

    [Fact]
    public void GetTypeName_ByteArray_AllowNull_ReturnsByteArrayQuestion()
    {
        Assert.Equal("byte[]?", InvokeGetTypeName(typeof(byte[]), true));
    }

    [Fact]
    public void GetTypeName_ScalarArray_ReturnsCSharpArrayName()
    {
        Assert.Equal("int[]", InvokeGetTypeName(typeof(int[]), false));
        Assert.Equal("Guid[]?", InvokeGetTypeName(typeof(Guid[]), true));
    }

    // ── GetUnqualifiedName ──────────────────────────────────────────────────

    [Theory]
    [InlineData(typeof(string), 64, 64)]
    [InlineData(typeof(byte[]), 32, 32)]
    [InlineData(typeof(int), 32, null)]
    [InlineData(typeof(string), 0, null)]
    [InlineData(typeof(string), int.MaxValue, null)]
    [InlineData(typeof(string), 1073741823, null)]
    public void GetScaffoldMaxLength_StaticAndDynamic_UseBoundedStringAndBinarySizes(Type clrType, int columnSize, int? expected)
    {
        Assert.Equal(expected, InvokeGetScaffoldMaxLength(clrType, columnSize));
        Assert.Equal(expected, InvokeDynamicGetScaffoldMaxLength(clrType, columnSize));
    }

    [Fact]
    public void DynamicBuildType_DecimalPrecision_EmitsColumnTypeName()
    {
        var type = InvokeDynamicBuildTypeWithDecimalPrecision(28, 6);
        var amount = type.GetProperty("Amount")!;
        var column = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());

        Assert.Equal(typeof(decimal), amount.PropertyType);
        Assert.Equal("Amount", column.Name);
        Assert.Equal("decimal(28,6)", column.TypeName);
    }

    [Fact]
    public void DynamicBuildType_DecimalPrecisionWithoutScale_EmitsPrecisionOnlyColumnTypeName()
    {
        var type = InvokeDynamicBuildTypeWithDecimalPrecision(10, null);
        var amount = type.GetProperty("Amount")!;
        var column = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());

        Assert.Equal("decimal(10)", column.TypeName);
    }

    [Fact]
    public void DynamicSchemaDescriptor_DecimalPrecision_AffectsCacheIdentity()
    {
        var first = InvokeDynamicSchemaDescriptorWithDecimalPrecision(18, 2);
        var second = InvokeDynamicSchemaDescriptorWithDecimalPrecision(28, 6);

        Assert.NotEqual(first, second);
        Assert.Contains("decimal(18,2)", first, StringComparison.Ordinal);
        Assert.Contains("decimal(28,6)", second, StringComparison.Ordinal);
    }

    [Fact]
    public void DynamicMySqlIdentityProbe_UsesSchemaQualifiedCatalogWhenProvided()
    {
        var (sql, parameters) = InvokeDynamicMySqlMetadataProbe("GetIdentityColumns");

        Assert.Contains("table_schema = COALESCE(@schemaName, DATABASE())", sql, StringComparison.Ordinal);
        Assert.Equal("tenant_catalog", parameters["@schemaName"]);
        Assert.Equal("Orders", parameters["@tableName"]);
    }

    [Fact]
    public void DynamicMySqlPrimaryKeyProbe_UsesSchemaQualifiedCatalogWhenProvided()
    {
        var (sql, parameters) = InvokeDynamicMySqlMetadataProbe("GetPrimaryKeyOrdinals");

        Assert.Contains("table_schema = COALESCE(@schemaName, DATABASE())", sql, StringComparison.Ordinal);
        Assert.Equal("tenant_catalog", parameters["@schemaName"]);
        Assert.Equal("Orders", parameters["@tableName"]);
    }

    [Fact]
    public void DynamicMySqlSetWriteBlockingProbe_UsesColumnTypeParserInput()
    {
        var (sql, parameters) = InvokeDynamicMySqlMetadataProbe("HasWriteBlockingMySqlSetColumns");

        Assert.Contains("column_type AS ColumnType", sql, StringComparison.Ordinal);
        Assert.Contains("data_type = 'set'", sql, StringComparison.Ordinal);
        Assert.Contains("table_schema = COALESCE(@schemaName, DATABASE())", sql, StringComparison.Ordinal);
        Assert.Equal("tenant_catalog", parameters["@schemaName"]);
        Assert.Equal("Orders", parameters["@tableName"]);
    }

    [Theory]
    [InlineData("set('read','write','admin')", false)]
    [InlineData("set('read', 'write')", false)]
    [InlineData("set('a','b','c','d','e','f','g','h')", false)]
    [InlineData("set('a','b','c','d','e','f','g','h','i')", true)]
    [InlineData("set('read,write','admin')", true)]
    [InlineData("set('read','read')", true)]
    [InlineData("set('read' 'write')", true)]
    [InlineData("set('read',)", true)]
    [InlineData("set(,'read')", true)]
    public void DynamicMySqlWriteBlockingProviderSpecificColumns_MarksUnsafeSetShapes(string columnType, bool expected)
    {
        var connection = new DynamicMySqlMetadataProbeConnection(columnType);

        Assert.Equal(expected, InvokeDynamicHasWriteBlockingProviderSpecificColumns(connection));
        Assert.Contains("data_type = 'set'", connection.LastCommandText, StringComparison.Ordinal);
        Assert.Equal("tenant_catalog", connection.LastParameters["@schemaName"]);
        Assert.Equal("Orders", connection.LastParameters["@tableName"]);
    }

    [Fact]
    public void DynamicSqlServerUnqualifiedResolver_UsesUniqueCatalogSchema()
    {
        var connection = new DynamicSqlConnectionSchemaProbeConnection("sales");

        var schema = InvokeResolveUniqueUnqualifiedSchema(connection, "Orders");

        Assert.Equal("sales", schema);
        Assert.Contains("sys.objects", connection.LastCommandText, StringComparison.Ordinal);
        Assert.Equal("Orders", connection.LastParameters["@tableName"]);
    }

    [Fact]
    public void DynamicPostgresUnqualifiedResolver_UsesUniqueCatalogSchema()
    {
        var connection = new DynamicNpgsqlSchemaProbeConnection("inventory");

        var schema = InvokeResolveUniqueUnqualifiedSchema(connection, "Products");

        Assert.Equal("inventory", schema);
        Assert.Contains("information_schema.tables", connection.LastCommandText, StringComparison.Ordinal);
        Assert.Equal("Products", connection.LastParameters["@tableName"]);
    }

    [Fact]
    public void DynamicSqlServerUnqualifiedResolver_ThrowsWhenCatalogSchemaAmbiguous()
    {
        var connection = new DynamicSqlConnectionSchemaProbeConnection("dbo", "sales");

        var ex = Assert.Throws<TargetInvocationException>(
            () => InvokeResolveUniqueUnqualifiedSchema(connection, "Orders"));
        var configurationException = Assert.IsType<NormConfigurationException>(ex.InnerException);

        Assert.Contains("'dbo'", configurationException.Message, StringComparison.Ordinal);
        Assert.Contains("'sales'", configurationException.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DynamicPostgresUnqualifiedResolver_ThrowsWhenCatalogSchemaAmbiguous()
    {
        var connection = new DynamicNpgsqlSchemaProbeConnection("inventory", "reporting");

        var ex = Assert.Throws<TargetInvocationException>(
            () => InvokeResolveUniqueUnqualifiedSchema(connection, "Products"));
        var configurationException = Assert.IsType<NormConfigurationException>(ex.InnerException);

        Assert.Contains("'inventory'", configurationException.Message, StringComparison.Ordinal);
        Assert.Contains("'reporting'", configurationException.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DynamicMySqlUnqualifiedResolver_KeepsCurrentDatabaseSemantics()
    {
        var connection = new DynamicMySqlMetadataProbeConnection();

        var schema = InvokeResolveUniqueUnqualifiedSchema(connection, "Orders");

        Assert.Null(schema);
        Assert.Equal(string.Empty, connection.LastCommandText);
    }

    [Theory]
    [InlineData("tinyint(3) unsigned", typeof(byte))]
    [InlineData("smallint(5) unsigned", typeof(ushort))]
    [InlineData("mediumint(8) unsigned", typeof(uint))]
    [InlineData("int(10) unsigned", typeof(uint))]
    [InlineData("integer(10) unsigned", typeof(uint))]
    [InlineData("bigint(20) unsigned", typeof(ulong))]
    [InlineData("int unsigned", typeof(uint))]
    public void TryMapMySqlUnsignedType_StaticAndDynamic_IgnoreDisplayWidth(string detail, Type expected)
    {
        var staticResult = InvokeTryMapMySqlUnsignedType(typeof(DatabaseScaffolder), detail);
        var dynamicResult = InvokeTryMapMySqlUnsignedType(typeof(DynamicEntityTypeGenerator), detail);

        Assert.True(staticResult.Mapped);
        Assert.True(dynamicResult.Mapped);
        Assert.Equal(expected, staticResult.Type);
        Assert.Equal(expected, dynamicResult.Type);
    }

    [Theory]
    [InlineData("user-defined type (dbo.EmailAddress -> nvarchar(320))", "nvarchar(320)", typeof(string))]
    [InlineData("user-defined type (dbo.MoneyAmount -> decimal(18,4))", "decimal(18,4)", typeof(decimal))]
    [InlineData("user-defined type (dbo.TokenBytes -> varbinary(64))", "varbinary(64)", typeof(byte[]))]
    [InlineData("user-defined type (dbo.ExternalToken -> uniqueidentifier)", "uniqueidentifier", typeof(Guid))]
    [InlineData("user-defined type (dbo.CreatedOn -> datetimeoffset)", "datetimeoffset", typeof(DateTimeOffset))]
    [InlineData("user-defined type (dbo.WorkDay -> date)", "date", typeof(DateOnly))]
    [InlineData("user-defined type (dbo.StartAt -> time)", "time", typeof(TimeOnly))]
    public void NormalizeScaffoldClrType_MapsSafeSqlServerAliasBaseTypeWhenSchemaTypeIsVague(string detail, string baseType, Type expected)
    {
        var method = GetMethod(
            "NormalizeScaffoldClrType",
            new[] { typeof(DatabaseProvider), typeof(Type), typeof(bool), typeof(bool), typeof(bool), typeof(string), typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("TryMapSqlServerAliasBaseClrType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string), typeof(Type).MakeByRefType() }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "TryMapSqlServerAliasBaseClrType");

        var result = (Type)method.Invoke(
            null,
            new object?[] { new SqlServerProvider(), typeof(object), false, false, false, null, detail })!;
        object?[] dynamicArgs = { baseType, null };

        Assert.Equal(expected, result);
        Assert.True((bool)dynamicMethod.Invoke(null, dynamicArgs)!);
        Assert.Equal(expected, (Type)dynamicArgs[1]!);
    }

    [Fact]
    public void NormalizeScaffoldClrType_DoesNotMapUnsafeSqlServerAliasBaseType()
    {
        var method = GetMethod(
            "NormalizeScaffoldClrType",
            new[] { typeof(DatabaseProvider), typeof(Type), typeof(bool), typeof(bool), typeof(bool), typeof(string), typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("TryMapSqlServerAliasBaseClrType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string), typeof(Type).MakeByRefType() }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "TryMapSqlServerAliasBaseClrType");

        var result = (Type)method.Invoke(
            null,
            new object?[] { new SqlServerProvider(), typeof(object), false, false, false, null, "user-defined type (dbo.Shape -> geography)" })!;
        object?[] dynamicArgs = { "geography", null };

        Assert.Equal(typeof(object), result);
        Assert.False((bool)dynamicMethod.Invoke(null, dynamicArgs)!);
    }

    [Theory]
    [InlineData("user-defined type (dbo.EmailAddress -> nvarchar(320))", "nvarchar(320)", 320)]
    [InlineData("user-defined type (dbo.Code -> varchar(40))", "varchar(40)", 40)]
    [InlineData("user-defined type (dbo.TokenBytes -> varbinary(64))", "varbinary(64)", 64)]
    [InlineData("user-defined type (dbo.FixedToken -> binary(16))", "binary(16)", 16)]
    [InlineData("user-defined type (dbo.Notes -> nvarchar(max))", "nvarchar(max)", null)]
    [InlineData("user-defined type (dbo.Amount -> decimal(18,4))", "decimal(18,4)", null)]
    public void SqlServerAliasBaseMaxLength_StaticAndDynamic_ParseBoundedTextAndBinaryFacets(string detail, string baseType, int? expected)
    {
        var staticMethod = GetMethod("GetSqlServerAliasBaseMaxLength", new[] { typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("GetSqlServerAliasBaseMaxLengthFromTypeText", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "GetSqlServerAliasBaseMaxLengthFromTypeText");

        Assert.Equal(expected, (int?)staticMethod.Invoke(null, new object[] { detail }));
        Assert.Equal(expected, (int?)dynamicMethod.Invoke(null, new object[] { baseType }));
    }

    [Theory]
    [InlineData("set('read','write','admin')", true, 3)]
    [InlineData("set('read', 'write')", true, 2)]
    [InlineData("set('a','b','c','d','e','f','g','h')", true, 8)]
    [InlineData("set('a','b','c','d','e','f','g','h','i')", false, 0)]
    [InlineData("set('read,write','admin')", false, 0)]
    [InlineData("set('read','read')", false, 0)]
    [InlineData("set('read' 'write')", false, 0)]
    [InlineData("set('read',)", false, 0)]
    [InlineData("set(,'read')", false, 0)]
    [InlineData("enum('read','write')", false, 0)]
    public void TryParseBoundedMySqlSetValues_StaticAndDynamic_MatchWriteSafety(string detail, bool expected, int expectedCount)
    {
        var staticResult = InvokeTryParseBoundedMySqlSetValues(typeof(DatabaseScaffolder), detail);
        var dynamicResult = InvokeTryParseBoundedMySqlSetValues(typeof(DynamicEntityTypeGenerator), detail);

        Assert.Equal(expected, staticResult.Parsed);
        Assert.Equal(expected, dynamicResult.Parsed);
        Assert.Equal(expectedCount, staticResult.Values.Length);
        Assert.Equal(expectedCount, dynamicResult.Values.Length);
    }

    [Theory]
    [InlineData("JSON", false)]
    [InlineData("XML", false)]
    [InlineData("UUID", false)]
    [InlineData("GEOMETRY", true)]
    [InlineData("GEOMETRY_JSON", true)]
    [InlineData("JSON GEOMETRY", true)]
    [InlineData("XML_GEOGRAPHY", true)]
    [InlineData("POINT", true)]
    [InlineData("POINT_JSON", true)]
    [InlineData("POLYGON", true)]
    [InlineData("MULTIPOLYGON", true)]
    [InlineData("GEOMETRYCOLLECTION", true)]
    [InlineData("UUID[]", true)]
    [InlineData("APPOINTMENT", false)]
    [InlineData("CABINET", false)]
    [InlineData("ENUMERATION", false)]
    [InlineData("SETTINGS", false)]
    [InlineData("TEXT", false)]
    [InlineData("INTEGER", false)]
    public void IsSqliteProviderSpecificDeclaredType_FlagsProviderShapedTypes(string declaredType, bool expected)
    {
        var m = GetMethod("IsSqliteProviderSpecificDeclaredType", new[] { typeof(string) });
        Assert.Equal(expected, (bool)m.Invoke(null, new object[] { declaredType })!);
    }

    [Theory]
    [InlineData("JSON", false)]
    [InlineData("XML", false)]
    [InlineData("UUID", false)]
    [InlineData("GEOMETRY", true)]
    [InlineData("GEOMETRY_JSON", true)]
    [InlineData("JSON GEOMETRY", true)]
    [InlineData("XML_GEOGRAPHY", true)]
    [InlineData("POINT", true)]
    [InlineData("POINT_JSON", true)]
    [InlineData("POLYGON", true)]
    [InlineData("MULTIPOLYGON", true)]
    [InlineData("GEOMETRYCOLLECTION", true)]
    [InlineData("UUID[]", true)]
    [InlineData("APPOINTMENT", false)]
    [InlineData("CABINET", false)]
    [InlineData("ENUMERATION", false)]
    [InlineData("SETTINGS", false)]
    [InlineData("TEXT", false)]
    [InlineData("INTEGER", false)]
    public void IsWriteBlockingSqliteDeclaredType_Dynamic_MatchesStaticDeclaredTypeSafety(string declaredType, bool expected)
    {
        var method = typeof(DynamicEntityTypeGenerator)
            .GetMethod("IsWriteBlockingSqliteDeclaredType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "IsWriteBlockingSqliteDeclaredType");

        Assert.Equal(expected, (bool)method.Invoke(null, new object[] { declaredType })!);
    }

    [Theory]
    [InlineData("UUID", true)]
    [InlineData("uuid", true)]
    [InlineData("UUID TEXT", true)]
    [InlineData("UUID_JSON", true)]
    [InlineData("UUID[]", false)]
    [InlineData("GEOMETRY_UUID", false)]
    [InlineData("MYUUID", false)]
    public void IsSqliteUuidDeclaredType_StaticAndDynamic_RequiresSafeUuidToken(string declaredType, bool expected)
    {
        var staticMethod = GetMethod("IsSqliteUuidDeclaredType", new[] { typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("IsSqliteUuidDeclaredType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "IsSqliteUuidDeclaredType");

        Assert.Equal(expected, (bool)staticMethod.Invoke(null, new object[] { declaredType })!);
        Assert.Equal(expected, (bool)dynamicMethod.Invoke(null, new object[] { declaredType })!);
    }

    [Theory]
    [InlineData("xml", true)]
    [InlineData("json", true)]
    [InlineData("jsonb", true)]
    [InlineData("uuid", true)]
    [InlineData("USER-DEFINED (citext)", true)]
    [InlineData("USER-DEFINED (uuid)", true)]
    [InlineData("year", true)]
    [InlineData("geometry", false)]
    [InlineData("geography", false)]
    [InlineData("hierarchyid", false)]
    [InlineData("sql_variant", false)]
    [InlineData("inet", false)]
    [InlineData("cidr", false)]
    [InlineData("macaddr", false)]
    [InlineData("tsvector", false)]
    [InlineData("tsquery", false)]
    [InlineData("point", false)]
    [InlineData("linestring", false)]
    [InlineData("multipolygon", false)]
    [InlineData("geometrycollection", false)]
    [InlineData("enum", false)]
    [InlineData("enum('draft','paid','cancelled')", true)]
    [InlineData("enum('draft', 'paid')", true)]
    [InlineData("enum('draft' 'paid')", false)]
    [InlineData("enum('draft',)", false)]
    [InlineData("enum(,'draft')", false)]
    [InlineData("ENUM (public.customer_status: 'draft','active','archived')", true)]
    [InlineData("ENUM (public.customer_status: 'draft', 'active')", true)]
    [InlineData("ENUM (public.customer_status: 'draft' 'active')", false)]
    [InlineData("ENUM (public.customer_status: 'draft',)", false)]
    [InlineData("ENUM (public.customer_status: ,'draft')", false)]
    [InlineData("set('read','write','admin')", true)]
    [InlineData("set('a','b','c','d','e','f','g','h')", true)]
    [InlineData("set('a','b','c','d','e','f','g','h','i')", false)]
    [InlineData("set('read,write','admin')", false)]
    [InlineData("DOMAIN (public.email_address -> character varying)", false)]
    [InlineData("DOMAIN (public.customer_status_domain -> ENUM (public.customer_status: 'draft','active','archived'))", false)]
    [InlineData("user-defined type (dbo.EmailAddress -> nvarchar)", false)]
    [InlineData("int unsigned", false)]
    [InlineData("bigint unsigned", false)]
    public void IsScaffoldableProviderSpecificColumnType_PromotesSafeScalarStorage(string detail, bool expected)
    {
        var m = GetMethod("IsScaffoldableProviderSpecificColumnType", new[] { typeof(string) });
        Assert.Equal(expected, (bool)m.Invoke(null, new object[] { detail })!);
    }

    [Fact]
    public void HasWriteBlockingProviderSpecificColumnTypes_AllowsSafeScalarsAndUnsignedButBlocksProviderOwnedTypes()
    {
        var m = GetMethod("HasWriteBlockingProviderSpecificColumnTypes", new[] { typeof(IReadOnlyDictionary<string, string>) });

        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Payload"] = "json" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Email"] = "USER-DEFINED (citext)" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Year"] = "year" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Count"] = "int unsigned" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Amount"] = "decimal(18,4) unsigned" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Amount"] = "numeric(18,4) unsigned" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Email"] = "DOMAIN (public.email_address -> character varying)" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Email"] = "DOMAIN (public.email_ci -> USER-DEFINED (citext))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Scores"] = "DOMAIN (public.score_values -> ARRAY (_int4))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Status"] = "DOMAIN (public.customer_status_domain -> ENUM (public.customer_status: 'draft','active','archived'))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Email"] = "user-defined type (dbo.EmailAddress -> nvarchar)" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Email"] = "user-defined type (dbo.EmailAddress -> nvarchar(320))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Amount"] = "user-defined type (dbo.MoneyAmount -> decimal(18,4))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Token"] = "user-defined type (dbo.TokenBytes -> varbinary(64))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Token"] = "user-defined type (dbo.ExternalToken -> uniqueidentifier)" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Shape"] = "geometry" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Shape"] = "geography" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Path"] = "hierarchyid" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Payload"] = "sql_variant" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Address"] = "inet" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Network"] = "cidr" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Mac"] = "macaddr" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Search"] = "tsvector" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Shape"] = "point" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Shape"] = "multipolygon" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Network"] = "DOMAIN (public.network_address -> inet)" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Network"] = "DOMAIN (public.network_range -> cidr)" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Payload"] = "DOMAIN (public.payload -> USER-DEFINED (custom_payload))" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Shape"] = "user-defined type (dbo.Shape -> geography)" } })!);
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Custom"] = "user-defined type (dbo.CustomPayload)" } })!);
    }

    [Fact]
    public void BuildEnumCheckConstraintConfigurations_EmitsCheckForPostgresDomainEnum()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildEnumCheckConstraintConfigurations",
            new[]
            {
                typeof(IReadOnlyDictionary<string, string>),
                typeof(IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>),
                typeof(IEnumerable<>).MakeGenericType(featureType)
            });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Customers"] = "Customer"
        };
        var propertiesByTable = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Customers"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Status"] = "Status"
            }
        };
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Customers",
            "ProviderSpecificColumnType",
            "Status",
            "DOMAIN (public.customer_status_domain -> ENUM (public.customer_status: 'draft','active','archived'))")!, 0);

        var result = ((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, propertiesByTable, features })!)
            .Cast<object>()
            .ToArray();

        var check = Assert.Single(result);
        Assert.Equal("public.Customers", check.GetType().GetProperty("TableKey")!.GetValue(check));
        Assert.Equal("Customer", check.GetType().GetProperty("EntityName")!.GetValue(check));
        Assert.Equal("CK_Customer_Status_Enum", check.GetType().GetProperty("Name")!.GetValue(check));
        Assert.Equal("Status IN ('draft', 'active', 'archived')", check.GetType().GetProperty("Sql")!.GetValue(check));
    }

    [Fact]
    public void BuildCheckConstraintConfigurations_ReplacesSyntheticConstraintNameWithStableName()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildCheckConstraintConfigurations",
            new[]
            {
                typeof(IReadOnlyDictionary<string, string>),
                typeof(IEnumerable<>).MakeGenericType(featureType)
            });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["dbo.Orders"] = "Order"
        };
        var feature = Activator.CreateInstance(
            featureType,
            "dbo.Orders",
            "CheckConstraint",
            "CK__Orders__Amount__12345678",
            "([Amount]>(0))")!;
        featureType.GetProperty("Metadata")!.SetValue(
            feature,
            new Dictionary<string, object?> { ["isSyntheticName"] = true });
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(feature, 0);

        var result = ((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>()
            .ToArray();

        var check = Assert.Single(result);
        var name = Assert.IsType<string>(check.GetType().GetProperty("Name")!.GetValue(check));
        Assert.StartsWith("CK_Order_", name, StringComparison.Ordinal);
        Assert.DoesNotContain("CK__Orders__", name, StringComparison.Ordinal);
        Assert.Equal("[Amount]>(0)", check.GetType().GetProperty("Sql")!.GetValue(check));
    }

    [Theory]
    [InlineData("character varying(320)", "character varying(320)")]
    [InlineData("varchar(64)", "character varying(64)")]
    [InlineData("character(12)", "character(12)")]
    [InlineData("char(8)", "character(8)")]
    [InlineData("numeric(10,2)", "numeric(10,2)")]
    [InlineData("decimal(18, 4)", "numeric(18,4)")]
    [InlineData("numeric(18,,2)", "text")]
    [InlineData("numeric(18,)", "text")]
    [InlineData("numeric(,2)", "text")]
    [InlineData("varchar()", "text")]
    [InlineData("USER-DEFINED (citext)", "citext")]
    [InlineData("USER-DEFINED (uuid)", "uuid")]
    [InlineData("ARRAY (_int4)", "integer[]")]
    [InlineData("ARRAY (_text)", "text[]")]
    [InlineData("ARRAY (_bytea)", "bytea[]")]
    [InlineData("ARRAY (_timestamptz)", "timestamp with time zone[]")]
    public void NormalizePostgresDomainProbeCastType_StaticAndDynamic_NormalizesSafeFacetsAndTextCastsMalformedTypes(string typeText, string expected)
    {
        var staticMethod = GetMethod("NormalizePostgresDomainProbeCastType", new[] { typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("NormalizePostgresDomainProbeCastType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "NormalizePostgresDomainProbeCastType");

        Assert.Equal(expected, (string)staticMethod.Invoke(null, new object[] { typeText })!);
        Assert.Equal(expected, (string)dynamicMethod.Invoke(null, new object[] { typeText })!);
    }

    [Theory]
    [InlineData("USER-DEFINED (citext)", "citext")]
    [InlineData("USER-DEFINED (uuid)", "uuid")]
    [InlineData("USER-DEFINED (custom_payload)", "text")]
    public void TryGetPostgresSchemaProbeCastType_Static_PreservesSafeUdtsAndTextCastsUnsafe(string detail, string expected)
    {
        var method = GetMethod("TryGetPostgresSchemaProbeCastType", new[] { typeof(string), typeof(string).MakeByRefType() });
        object?[] args = { detail, null };

        Assert.True((bool)method.Invoke(null, args)!);
        Assert.Equal(expected, args[1]);
    }

    [Fact]
    public void ShouldMarkScaffoldedEntityReadOnly_BlocksUnparsedIdentityStrategyAndUnmodeledDefaults()
    {
        var m = GetMethod(
            "ShouldMarkScaffoldedEntityReadOnly",
            new[]
            {
                typeof(string),
                typeof(IReadOnlySet<string>),
                typeof(IReadOnlySet<string>),
                typeof(IReadOnlySet<string>),
                typeof(IReadOnlySet<string>),
                typeof(IReadOnlySet<string>),
                typeof(IReadOnlyDictionary<string, string>),
                typeof(IReadOnlyDictionary<string, IReadOnlyList<string>>)
            });
        var emptyTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var identityStrategyTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "dbo.Orders"
        };
        var defaultTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "dbo.AuditRows"
        };
        var primaryKeys = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["dbo.Orders"] = new[] { "Id" },
            ["dbo.AuditRows"] = new[] { "Id" },
            ["dbo.Customers"] = new[] { "Id" },
            ["public.Customers"] = new[] { "Id" }
        };

        Assert.True((bool)m.Invoke(
            null,
            new object?[]
            {
                "dbo.Orders",
                emptyTables,
                emptyTables,
                emptyTables,
                identityStrategyTables,
                emptyTables,
                null,
                primaryKeys
            })!);
        Assert.True((bool)m.Invoke(
            null,
            new object?[]
            {
                "dbo.AuditRows",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                defaultTables,
                null,
                primaryKeys
            })!);
        Assert.False((bool)m.Invoke(
            null,
            new object?[]
            {
                "dbo.Orders",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                null,
                primaryKeys
            })!);
        Assert.False((bool)m.Invoke(
            null,
            new object?[]
            {
                "public.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Email"] = "DOMAIN (public.email_ci -> USER-DEFINED (citext))" },
                primaryKeys
            })!);
        Assert.False((bool)m.Invoke(
            null,
            new object?[]
            {
                "public.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Email"] = "USER-DEFINED (citext)" },
                primaryKeys
            })!);
        Assert.False((bool)m.Invoke(
            null,
            new object?[]
            {
                "public.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Status"] = "DOMAIN (public.customer_status_domain -> ENUM (public.customer_status: 'draft','active','archived'))" },
                primaryKeys
            })!);
        Assert.False((bool)m.Invoke(
            null,
            new object?[]
            {
                "dbo.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Email"] = "user-defined type (dbo.EmailAddress -> nvarchar(320))" },
                primaryKeys
            })!);
        Assert.False((bool)m.Invoke(
            null,
            new object?[]
            {
                "dbo.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Amount"] = "user-defined type (dbo.MoneyAmount -> decimal(18,4))" },
                primaryKeys
            })!);
        Assert.True((bool)m.Invoke(
            null,
            new object?[]
            {
                "public.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Network"] = "DOMAIN (public.network_address -> inet)" },
                primaryKeys
            })!);
        Assert.True((bool)m.Invoke(
            null,
            new object?[]
            {
                "dbo.Customers",
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                emptyTables,
                new Dictionary<string, string> { ["Shape"] = "user-defined type (dbo.Shape -> geography)" },
                primaryKeys
            })!);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_SuppressesProviderSpecificAccessMethods()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents"] = "Document"
        };

        var ordinaryExpressionFeatures = Array.CreateInstance(featureType, 1);
        ordinaryExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName",
            "CREATE INDEX \"IX_Documents_LowerName\" ON public.\"Documents\" USING btree (lower(\"Name\"))")!, 0);

        var ordinaryResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, ordinaryExpressionFeatures })!;

        var providerSpecificExpressionFeatures = Array.CreateInstance(featureType, 2);
        providerSpecificExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_Search",
            "CREATE INDEX \"IX_Documents_Search\" ON public.\"Documents\" USING gin (to_tsvector('simple'::regconfig, \"Name\"))")!, 0);
        providerSpecificExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ProviderSpecificIndex",
            "IX_Documents_Search",
            "gin index")!, 1);

        var providerSpecificResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, providerSpecificExpressionFeatures })!;

        var includedExpressionFeatures = Array.CreateInstance(featureType, 2);
        includedExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName_Include",
            "CREATE INDEX \"IX_Documents_LowerName_Include\" ON public.\"Documents\" USING btree (lower(\"Name\")) INCLUDE (\"Score\")")!, 0);
        includedExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "IncludedColumnIndex",
            "IX_Documents_LowerName_Include",
            "PostgreSQL index with included columns")!, 1);

        var includedExpressionResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, includedExpressionFeatures })!;

        var descendingExpressionFeatures = Array.CreateInstance(featureType, 2);
        descendingExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName_Desc",
            "CREATE INDEX \"IX_Documents_LowerName_Desc\" ON public.\"Documents\" USING btree (lower(\"Name\") DESC)")!, 0);
        descendingExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "DescendingIndex",
            "IX_Documents_LowerName_Desc",
            "PostgreSQL descending expression index key")!, 1);

        var descendingExpressionResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, descendingExpressionFeatures })!;

        var mySqlExpressionFeatures = Array.CreateInstance(featureType, 1);
        mySqlExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_MySqlExpression",
            "MySQL expression index; expression=(LOWER(`Name`)), `Score`; isUnique=true")!, 0);

        var mySqlExpressionResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, mySqlExpressionFeatures })!;

        Assert.Single((System.Collections.IEnumerable)ordinaryResult);
        Assert.Empty((System.Collections.IEnumerable)providerSpecificResult);
        Assert.Empty((System.Collections.IEnumerable)includedExpressionResult);
        var descendingExpression = Assert.Single((System.Collections.IEnumerable)descendingExpressionResult);
        Assert.NotNull(descendingExpression);
        Assert.Equal(
            "lower(\"Name\") DESC",
            descendingExpression.GetType().GetProperty("ExpressionSql")!.GetValue(descendingExpression));
        var mySqlExpression = Assert.Single((System.Collections.IEnumerable)mySqlExpressionResult);
        Assert.NotNull(mySqlExpression);
        Assert.Equal(
            "(LOWER(`Name`)), `Score`",
            mySqlExpression.GetType().GetProperty("ExpressionSql")!.GetValue(mySqlExpression));
        Assert.Equal(
            true,
            mySqlExpression.GetType().GetProperty("IsUnique")!.GetValue(mySqlExpression));
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ExtractsFilterAfterExpressionBody()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents"] = "Document"
        };
        var features = Array.CreateInstance(featureType, 2);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LiteralWhere",
            "CREATE INDEX \"IX_Documents_LiteralWhere\" ON public.\"Documents\" USING btree (strpos(\"Name\", ' WHERE '))")!, 0);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LiteralWhere_Filtered",
            "CREATE INDEX \"IX_Documents_LiteralWhere_Filtered\" ON public.\"Documents\" USING btree (strpos(\"Name\", ' WHERE ')) WHERE \"Name\" IS NOT NULL")!, 1);

        var result = ((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>()
            .ToDictionary(
                item => (string)item.GetType().GetProperty("Name")!.GetValue(item)!,
                StringComparer.Ordinal);

        Assert.Equal(2, result.Count);
        Assert.Equal(
            "strpos(\"Name\", ' WHERE ')",
            result["IX_Documents_LiteralWhere"].GetType().GetProperty("ExpressionSql")!.GetValue(result["IX_Documents_LiteralWhere"]));
        Assert.Null(result["IX_Documents_LiteralWhere"].GetType().GetProperty("FilterSql")!.GetValue(result["IX_Documents_LiteralWhere"]));
        Assert.Equal(
            "strpos(\"Name\", ' WHERE ')",
            result["IX_Documents_LiteralWhere_Filtered"].GetType().GetProperty("ExpressionSql")!.GetValue(result["IX_Documents_LiteralWhere_Filtered"]));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            result["IX_Documents_LiteralWhere_Filtered"].GetType().GetProperty("FilterSql")!.GetValue(result["IX_Documents_LiteralWhere_Filtered"]));
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ExtractsFilterAcrossWhitespace()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["main.Documents"] = "Document"
        };
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "main.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName_Filtered",
            "CREATE INDEX \"IX_Documents_LowerName_Filtered\" ON \"Documents\" (lower(\"Name\"))\r\nWHERE \"Name\" IS NOT NULL")!, 0);

        var result = Assert.Single(((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>());

        Assert.Equal(
            "lower(\"Name\")",
            result.GetType().GetProperty("ExpressionSql")!.GetValue(result));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            result.GetType().GetProperty("FilterSql")!.GetValue(result));
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_DetectsUniqueOnlyFromCreateIndexPrefix()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents"] = "Document"
        };
        var features = Array.CreateInstance(featureType, 2);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_CreateUniqueLiteral",
            "CREATE INDEX \"IX_Documents_CreateUniqueLiteral\" ON public.\"Documents\" USING btree (strpos(\"Name\", 'CREATE UNIQUE'))")!, 0);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "UX_Documents_LowerName",
            "CREATE UNIQUE INDEX \"UX_Documents_LowerName\" ON public.\"Documents\" USING btree (lower(\"Name\"))")!, 1);

        var result = ((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>()
            .ToDictionary(
                item => (string)item.GetType().GetProperty("Name")!.GetValue(item)!,
                StringComparer.Ordinal);

        Assert.False((bool)result["IX_Documents_CreateUniqueLiteral"].GetType().GetProperty("IsUnique")!.GetValue(result["IX_Documents_CreateUniqueLiteral"])!);
        Assert.True((bool)result["UX_Documents_LowerName"].GetType().GetProperty("IsUnique")!.GetValue(result["UX_Documents_LowerName"])!);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ParsesKeyListAfterQuotedTableName()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents(Archive)"] = "DocumentArchive"
        };
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents(Archive)",
            "ExpressionIndex",
            "IX_Documents_Archive_LowerName",
            "CREATE INDEX \"IX_Documents_Archive_LowerName\" ON public.\"Documents(Archive)\" USING btree (lower(\"Name\")) WHERE \"Name\" IS NOT NULL")!, 0);

        var result = Assert.Single(((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>());

        Assert.Equal(
            "lower(\"Name\")",
            result.GetType().GetProperty("ExpressionSql")!.GetValue(result));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            result.GetType().GetProperty("FilterSql")!.GetValue(result));
    }

    [Theory]
    [InlineData("ARRAY (_int4)", typeof(int[]))]
    [InlineData("ARRAY (_text)", typeof(string[]))]
    [InlineData("ARRAY (_citext)", typeof(string[]))]
    [InlineData("ARRAY (_uuid)", typeof(Guid[]))]
    [InlineData("ARRAY (_bytea)", typeof(byte[][]))]
    [InlineData("ARRAY (_time)", typeof(TimeOnly[]))]
    [InlineData("ARRAY (_interval)", typeof(TimeSpan[]))]
    [InlineData("ARRAY (_timestamptz)", typeof(DateTimeOffset[]))]
    public void TryMapPostgresArrayType_MapsSafeScalarArrays(string detail, Type expected)
    {
        var m = GetMethod("TryMapPostgresArrayType", new[] { typeof(string), typeof(Type).MakeByRefType() });
        object?[] args = { detail, null };

        Assert.True((bool)m.Invoke(null, args)!);
        Assert.Equal(expected, args[1]);
    }

    [Theory]
    [InlineData("ARRAY (_inet)")]
    [InlineData("USER-DEFINED (my_enum)")]
    public void TryMapPostgresArrayType_RejectsProviderSpecificElementArrays(string detail)
    {
        var m = GetMethod("TryMapPostgresArrayType", new[] { typeof(string), typeof(Type).MakeByRefType() });
        object?[] args = { detail, null };

        Assert.False((bool)m.Invoke(null, args)!);
    }

    [Theory]
    [InlineData("SQL Server synonym; baseObject=[dbo].[Orders]; baseType=U", true)]
    [InlineData("SQL Server synonym; baseObject=[dbo].[OrderView]; baseType=V", true)]
    [InlineData("SQL Server synonym; baseObject=[dbo].[Orders; note=retained]; baseType=U", true)]
    [InlineData("SQL Server synonym; baseObject=[dbo].[Rebuild]; baseType=P", false)]
    [InlineData("SQL Server synonym; baseObject=[remote].[dbo].[Orders]; baseType=", false)]
    public void IsTableLikeSqlServerSynonym_AllowsOnlyResolvedTableOrViewTargets(string detail, bool expected)
    {
        var m = GetMethod("IsTableLikeSqlServerSynonym", new[] { typeof(string) });
        Assert.Equal(expected, (bool)m.Invoke(null, new object[] { detail })!);
    }

    [Theory]
    [InlineData(typeof(sbyte), "sbyte")]
    [InlineData(typeof(uint), "uint")]
    [InlineData(typeof(ulong), "ulong")]
    [InlineData(typeof(ushort), "ushort")]
    [InlineData(typeof(char), "char")]
    [InlineData(typeof(DateOnly), "DateOnly")]
    [InlineData(typeof(DateTimeOffset), "DateTimeOffset")]
    [InlineData(typeof(TimeOnly), "TimeOnly")]
    [InlineData(typeof(TimeSpan), "TimeSpan")]
    public void GetTypeName_CommonScalarTypes_ReturnsStableCSharpName(Type type, string expected)
    {
        Assert.Equal(expected, InvokeGetTypeName(type, false));
        Assert.Equal(expected + "?", InvokeGetTypeName(type, true));
    }

    [Fact]
    public void GetUnqualifiedName_SchemaQualified_ReturnsTablePart()
    {
        Assert.Equal("table", InvokeGetUnqualifiedName("schema.table"));
    }

    [Fact]
    public void GetUnqualifiedName_NoSchema_ReturnsWhole()
    {
        Assert.Equal("mytable", InvokeGetUnqualifiedName("mytable"));
    }

    [Fact]
    public void GetUnqualifiedName_TwoLevelSchema_ReturnsLast()
    {
        Assert.Equal("leaf", InvokeGetUnqualifiedName("db.schema.leaf"));
    }

    // ── GetSchemaNameOrNull ─────────────────────────────────────────────────

    [Fact]
    public void GetSchemaNameOrNull_SchemaQualified_ReturnsSchema()
    {
        Assert.Equal("schema", InvokeGetSchemaNameOrNull("schema.table"));
    }

    [Fact]
    public void GetSchemaNameOrNull_NoSchema_ReturnsNull()
    {
        Assert.Null(InvokeGetSchemaNameOrNull("table"));
    }

    [Fact]
    public void GetSchemaNameOrNull_LeadingDot_ReturnsNull()
    {
        // ".table" → idx=0, which is not >0 → returns null
        Assert.Null(InvokeGetSchemaNameOrNull(".table"));
    }

    [Fact]
    public async Task ScaffoldAsync_EmptyDatabase_ThrowsOrWritesContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "TestCtx");
            Assert.True(File.Exists(Path.Combine(dir, "TestCtx.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTable_ThrowsOrWritesFiles()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SanWidget2 (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "MyCtx2");
            Assert.True(File.Exists(Path.Combine(dir, "MyCtx2.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SanWidget2.cs")));
            var entityCode = File.ReadAllText(Path.Combine(dir, "SanWidget2.cs"));
            Assert.Contains("[Required]", entityCode);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoWarnings_RemovesStaleWarningReportsWhenOverwriteAllowed()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CleanWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var warningMd = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
        var warningJson = Path.Combine(dir, "nORM.ScaffoldWarnings.json");
        await File.WriteAllTextAsync(warningMd, "stale");
        await File.WriteAllTextAsync(warningJson, """{"stale":true}""");
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CleanCtx");

            Assert.True(File.Exists(Path.Combine(dir, "CleanWidget.cs")));
            Assert.False(File.Exists(warningMd));
            Assert.False(File.Exists(warningJson));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoWarningsAndNoOverwrite_FailsOnStaleWarningReports()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CleanNoOverwriteWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var warningMd = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
        await File.WriteAllTextAsync(warningMd, "stale");
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "CleanNoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));

            Assert.Contains("stale scaffold warning report", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(File.Exists(warningMd));
            Assert.False(File.Exists(Path.Combine(dir, "CleanNoOverwriteWidget.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
