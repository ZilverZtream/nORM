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
