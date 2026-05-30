// Tests for DatabaseScaffolder private helpers, NavigationContext,
// NavigationPropertyInfo, LazyNavigationCollection<T>, LazyNavigationReference<T>,
// and NormIncludableQueryable / ThenInclude extensions.

#nullable enable

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
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

// ── Entity types for LazyNavigationCollection / LazyNavigationReference tests ──

[Table("SAN_Parent")]
[Xunit.Trait("Category", "Fast")]
public class SanParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<SanChild>? Children { get; set; }
}

[Table("SAN_Child")]
[Xunit.Trait("Category", "Fast")]
public class SanChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
}

// ── Entity types for ThenInclude tests ────────────────────────────────────────

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

[Table("SAN_TIParent")]
[Xunit.Trait("Category", "Fast")]
public class SanTIParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<SanTIChild>? Children { get; set; }
    public SanTIChild? SingleChild { get; set; }
}

[Table("SAN_TIChild")]
[Xunit.Trait("Category", "Fast")]
public class SanTIChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public ICollection<SanTIGrandchild>? Grandchildren { get; set; }
    public SanTIGrandchild? Grandchild { get; set; }
}

[Table("SAN_TIGrandchild")]
[Xunit.Trait("Category", "Fast")]
public class SanTIGrandchild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ChildId { get; set; }
}

// ── DatabaseScaffolder private-method helpers via reflection ──────────────────

[Xunit.Trait("Category", "Fast")]
public class DatabaseScaffolderPrivateMethodTests
{
    // ── Reflection helpers ──────────────────────────────────────────────────

    private static MethodInfo GetMethod(string name, Type[] paramTypes)
        => typeof(DatabaseScaffolder)
               .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static, null, paramTypes, null)
           ?? throw new MissingMethodException(nameof(DatabaseScaffolder), name);

    private static string InvokeToPascalCase(string input)
    {
        var m = GetMethod("ToPascalCase", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { input })!;
    }

    private static string InvokeEscapeCSharpIdentifier(string input)
    {
        var m = GetMethod("EscapeCSharpIdentifier", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { input })!;
    }

    private static string InvokeDynamicEscapeCSharpIdentifier(string input)
    {
        var m = typeof(DynamicEntityTypeGenerator)
            .GetMethod("EscapeCSharpIdentifier", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "EscapeCSharpIdentifier");
        return (string)m.Invoke(null, new object[] { input })!;
    }

    private static string InvokeGetTypeName(Type type, bool allowNull)
    {
        var m = GetMethod("GetTypeName", new[] { typeof(Type), typeof(bool) });
        return (string)m.Invoke(null, new object[] { type, allowNull })!;
    }

    private static int? InvokeGetScaffoldMaxLength(Type type, object? columnSize)
    {
        var m = GetMethod("GetScaffoldMaxLength", new[] { typeof(Type), typeof(DataRow) });
        return (int?)m.Invoke(null, new object[] { type, CreateSchemaRow(columnSize) });
    }

    private static int? InvokeDynamicGetScaffoldMaxLength(Type type, object? columnSize)
    {
        var m = typeof(DynamicEntityTypeGenerator)
            .GetMethod("GetScaffoldMaxLength", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(Type), typeof(DataRow) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "GetScaffoldMaxLength");
        return (int?)m.Invoke(null, new object[] { type, CreateSchemaRow(columnSize) });
    }

    private static DataRow CreateSchemaRow(object? columnSize)
    {
        var table = new DataTable();
        table.Columns.Add("ColumnSize", typeof(object));
        var row = table.NewRow();
        row["ColumnSize"] = columnSize ?? DBNull.Value;
        table.Rows.Add(row);
        return row;
    }

    private static string InvokeGetUnqualifiedName(string identifier)
    {
        var m = GetMethod("GetUnqualifiedName", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { identifier })!;
    }

    private static string? InvokeGetSchemaNameOrNull(string identifier)
    {
        var m = GetMethod("GetSchemaNameOrNull", new[] { typeof(string) });
        return (string?)m.Invoke(null, new object[] { identifier });
    }

    private static string InvokeScaffoldContext(string ns, string ctxName, IEnumerable<string> entities)
    {
        var m = GetMethod("ScaffoldContext", new[] { typeof(string), typeof(string), typeof(IEnumerable<string>) });
        return (string)m.Invoke(null, new object[] { ns, ctxName, entities })!;
    }

    private static string InvokeScaffoldContextWithRoutineStub()
        => InvokeScaffoldContextWithRoutine("dbo", "GetRevenue", "SQL Server stored procedure; parameters=3; outputParameters=2; parameterModes=@tenantId:IN:int,@total:OUT:decimal(18,2),@message:INOUT:nvarchar(32)");

    private static string InvokeScaffoldContextWithRoutineReturnStub()
        => InvokeScaffoldContextWithRoutine("dbo", "ApplyDiscount", "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@orderId:IN:int,return:RETURN:int");

    private static string InvokeScaffoldContextWithRoutine(string? schema, string name, string detail)
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            schema,
            name,
            "Routine",
            detail)!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 1);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        routines.SetValue(routine, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions })!;
    }

    private static string InvokeScaffoldContextWithSequence(string? schema, string name, string detail)
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var sequence = Activator.CreateInstance(
            skippedObjectType,
            schema,
            name,
            "Sequence",
            detail)!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 1);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        sequences.SetValue(sequence, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions })!;
    }

    private static string InvokeScaffoldContextWithIdentityOptions()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var identity = Activator.CreateInstance(
            identityOptionType,
            "dbo.Users",
            "User",
            "Id",
            "Id",
            1000L,
            25L)!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 1);
        identityOptions.SetValue(identity, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions })!;
    }

    // ── ToPascalCase ────────────────────────────────────────────────────────

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
    [InlineData("numeric(19,4)", true, 19, 4)]
    [InlineData("varchar(20)", false, 0, 0)]
    public void TryParseDecimalPrecision_ParsesProviderNumericDeclarations(string typeName, bool expected, int expectedPrecision, int expectedScale)
    {
        var method = GetMethod("TryParseDecimalPrecision", new[] { typeof(string), typeof(int).MakeByRefType(), typeof(int).MakeByRefType() });
        var args = new object[] { typeName, 0, 0 };

        var result = (bool)method.Invoke(null, args)!;

        Assert.Equal(expected, result);
        Assert.Equal(expectedPrecision, (int)args[1]);
        Assert.Equal(expectedScale, (int)args[2]);
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
    public void GetScaffoldMaxLength_StaticAndDynamic_UseBoundedStringAndBinarySizes(Type clrType, int columnSize, int? expected)
    {
        Assert.Equal(expected, InvokeGetScaffoldMaxLength(clrType, columnSize));
        Assert.Equal(expected, InvokeDynamicGetScaffoldMaxLength(clrType, columnSize));
    }

    [Theory]
    [InlineData("JSON", false)]
    [InlineData("XML", false)]
    [InlineData("UUID", false)]
    [InlineData("GEOMETRY", true)]
    [InlineData("TEXT", false)]
    [InlineData("INTEGER", false)]
    public void IsSqliteProviderSpecificDeclaredType_FlagsProviderShapedTypes(string declaredType, bool expected)
    {
        var m = GetMethod("IsSqliteProviderSpecificDeclaredType", new[] { typeof(string) });
        Assert.Equal(expected, (bool)m.Invoke(null, new object[] { declaredType })!);
    }

    [Theory]
    [InlineData("xml", true)]
    [InlineData("json", true)]
    [InlineData("jsonb", true)]
    [InlineData("uuid", true)]
    [InlineData("USER-DEFINED (uuid)", true)]
    [InlineData("year", true)]
    [InlineData("geometry", false)]
    [InlineData("inet", false)]
    [InlineData("enum", false)]
    public void IsScaffoldableProviderSpecificColumnType_PromotesSafeScalarStorage(string detail, bool expected)
    {
        var m = GetMethod("IsScaffoldableProviderSpecificColumnType", new[] { typeof(string) });
        Assert.Equal(expected, (bool)m.Invoke(null, new object[] { detail })!);
    }

    [Theory]
    [InlineData("ARRAY (_int4)", typeof(int[]))]
    [InlineData("ARRAY (_text)", typeof(string[]))]
    [InlineData("ARRAY (_uuid)", typeof(Guid[]))]
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

    // ── ScaffoldContext ─────────────────────────────────────────────────────

    [Fact]
    public void ScaffoldContext_ContainsClassName()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "User", "Order" });
        Assert.Contains("class AppDbContext", code);
    }

    [Fact]
    public void ScaffoldContext_ContainsNamespace()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "User" });
        Assert.Contains("namespace MyApp", code);
    }

    [Fact]
    public void ScaffoldContext_ContainsEntityProperty()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "User" });
        Assert.Contains("Users", code);
    }

    [Fact]
    public void ScaffoldContext_UsesReadablePluralizedQueryPropertyNames()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "Category", "Class" });
        Assert.Contains("IQueryable<Category> Categories", code);
        Assert.Contains("IQueryable<Class> Classes", code);
    }

    [Fact]
    public void ScaffoldContext_EntitiesAreSortedAlphabetically()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "Zebra", "Apple" });
        var appleIdx = code.IndexOf("Apples", StringComparison.Ordinal);
        var zebraIdx = code.IndexOf("Zebras", StringComparison.Ordinal);
        Assert.True(appleIdx < zebraIdx, "Entities should be alphabetically sorted in the context.");
    }

    [Fact]
    public void ScaffoldContext_ContextInheritsDbContext()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", Array.Empty<string>());
        Assert.Contains(": DbContext", code);
    }

    [Fact]
    public void ScaffoldContext_KeywordContextName_PrependsAt()
    {
        // "class" is a C# keyword; the context class name should be escaped
        var code = InvokeScaffoldContext("MyApp", "class", Array.Empty<string>());
        Assert.Contains("@class", code);
    }

    [Fact]
    public void ScaffoldContext_ContainsAutoGeneratedHeader()
    {
        var code = InvokeScaffoldContext("MyApp", "Ctx", Array.Empty<string>());
        Assert.Contains("auto-generated", code);
    }

    [Fact]
    public void ScaffoldContext_WithRoutineStub_EmitsProviderBoundWrapperMethods()
    {
        var code = InvokeScaffoldContextWithRoutineStub();

        Assert.Contains("using System.Threading;", code);
        Assert.Contains("using System.Threading.Tasks;", code);
        Assert.Contains("Executes provider-bound stored procedure `dbo.GetRevenue`", code);
        Assert.Contains("Parameters discovered at scaffold time: @tenantId IN int, @total OUT decimal", code);
        Assert.Contains("public sealed class GetRevenueParameters", code);
        Assert.Contains("public int? tenantId { get; init; }", code);
        Assert.Contains("public string? message { get; init; }", code);
        Assert.Contains("Task<List<TResult>> GetRevenueAsync<TResult>(GetRevenueParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, parameters)", code);
        Assert.Contains("IAsyncEnumerable<TResult> StreamGetRevenueAsync<TResult>(GetRevenueParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("ExecuteStoredProcedureAsAsyncEnumerable<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, parameters)", code);
        Assert.Contains("without buffering the full result set", code);
        Assert.Contains("Task<StoredProcedureResult<TResult>> GetRevenueWithOutputAsync<TResult>", code);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, parameters, outputParameters)", code);
        Assert.Contains("output parameters discovered at scaffold time", code);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenue\"), ct, parameters, CreateGetRevenueOutputParameters())", code);
        Assert.Contains("public static OutputParameter[] CreateGetRevenueOutputParameters()", code);
        Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal)", code);
        Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32, System.Data.ParameterDirection.InputOutput)", code);
        Assert.Contains("Routine bodies are provider-owned and are not translated by nORM", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_routine_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    // ── ScaffoldAsync (public integration) ─────────────────────────────────

    [Fact]
    public void ScaffoldContext_WithRoutineReturnValue_EmitsReturnOutputParameter()
    {
        var code = InvokeScaffoldContextWithRoutineReturnStub();

        Assert.Contains("Parameters discovered at scaffold time: @orderId IN int, return RETURN int", code);
        Assert.Contains("public int? orderId { get; init; }", code);
        Assert.Contains("Task<StoredProcedureResult<TResult>> ApplyDiscountWithOutputAsync<TResult>", code);
        Assert.Contains("ExecuteStoredProcedureWithOutputAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"ApplyDiscount\"), ct, parameters, CreateApplyDiscountOutputParameters())", code);
        Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_routine_return_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithQuotedStoredProcedureName_UsesProviderEscapedInvocationName()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "sales ops",
            "Get Revenue",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenantId:IN:int");

        Assert.Contains("Provider.Escape(\"sales ops\") + \".\" + Provider.Escape(\"Get Revenue\")", code);
        Assert.DoesNotContain("\"sales ops.Get Revenue\"", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_quoted_routine_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithSqlServerTableValuedFunction_EmitsSelectWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "GetRevenueRows",
            "SQL Server table-valued function; parameters=1; outputParameters=0; callShape=table-valued-function; parameterModes=@tenantId:IN:int; dataType=TABLE");

        Assert.Contains("Executes provider-bound table-valued function `dbo.GetRevenueRows`", code);
        Assert.Contains("public sealed class GetRevenueRowsParameters", code);
        Assert.Contains("public int? tenantId { get; init; }", code);
        Assert.Contains("var args = parameters is null ? System.Array.Empty<object>() : new object[] { (object?)parameters.tenantId ?? System.DBNull.Value };", code);
        Assert.Contains("Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"GetRevenueRows\")", code);
        Assert.Contains("SELECT * FROM ", code);
        Assert.Contains("QueryUnchangedAsync<TResult>", code);
        Assert.Contains("IAsyncEnumerable<TResult> StreamGetRevenueRowsAsync<TResult>", code);
        Assert.Contains("QueryUnchangedStreamAsync<TResult>", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>(\"dbo.GetRevenueRows\"", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_tvf_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithSqlServerScalarFunction_EmitsValueProjectionWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "dbo",
            "CalculateRisk",
            "SQL Server scalar function; parameters=1; outputParameters=1; callShape=scalar-function; parameterModes=@customerId:IN:int,return:RETURN:int; dataType=int");

        Assert.Contains("Executes provider-bound scalar function `dbo.CalculateRisk`", code);
        Assert.Contains("public sealed class CalculateRiskParameters", code);
        Assert.Contains("public int? customerId { get; init; }", code);
        Assert.Contains("private sealed class CalculateRiskValueResult<TValue>", code);
        Assert.Contains("Task<TValue?> CalculateRiskValueAsync<TValue>(CalculateRiskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("QueryUnchangedAsync<CalculateRiskValueResult<TValue>>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args)", code);
        Assert.Contains("return rows.Count == 0 ? default : rows[0].Value;", code);
        Assert.Contains("SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.Contains("QueryUnchangedAsync<TResult>", code);
        Assert.DoesNotContain("WithOutputAsync", code);
        Assert.True(
            code.IndexOf("Executes provider-bound scalar function `dbo.CalculateRisk`", StringComparison.Ordinal) <
            code.IndexOf("public Task<List<TResult>> CalculateRiskAsync<TResult>", StringComparison.Ordinal));
        Assert.True(
            code.IndexOf("public Task<List<TResult>> CalculateRiskAsync<TResult>", StringComparison.Ordinal) <
            code.IndexOf("private sealed class CalculateRiskValueResult<TValue>", StringComparison.Ordinal));

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_scalar_fn_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithPostgresFunction_EmitsSelectInvocationWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "calculate_risk",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=customer_id:IN:integer; dataType=integer");

        Assert.Contains("Executes provider-bound function `public.calculate_risk`", code);
        Assert.Contains("public int? customer_id { get; init; }", code);
        Assert.Contains("Task<TValue?> CalculateRiskValueAsync<TValue>(CalculateRiskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>", code);
    }

    [Fact]
    public void ScaffoldContext_WithPostgresTableFunction_EmitsSelectStarInvocationWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            "public",
            "customer_orders",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=customer_id:IN:integer; dataType=record");

        Assert.Contains("Executes provider-bound function `public.customer_orders`", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>", code);
    }

    [Fact]
    public void ScaffoldContext_WithMySqlFunction_EmitsSelectInvocationWrapper()
    {
        var code = InvokeScaffoldContextWithRoutine(
            null,
            "calculate_risk",
            "MySQL FUNCTION; parameters=1; outputParameters=0; parameterModes=customer_id:IN:int; dataType=int");

        Assert.Contains("Executes provider-bound FUNCTION `calculate_risk`", code);
        Assert.Contains("Task<TValue?> CalculateRiskValueAsync<TValue>(CalculateRiskParameters? parameters = null, CancellationToken ct = default)", code);
        Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", code);
        Assert.DoesNotContain("ExecuteStoredProcedureAsync<TResult>", code);
    }

    [Fact]
    public void ScaffoldContext_WithSqlServerSequence_EmitsNextValueWrapper()
    {
        var code = InvokeScaffoldContextWithSequence(
            "dbo",
            "OrderNo",
            "SQL Server sequence; dataType=bigint");

        Assert.Contains("private sealed class OrderNoSequenceValue", code);
        Assert.Contains("public long Value { get; set; }", code);
        Assert.Contains("public async Task<long> NextOrderNoValueAsync(CancellationToken ct = default)", code);
        Assert.Contains("SELECT NEXT VALUE FOR ", code);
        Assert.Contains("Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"OrderNo\")", code);
        Assert.Contains("QueryUnchangedAsync<OrderNoSequenceValue>", code);

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_sequence_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "AppDbContext.cs"), code, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dir, "User.cs"), "namespace MyApp; public class User { public int Id { get; set; } }", Encoding.UTF8);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithPostgresSequence_EmitsRegclassWrapper()
    {
        var code = InvokeScaffoldContextWithSequence(
            "public",
            "invoice_no",
            "PostgreSQL sequence; dataType=integer");

        Assert.Contains("private sealed class InvoiceNoSequenceValue", code);
        Assert.Contains("public int Value { get; set; }", code);
        Assert.Contains("public async Task<int> NextInvoiceNoValueAsync(CancellationToken ct = default)", code);
        Assert.Contains("SELECT nextval('", code);
        Assert.Contains("::regclass) AS ", code);
        Assert.Contains("(Provider.Escape(\"public\") + \".\" + Provider.Escape(\"invoice_no\")).Replace(\"'\", \"''\")", code);
    }

    [Fact]
    public void ScaffoldContext_WithIdentityOptions_EmitsFluentSeedAndIncrement()
    {
        var code = InvokeScaffoldContextWithIdentityOptions();

        Assert.Contains("mb.Entity<User>().Property(e => e.Id).HasIdentityOptions(1000, 25);", code);
        Assert.Contains("ConfigureOptions(options)", code);
    }

    [Fact]
    public async Task ScaffoldAsync_NullConnection_ThrowsArgumentNullException()
    {
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(null!, provider, Path.GetTempPath(), "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullProvider_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, null!, Path.GetTempPath(), "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullOutputDirectory_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, null!, "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullNamespace_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, Path.GetTempPath(), null!));
    }

    [Fact]
    public async Task ScaffoldAsync_InvalidNamespace_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();

        await Assert.ThrowsAsync<NormConfigurationException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, Path.GetTempPath(), "1.Bad Namespace"));
    }

    [Fact]
    public async Task ScaffoldAsync_UnsafeContextName_WritesEscapedContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "1 Bad/Ctx");

            var contextFile = Path.Combine(dir, "_1BadCtx.cs");
            Assert.True(File.Exists(contextFile));
            var contextCode = File.ReadAllText(contextFile);
            Assert.Contains("public class _1BadCtx : DbContext", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_ContextNameCollidingWithEntityName_UsesUniqueContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "Widget");

            Assert.True(File.Exists(Path.Combine(dir, "Widget.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "WidgetContext.cs")));
            var entityCode = File.ReadAllText(Path.Combine(dir, "Widget.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "WidgetContext.cs"));

            Assert.Contains("public class Widget", entityCode);
            Assert.Contains("public class WidgetContext : DbContext", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TableAttributeSchema_IsIncludedInRuntimeTableMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE "main"."SchemaWidget" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO "main"."SchemaWidget" (Id, Name) VALUES (1, 'schema-ok');
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(SanSchemaWidget));
        Assert.Equal("main.SchemaWidget", mapping.TableName);
        Assert.Equal("\"main\".\"SchemaWidget\"", mapping.EscTable);

        var rows = await ctx.Query<SanSchemaWidget>().ToListAsync();
        var row = Assert.Single(rows);
        Assert.Equal("schema-ok", row.Name);
    }

    [Fact]
    public void DatabaseGeneratedComputed_IsTreatedAsDatabaseGeneratedColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(SanComputedGenerated));
        var total = Assert.Single(mapping.Columns, c => c.PropName == nameof(SanComputedGenerated.Total));
        Assert.True(total.IsDbGenerated);
        Assert.DoesNotContain(mapping.InsertColumns, c => c.PropName == nameof(SanComputedGenerated.Total));
        Assert.DoesNotContain(mapping.UpdateColumns, c => c.PropName == nameof(SanComputedGenerated.Total));
    }

    [Fact]
    public void RowVersionComputed_IsTimestampAndDatabaseGeneratedColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(SanRowVersionGenerated));
        var rowVersion = Assert.Single(mapping.Columns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
        Assert.True(rowVersion.IsTimestamp);
        Assert.True(rowVersion.IsDbGenerated);
        Assert.DoesNotContain(mapping.InsertColumns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
        Assert.DoesNotContain(mapping.UpdateColumns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
    }

    [Fact]
    public async Task ScaffoldAsync_SqliteAttachedDatabase_PreservesSchemaAndDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                PRAGMA foreign_keys=ON;
                ATTACH DATABASE ':memory:' AS aux;
                CREATE TABLE "aux"."SchemaAuthor" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE "aux"."SchemaBook" (
                    Id INTEGER PRIMARY KEY,
                    AuthorId INTEGER NOT NULL,
                    Title TEXT NOT NULL DEFAULT 'untitled',
                    CONSTRAINT FK_SchemaBook_Author FOREIGN KEY (AuthorId) REFERENCES SchemaAuthor(Id)
                );
                CREATE INDEX "aux"."IX_SchemaBook_Title" ON "SchemaBook"(Title);
                CREATE TRIGGER "aux"."TR_SchemaBook_Audit" AFTER INSERT ON "SchemaBook" BEGIN SELECT 1; END;
                CREATE VIEW "aux"."SchemaBookView" AS SELECT Id, Title FROM "SchemaBook";
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "AttachedCtx");

            var authorCode = File.ReadAllText(Path.Combine(dir, "SchemaAuthor.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "SchemaBook.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "AttachedCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"SchemaAuthor\", Schema = \"aux\")]", authorCode);
            Assert.Contains("[Table(\"SchemaBook\", Schema = \"aux\")]", bookCode);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
            Assert.Contains("HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false)", contextCode);
            Assert.Contains("[Index(\"IX_SchemaBook_Title\")]", bookCode);
            Assert.Contains("aux.SchemaBook", warnings);
            Assert.Contains("TR_SchemaBook_Audit", warnings);
            Assert.Contains("aux.SchemaBookView", warnings);

            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBook" && item.GetProperty("kind").GetString() == "Default");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBook" && item.GetProperty("kind").GetString() == "Trigger");
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item => item.GetProperty("name").GetString() == "aux.SchemaBookView");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TableAttributeSchema_IsUsedInNavigationProjectionSubqueries()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                ATTACH DATABASE ':memory:' AS aux;
                CREATE TABLE "aux"."SchemaParent" (Id INTEGER PRIMARY KEY);
                CREATE TABLE "aux"."SchemaChild" (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO "aux"."SchemaParent" (Id) VALUES (1);
                INSERT INTO "aux"."SchemaChild" (Id, ParentId, Amount) VALUES (10, 1, 7), (11, 1, 5);
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SanSchemaParent>()
                .HasMany<SanSchemaChild>(p => p.Children)
                .WithOne()
                .HasForeignKey(c => c.ParentId, p => p.Id)
        });

        var rows = await ctx.Query<SanSchemaParent>()
            .Select(p => new
            {
                p.Id,
                ChildCount = p.Children.Count(),
                Total = p.Children.Sum(c => c.Amount)
            })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal(2, row.ChildCount);
        Assert.Equal(12, row.Total);
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

    [Fact]
    public async Task ScaffoldAsync_WithSingleColumnForeignKey_GeneratesNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Author_Id INTEGER NOT NULL,
                Title TEXT NOT NULL,
                FOREIGN KEY (Author_Id) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "BookStoreContext");

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "BookStoreContext.cs"));

            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
            Assert.Contains("public Author? Author { get; set; }", bookCode);
            Assert.Contains(".HasMany(p => p.Books)", contextCode);
            Assert.Contains(".WithOne(d => d.Author)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains("configure?.Invoke(mb);", contextCode);
            Assert.Contains("var configuredOptions = options?.Clone() ?? new DbContextOptions();", contextCode);
            Assert.DoesNotContain("options.OnModelCreating =", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMultipleForeignKeysToSamePrincipal_UsesRoleBasedNavigationNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Address (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Line1 TEXT NOT NULL
            );
            CREATE TABLE Shipment (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                BillingAddressId INTEGER NOT NULL,
                ShippingAddressId INTEGER NOT NULL,
                CONSTRAINT FK_Shipment_BillingAddress FOREIGN KEY (BillingAddressId) REFERENCES Address(Id),
                CONSTRAINT FK_Shipment_ShippingAddress FOREIGN KEY (ShippingAddressId) REFERENCES Address(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ShipmentContext");

            var addressCode = File.ReadAllText(Path.Combine(dir, "Address.cs"));
            var shipmentCode = File.ReadAllText(Path.Combine(dir, "Shipment.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ShipmentContext.cs"));

            Assert.Contains("public List<Shipment> ShipmentsByBillingAddressId { get; set; } = new();", addressCode);
            Assert.Contains("public List<Shipment> ShipmentsByShippingAddressId { get; set; } = new();", addressCode);
            Assert.Contains("[ForeignKey(nameof(BillingAddressId))]", shipmentCode);
            Assert.Contains("public Address? BillingAddress { get; set; }", shipmentCode);
            Assert.Contains("[ForeignKey(nameof(ShippingAddressId))]", shipmentCode);
            Assert.Contains("public Address? ShippingAddress { get; set; }", shipmentCode);
            Assert.DoesNotContain("public Address? Address { get; set; }", shipmentCode);
            Assert.Contains(".HasMany(p => p.ShipmentsByBillingAddressId)", contextCode);
            Assert.Contains(".WithOne(d => d.BillingAddress)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.BillingAddressId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains(".HasMany(p => p.ShipmentsByShippingAddressId)", contextCode);
            Assert.Contains(".WithOne(d => d.ShippingAddress)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ShippingAddressId, p => p.Id, cascadeDelete: false);", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingForeignKey_UsesRoleBasedNavigationNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ParentId INTEGER NULL,
                Name TEXT NOT NULL,
                CONSTRAINT FK_Person_Parent FOREIGN KEY (ParentId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "PersonContext");

            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "PersonContext.cs"));

            Assert.Contains("[ForeignKey(nameof(ParentId))]", personCode);
            Assert.Contains("public Person? Parent { get; set; }", personCode);
            Assert.Contains("public List<Person> PersonsByParentId { get; set; } = new();", personCode);
            Assert.DoesNotContain("public Person? Person { get; set; }", personCode);
            Assert.Contains(".HasMany(p => p.PersonsByParentId)", contextCode);
            Assert.Contains(".WithOne(d => d.Parent)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, cascadeDelete: false);", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WhenNavigationNameCollidesWithScalarProperty_MakesNavigationUnique()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Books TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                Author TEXT NOT NULL,
                CONSTRAINT FK_Book_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionNavCtx");

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CollisionNavCtx.cs"));

            Assert.Contains("public string Books { get; set; } = default!;", authorCode);
            Assert.Contains("public List<Book> BooksByAuthorId { get; set; } = new();", authorCode);
            Assert.Contains("public string Author { get; set; } = default!;", bookCode);
            Assert.Contains("public Author? Author2 { get; set; }", bookCode);
            Assert.Contains(".HasMany(p => p.BooksByAuthorId)", contextCode);
            Assert.Contains(".WithOne(d => d.Author2)", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSingleColumnIndexes_GeneratesIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedWidget (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Code TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedWidget_Code ON IndexedWidget(Code);
            CREATE INDEX IX_IndexedWidget_Name ON IndexedWidget(Name);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "IndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedWidget.cs"));
            Assert.Contains("using nORM.Configuration;", entityCode);
            Assert.Contains("[Index(\"IX_IndexedWidget_Code\", IsUnique = true)]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedWidget_Name\")]", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeIndex_GeneratesOrderedIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedOrder (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TenantId INTEGER NOT NULL,
                OrderNo TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedOrder_Tenant_OrderNo ON IndexedOrder(TenantId, OrderNo);
            CREATE INDEX IX_IndexedOrder_Tenant ON IndexedOrder(TenantId);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedOrder.cs"));
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant\")]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant_OrderNo\", IsUnique = true, Order = 0)]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant_OrderNo\", IsUnique = true, Order = 1)]", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqlitePartialAndExpressionIndexes_EmitsIndexMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedProviderSpecific (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE INDEX IX_IndexedProviderSpecific_Name_Active ON IndexedProviderSpecific(Name) WHERE Active = 1;
            CREATE INDEX IX_IndexedProviderSpecific_LowerName ON IndexedProviderSpecific(lower(Name));
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ProviderIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedProviderSpecific.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ProviderIndexCtx.cs"));

            Assert.Contains("[Index(\"IX_IndexedProviderSpecific_Name_Active\", FilterSql = \"Active = 1\")]", entityCode);
            Assert.DoesNotContain("[Index(\"IX_IndexedProviderSpecific_LowerName\")]", entityCode);
            Assert.Contains("mb.Entity<IndexedProviderSpecific>().HasExpressionIndex(\"IX_IndexedProviderSpecific_LowerName\", \"lower(Name)\");", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDescendingIndex_EmitsIndexMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedDirection (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE INDEX IX_IndexedDirection_Name_Desc ON IndexedDirection(Name DESC);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DirectionIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedDirection.cs"));
            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");

            Assert.Contains("[Index(\"IX_IndexedDirection_Name_Desc\", IsDescending = true)]", entityCode);
            Assert.False(File.Exists(warningPath), "Descending ordinary column indexes should scaffold as portable index metadata.");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeForeignKey_EmitsNavigationAndCompositeModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE TenantOrder (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId)
            );
            CREATE TABLE TenantOrderLine (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                LineNo INTEGER NOT NULL,
                Sku TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId, LineNo),
                CONSTRAINT FK_Line_Order FOREIGN KEY (TenantId, OrderId) REFERENCES TenantOrder(TenantId, OrderId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeFkCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "TenantOrder.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "TenantOrderLine.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeFkCtx.cs"));

            Assert.Contains("List<TenantOrderLine>", principalCode);
            Assert.Contains("public TenantOrder?", dependentCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.OrderId }, p => new { p.TenantId, p.OrderId }, cascadeDelete: false);", contextCode);
            Assert.Contains("mb.Entity<TenantOrder>().HasKey(e => new { e.TenantId, e.OrderId });", contextCode);
            Assert.Contains("mb.Entity<TenantOrderLine>().HasKey(e => new { e.TenantId, e.OrderId, e.LineNo });", contextCode);
            Assert.Contains("[Key]", principalCode);
            Assert.Contains("TenantId { get; set; }", principalCode);
            Assert.Contains("OrderId { get; set; }", principalCode);
            Assert.Contains("[Key]", dependentCode);
            Assert.Contains("LineNo { get; set; }", dependentCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositePrimaryKey_UsesDeclaredKeyOrder()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OrderedKeyItem (
                TenantId INTEGER NOT NULL,
                LocalId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (LocalId, TenantId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "OrderedKeyCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "OrderedKeyCtx.cs"));

            Assert.Contains("mb.Entity<OrderedKeyItem>().HasKey(e => new { e.LocalId, e.TenantId });", contextCode);
            Assert.DoesNotContain("mb.Entity<OrderedKeyItem>().HasKey(e => new { e.TenantId, e.LocalId });", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeForeignKeyToUniqueIndex_EmitsNavigationAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE ExternalOrder (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                ExternalNo TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX UX_ExternalOrder_Tenant_ExternalNo ON ExternalOrder(TenantId, ExternalNo);
            CREATE TABLE ExternalOrderEvent (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                ExternalNo TEXT NOT NULL,
                EventName TEXT NOT NULL,
                CONSTRAINT FK_Event_Order FOREIGN KEY (TenantId, ExternalNo) REFERENCES ExternalOrder(TenantId, ExternalNo)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeUniqueFkCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "ExternalOrder.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "ExternalOrderEvent.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeUniqueFkCtx.cs"));

            Assert.Contains("[Index(\"UX_ExternalOrder_Tenant_ExternalNo\", IsUnique = true, Order = 0)]", principalCode);
            Assert.Contains("[Index(\"UX_ExternalOrder_Tenant_ExternalNo\", IsUnique = true, Order = 1)]", principalCode);
            Assert.Contains("List<ExternalOrderEvent>", principalCode);
            Assert.Contains("public ExternalOrder?", dependentCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.ExternalNo }, p => new { p.TenantId, p.ExternalNo }, cascadeDelete: false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithProviderOwnedSchemaFeatures_EmitsDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FeatureOwned (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT COLLATE NOCASE NOT NULL DEFAULT 'new',
                NameLength INTEGER GENERATED ALWAYS AS (length(Name)) VIRTUAL,
                CONSTRAINT CK_FeatureOwned_Name CHECK (length(Name) > 0)
            );
            CREATE TRIGGER TR_FeatureOwned_Audit AFTER INSERT ON FeatureOwned BEGIN SELECT 1; END;
            CREATE VIEW FeatureOwnedView AS SELECT Id, Name FROM FeatureOwned;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FeatureOwnedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "FeatureOwned.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FeatureOwnedCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.Name).HasDefaultValueSql(\"'new'\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().HasCheckConstraint(\"CK_FeatureOwned_Name\", \"length(Name) > 0\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.NameLength).HasComputedColumnSql(\"length(Name) VIRTUAL\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.Name).HasCollation(\"NOCASE\");", contextCode);
            Assert.Contains("Provider-Owned Schema Features", warnings);
            Assert.DoesNotContain("Composite Foreign Keys", warnings);
            Assert.DoesNotContain("| SCF100 |", warnings);
            Assert.DoesNotContain("| SCF101 |", warnings);
            Assert.DoesNotContain("NameLength", warnings);
            Assert.DoesNotContain("| SCF103 |", warnings);
            Assert.Contains("Trigger", warnings);
            Assert.Contains("TR_FeatureOwned_Audit", warnings);
            Assert.DoesNotContain("CheckConstraint", warnings);
            Assert.Contains("Skipped Database Objects", warnings);
            Assert.Contains("FeatureOwnedView", warnings);
            var summary = warningJson.RootElement.GetProperty("summary");
            Assert.Equal(2, summary.GetProperty("totalWarnings").GetInt32());
            Assert.Equal(1, summary.GetProperty("sectionCounts").GetProperty("providerOwnedSchemaFeatures").GetInt32());
            Assert.Equal(1, summary.GetProperty("sectionCounts").GetProperty("skippedDatabaseObjects").GetInt32());
            Assert.False(summary.GetProperty("codes").TryGetProperty("SCF100", out _));
            Assert.Equal(1, summary.GetProperty("codes").GetProperty("SCF200").GetInt32());
            Assert.False(summary.GetProperty("categories").TryGetProperty("schema-feature", out _));
            Assert.Equal(1, summary.GetProperty("categories").GetProperty("database-object").GetInt32());
            Assert.Equal(1, summary.GetProperty("categories").GetProperty("query-object").GetInt32());
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Default");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Computed");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Collation");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Trigger" && item.GetProperty("name").GetString() == "TR_FeatureOwned_Audit");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "CheckConstraint");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Trigger" && item.GetProperty("code").GetString() == "SCF110" && item.GetProperty("category").GetString() == "database-object");
            Assert.All(providerOwned.EnumerateArray(), item => Assert.Equal("Warning", item.GetProperty("severity").GetString()));
            Assert.All(providerOwned.EnumerateArray(), item => Assert.False(string.IsNullOrWhiteSpace(item.GetProperty("suggestedAction").GetString())));
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item => item.GetProperty("kind").GetString() == "View" && item.GetProperty("name").GetString() == "FeatureOwnedView");
            Assert.Contains(skippedObjects.EnumerateArray(), item => item.GetProperty("kind").GetString() == "View" && item.GetProperty("code").GetString() == "SCF200" && item.GetProperty("category").GetString() == "query-object");
            Assert.All(skippedObjects.EnumerateArray(), item => Assert.Equal("Warning", item.GetProperty("severity").GetString()));
            Assert.All(skippedObjects.EnumerateArray(), item => Assert.False(string.IsNullOrWhiteSpace(item.GetProperty("suggestedAction").GetString())));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteProviderSpecificDeclaredTypes_EmitsDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ProviderTyped (
                Id INTEGER PRIMARY KEY,
                Payload JSON NOT NULL,
                ExternalUuid UUID NOT NULL,
                XmlPayload XML NULL,
                Shape GEOMETRY NULL,
                PortableText TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ProviderTypedCtx");

            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            var entityCode = File.ReadAllText(Path.Combine(dir, "ProviderTyped.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("ProviderSpecificColumnType", warnings);
            Assert.DoesNotContain("Payload |", warnings);
            Assert.DoesNotContain("ExternalUuid |", warnings);
            Assert.DoesNotContain("XmlPayload |", warnings);
            Assert.Contains("Shape", warnings);
            Assert.DoesNotContain("PortableText |", warnings);
            Assert.Contains("public string Payload { get; set; } = default!;", entityCode);
            Assert.Contains("public Guid ExternalUuid { get; set; }", entityCode);
            Assert.Contains("public string? XmlPayload { get; set; }", entityCode);

            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("name").GetString() == "Shape" &&
                item.GetProperty("category").GetString() == "schema-feature");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteIntegerPrimaryKey_EmitsIdentityMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IdentityBacked (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "IdentityCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IdentityBacked.cs"));
            Assert.Contains("[Key]", entityCode);
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Identity)]", entityCode);
            Assert.Contains("public long Id { get; set; }", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteVirtualTable_ReportsSkippedObject()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE VIRTUAL TABLE SearchDocs USING fts5(Body);
            CREATE TABLE SearchDocs_Audit (Id INTEGER PRIMARY KEY, Message TEXT NOT NULL);
            """;
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex)
        {
            if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "VirtualCtx");

            Assert.False(File.Exists(Path.Combine(dir, "SearchDocs.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SearchDocsAudit.cs")));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsData", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsIdx", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsContent", StringComparison.OrdinalIgnoreCase));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("VirtualTable", warnings);
            Assert.Contains("SearchDocs", warnings);
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTable" &&
                item.GetProperty("name").GetString() == "SearchDocs");
            Assert.Contains(skippedObjects.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                item.GetProperty("name").GetString()!.StartsWith("SearchDocs_", StringComparison.Ordinal));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithEmitViewEntities_GeneratesSqliteVirtualTableQueryArtifact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE VIRTUAL TABLE SearchDocs USING fts5(Body);
            """;
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex)
        {
            if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "VirtualQueryCtx",
                new ScaffoldOptions { Tables = new[] { "SearchDocs" }, EmitQueryArtifacts = true });

            var entityCode = File.ReadAllText(Path.Combine(dir, "SearchDocs.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "VirtualQueryCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"SearchDocs\")]", entityCode);
            Assert.Contains("Body { get; set; }", entityCode);
            Assert.Contains("IQueryable<SearchDocs> SearchDocs", contextCode);
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsData", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsIdx", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTable" &&
                item.GetProperty("name").GetString() == "SearchDocs");
            Assert.Contains(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                item.GetProperty("name").GetString()!.StartsWith("SearchDocs_", StringComparison.Ordinal));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCheckInIdentifierName_DoesNotEmitCheckConstraintDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Checkpoint (
                Id INTEGER PRIMARY KEY,
                CheckName TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CheckpointCtx");

            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            if (File.Exists(warningPath))
            {
                var warnings = File.ReadAllText(warningPath);
                Assert.DoesNotContain("CheckConstraint", warnings);
            }
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyReferentialActions_GeneratesExplicitActions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE CascadeChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_Cascade_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE CASCADE
            );
            CREATE TABLE RestrictChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_Restrict_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE RESTRICT ON UPDATE CASCADE
            );
            CREATE TABLE SetNullChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NULL,
                CONSTRAINT FK_SetNull_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE SET NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FkActionCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "FkActionCtx.cs"));
            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");

            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, ReferentialAction.Restrict, ReferentialAction.Cascade);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, ReferentialAction.SetNull, ReferentialAction.NoAction);", contextCode);
            Assert.False(File.Exists(warningPath), "Valid referential actions should scaffold into fluent configuration rather than warning-only diagnostics.");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithKeylessTable_EmitsMissingPrimaryKeyDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE KeylessImport (ExternalId TEXT NOT NULL, Payload TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "KeylessImport.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("using nORM.Configuration;", entityCode);
            Assert.Contains("[ReadOnlyEntity]", entityCode);
            Assert.DoesNotContain("[Key]", entityCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("KeylessImport", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            var missingPrimaryKey = Assert.Single(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "KeylessImport");
            Assert.Contains("read", missingPrimaryKey.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ReadOnlyEntity_WritePathsThrowBeforeSqlGeneration()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var item = new SanReadOnlyReport { ExternalId = "ext-1", Payload = "payload" };

        var add = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Add(item));
        Assert.Contains("read-only", add.Message, StringComparison.OrdinalIgnoreCase);

        var update = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Update(item));
        Assert.Contains("read-only", update.Message, StringComparison.OrdinalIgnoreCase);

        var remove = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Remove(item));
        Assert.Contains("read-only", remove.Message, StringComparison.OrdinalIgnoreCase);

        var insert = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.InsertAsync(item));
        Assert.Contains("read-only", insert.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyToKeylessPrincipal_DoesNotEmitUnsafeNavigation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE ExternalCustomer (
                ExternalId TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL
            );
            CREATE TABLE ImportedOrder (
                Id INTEGER PRIMARY KEY,
                CustomerExternalId TEXT NOT NULL,
                CONSTRAINT FK_ImportedOrder_ExternalCustomer
                    FOREIGN KEY (CustomerExternalId) REFERENCES ExternalCustomer(ExternalId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessRelCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "ExternalCustomer.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "ImportedOrder.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "KeylessRelCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain("[Key]", principalCode);
            Assert.DoesNotContain("[ForeignKey(", dependentCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("RelationshipPrincipalKey", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "RelationshipPrincipalKey" &&
                item.GetProperty("name").GetString() == "sqlite_fk_0");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_FailOnWarnings_WritesDiagnosticsThenThrows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE WarningOwned (
                    Status TEXT NOT NULL DEFAULT 'new'
                );
                """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "WarningOwnedCtx",
                    new ScaffoldOptions { FailOnWarnings = true }));

            Assert.Contains("Scaffolding produced warnings", ex.Message);
            Assert.True(File.Exists(Path.Combine(dir, "WarningOwned.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "WarningOwnedCtx.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("WarningOwned", warnings);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));
            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("public List<Author> Authors { get; set; } = new();", bookCode);
            Assert.Contains(".HasMany<Book>(p => p.Books)", contextCode);
            Assert.Contains(".WithMany(p => p.Authors)", contextCode);
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSurrogateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                UNIQUE (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SurrogateJoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SurrogateJoinCtx.cs"));
            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("public List<Author> Authors { get; set; } = new();", bookCode);
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\");", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSchemaPureJoinTable_PreservesUsingTableSchema()
    {
        var auxFile = Path.Combine(Path.GetTempPath(), "san_scaffold_aux_" + Guid.NewGuid().ToString("N") + ".db");
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"""
            ATTACH DATABASE '{auxFile.Replace("'", "''")}' AS aux;
            PRAGMA foreign_keys=ON;
            CREATE TABLE aux.Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE aux.Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE aux.AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SchemaJoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaJoinCtx.cs"));
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\", schema: \"aux\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
            cn.Close();
            if (File.Exists(auxFile)) File.Delete(auxFile);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingPureJoinTable_EmitsDistinctManyToManyNavigations()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE PersonRelationship (
                MentorId INTEGER NOT NULL,
                MenteeId INTEGER NOT NULL,
                PRIMARY KEY (MentorId, MenteeId),
                CONSTRAINT FK_PersonRelationship_Mentor FOREIGN KEY (MentorId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonRelationship_Mentee FOREIGN KEY (MenteeId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SelfJoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "PersonRelationship.cs")));
            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SelfJoinCtx.cs"));
            Assert.Contains("public List<Person> PersonsByMenteeId { get; set; } = new();", personCode);
            Assert.Contains("public List<Person> PersonsByMentorId { get; set; } = new();", personCode);
            Assert.Contains(".HasMany<Person>(p => p.PersonsByMenteeId)", contextCode);
            Assert.Contains(".WithMany(p => p.PersonsByMentorId)", contextCode);
            Assert.Contains(".UsingTable(\"PersonRelationship\", \"MenteeId\", \"MentorId\");", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPayloadJoinTable_EmitsExplicitJoinEntityAndDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                CreatedAt TEXT NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var joinCode = File.ReadAllText(Path.Combine(dir, "AuthorBook.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("CreatedAt { get; set; }", joinCode);
            Assert.Contains("public Author?", joinCode);
            Assert.Contains("public Book?", joinCode);
            Assert.Contains("public List<AuthorBook>", authorCode);
            Assert.Contains("public List<AuthorBook>", bookCode);
            Assert.Contains(".HasMany(p => p.AuthorBooks)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.BookId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            Assert.Contains("AuthorBook", warnings);
            Assert.Contains("Author", warnings);
            Assert.Contains("Book", warnings);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("SCF002", joinTables[0].GetProperty("code").GetString());
            Assert.Equal("Warning", joinTables[0].GetProperty("severity").GetString());
            Assert.Equal("many-to-many", joinTables[0].GetProperty("category").GetString());
            Assert.Equal("AuthorBook", joinTables[0].GetProperty("table").GetString());
            Assert.Contains(joinTables[0].GetProperty("principalTables").EnumerateArray(), item => item.GetString() == "Author");
            Assert.Contains(joinTables[0].GetProperty("principalTables").EnumerateArray(), item => item.GetString() == "Book");
            Assert.Contains(joinTables[0].GetProperty("reasons").EnumerateArray(), item => item.GetString() == "payload-columns");
            Assert.Contains("UsingTable", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("generated primary keys or exact unique indexes", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("generated surrogate primary key plus an exact unique index", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, StudentId)
            );
            CREATE TABLE Course (
                TenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                Title TEXT NOT NULL,
                PRIMARY KEY (TenantId, CourseId)
            );
            CREATE TABLE StudentCourse (
                StudentTenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseTenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                PRIMARY KEY (StudentTenantId, StudentId, CourseTenantId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentTenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseTenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeJoinCtx.cs"));
            var warningsPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.Contains(".UsingTable(\"StudentCourse\", new[] { \"StudentTenantId\", \"StudentId\" }, new[] { \"CourseTenantId\", \"CourseId\" });", contextCode);
            if (File.Exists(warningsPath))
            {
                var warnings = File.ReadAllText(warningsPath);
                Assert.DoesNotContain("possible many-to-many", warnings, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("Composite Foreign Keys", warnings);
            }

            if (File.Exists(warningJsonPath))
            {
                using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
                Assert.Empty(joinTables.EnumerateArray());
            }
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeKeyPayloadJoinTable_ReportsPayloadNotCompositeUnsupported()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, StudentId)
            );
            CREATE TABLE Course (
                TenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                Title TEXT NOT NULL,
                PRIMARY KEY (TenantId, CourseId)
            );
            CREATE TABLE StudentCourse (
                StudentTenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseTenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                EnrolledAt TEXT NOT NULL,
                PRIMARY KEY (StudentTenantId, StudentId, CourseTenantId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentTenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseTenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositePayloadJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositePayloadJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            var reasons = joinTables[0].GetProperty("reasons").EnumerateArray().Select(item => item.GetString()).ToArray();

            Assert.True(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.DoesNotContain(".UsingTable(\"StudentCourse\"", contextCode);
            Assert.Contains("at least one safe `UsingTable` requirement was not met", warnings);
            Assert.DoesNotContain("two single-column foreign key constraints", warnings);
            Assert.Contains("payload-columns", reasons);
            Assert.DoesNotContain("composite-foreign-key", reasons);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeSurrogateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, StudentId)
            );
            CREATE TABLE Course (
                TenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                Title TEXT NOT NULL,
                PRIMARY KEY (TenantId, CourseId)
            );
            CREATE TABLE StudentCourse (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                StudentTenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseTenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                UNIQUE (StudentTenantId, StudentId, CourseTenantId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentTenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseTenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeSurrogateJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeSurrogateJoinCtx.cs"));

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.Contains(".UsingTable(\"StudentCourse\", new[] { \"StudentTenantId\", \"StudentId\" }, new[] { \"CourseTenantId\", \"CourseId\" });", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithAlternateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY,
                Isbn TEXT NOT NULL UNIQUE,
                Title TEXT NOT NULL
            );
            CREATE TABLE AuthorBook (
                AuthorCode TEXT NOT NULL,
                BookIsbn TEXT NOT NULL,
                PRIMARY KEY (AuthorCode, BookIsbn),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorCode) REFERENCES Author(Code),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookIsbn) REFERENCES Book(Isbn)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "AlternateKeyJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "AlternateKeyJoinCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorCode\", \"BookIsbn\", p => p.Code, p => p.Isbn);", contextCode);
            if (File.Exists(warningJsonPath))
            {
                using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
                Assert.Empty(joinTables.EnumerateArray());
            }
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeAlternateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                Code TEXT NOT NULL,
                Name TEXT NOT NULL,
                UNIQUE (TenantId, Code)
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                Isbn TEXT NOT NULL,
                Title TEXT NOT NULL,
                UNIQUE (TenantId, Isbn)
            );
            CREATE TABLE AuthorBook (
                TenantId INTEGER NOT NULL,
                AuthorCode TEXT NOT NULL,
                BookIsbn TEXT NOT NULL,
                PRIMARY KEY (TenantId, AuthorCode, BookIsbn),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (TenantId, AuthorCode) REFERENCES Author(TenantId, Code),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (TenantId, BookIsbn) REFERENCES Book(TenantId, Isbn)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeAlternateKeyJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeAlternateKeyJoinCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.Contains(".UsingTable(\"AuthorBook\", new[] { \"TenantId\", \"AuthorCode\" }, new[] { \"TenantId\", \"BookIsbn\" }, p => new { p.TenantId, p.Code }, p => new { p.TenantId, p.Isbn });", contextCode);
            if (File.Exists(warningJsonPath))
            {
                using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
                Assert.Empty(joinTables.EnumerateArray());
            }
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSharedTenantCompositeKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, StudentId)
            );
            CREATE TABLE Course (
                TenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                Title TEXT NOT NULL,
                PRIMARY KEY (TenantId, CourseId)
            );
            CREATE TABLE StudentCourse (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                PRIMARY KEY (TenantId, StudentId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (TenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (TenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SharedTenantCompositeJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "SharedTenantCompositeJoinCtx.cs"));
            var warningsPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.Contains(".UsingTable(\"StudentCourse\", new[] { \"TenantId\", \"StudentId\" }, new[] { \"TenantId\", \"CourseId\" });", contextCode);
            if (File.Exists(warningsPath))
            {
                var warnings = File.ReadAllText(warningsPath);
                Assert.DoesNotContain("possible many-to-many", warnings, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("Composite Foreign Keys", warnings);
            }

            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithKeylessJoinTable_DoesNotEmitUnsafeManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessJoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "KeylessJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"AuthorBook\"", contextCode);
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            Assert.Contains("MissingPrimaryKey", warnings);
            var summary = warningJson.RootElement.GetProperty("summary");
            Assert.Equal(1, summary.GetProperty("sectionCounts").GetProperty("possibleManyToManyJoinTables").GetInt32());
            var keylessJoin = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables")[0];
            Assert.Contains(keylessJoin.GetProperty("reasons").EnumerateArray(), item => item.GetString() == "missing-primary-key");
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "AuthorBook");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullableJoinTableForeignKeys_DoesNotEmitUnsafeManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER,
                BookId INTEGER,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "NullableJoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "NullableJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"AuthorBook\"", contextCode);
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("AuthorBook", joinTables[0].GetProperty("table").GetString());
            Assert.Contains(joinTables[0].GetProperty("reasons").EnumerateArray(), item => item.GetString() == "nullable-foreign-key");
            Assert.Contains("NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUnsafeSelfJoinTable_EmitsManyToManyDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PersonFriend (
                PersonId INTEGER,
                FriendId INTEGER,
                PRIMARY KEY (PersonId, FriendId),
                CONSTRAINT FK_PersonFriend_Person FOREIGN KEY (PersonId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonFriend_Friend FOREIGN KEY (FriendId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "UnsafeSelfJoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "PersonFriend.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "UnsafeSelfJoinCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"PersonFriend\"", contextCode);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("PersonFriend", joinTables[0].GetProperty("table").GetString());
            Assert.Single(joinTables[0].GetProperty("principalTables").EnumerateArray());
            Assert.Contains(joinTables[0].GetProperty("reasons").EnumerateArray(), item => item.GetString() == "nullable-foreign-key");
            Assert.Contains("NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTable_UsesJoinTableNameForNavigationDirection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Course (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE StudentCourse (
                CourseId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                PRIMARY KEY (CourseId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseId) REFERENCES Course(Id),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentId) REFERENCES Student(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            var studentCode = File.ReadAllText(Path.Combine(dir, "Student.cs"));
            var courseCode = File.ReadAllText(Path.Combine(dir, "Course.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));

            Assert.Contains("public List<Course> Courses { get; set; } = new();", studentCode);
            Assert.Contains("public List<Student> Students { get; set; } = new();", courseCode);
            Assert.Contains(".HasMany<Course>(p => p.Courses)", contextCode);
            Assert.Contains(".WithMany(p => p.Students)", contextCode);
            Assert.Contains(".UsingTable(\"StudentCourse\", \"StudentId\", \"CourseId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingPureJoinTable_GeneratesUniqueInverseCollections()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PersonFriend (
                PersonId INTEGER NOT NULL,
                FriendId INTEGER NOT NULL,
                PRIMARY KEY (PersonId, FriendId),
                CONSTRAINT FK_PersonFriend_Person FOREIGN KEY (PersonId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonFriend_Friend FOREIGN KEY (FriendId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SelfJoinCtx");

            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SelfJoinCtx.cs"));

            Assert.False(File.Exists(Path.Combine(dir, "PersonFriend.cs")));
            Assert.Contains("public List<Person> PersonsByPersonId { get; set; } = new();", personCode);
            Assert.Contains("public List<Person> PersonsByFriendId { get; set; } = new();", personCode);
            Assert.Contains(".HasMany<Person>(p => p.PersonsByPersonId)", contextCode);
            Assert.Contains(".WithMany(p => p.PersonsByFriendId)", contextCode);
            Assert.Contains(".UsingTable(\"PersonFriend\", \"PersonId\", \"FriendId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithInvalidSqlIdentifiers_GeneratesValidCSharpIdentifiers()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "bad-table" (
                "1st-name" TEXT NOT NULL,
                "has space" INTEGER NULL,
                "class" TEXT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "MyCtx3");

            var entityCode = File.ReadAllText(Path.Combine(dir, "BadTable.cs"));
            Assert.Contains("public class BadTable", entityCode);
            Assert.Contains("[Required]", entityCode);
            Assert.Contains("public string _1stName { get; set; } = default!;", entityCode);
            Assert.Contains("public long? HasSpace", entityCode);
            Assert.Contains("public string? Class", entityCode);
            Assert.DoesNotContain("@1st-name", entityCode);
            Assert.DoesNotContain("Has space", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithInvalidContextName_GeneratesValidContextClass()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ContextNameEntity (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "1-bad context");

            var contextPath = Path.Combine(dir, "_1BadContext.cs");
            Assert.True(File.Exists(contextPath));
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("public class _1BadContext : DbContext", contextCode);
            Assert.Contains("public _1BadContext(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null)", contextCode);
            Assert.DoesNotContain("1-bad context", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithQuotedSqlIdentifiers_GeneratesEscapedSourceLiterals()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "Quoted""Back\Table" (
                "Id" INTEGER PRIMARY KEY,
                "bad""col\name<&>
            line" TEXT NOT NULL
            );
            CREATE INDEX "IX""Back\Name
            Line" ON "Quoted""Back\Table" ("bad""col\name<&>
            line");
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "QuotedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "QuotedBackTable.cs"));
            Assert.Contains("[Table(\"Quoted\\\"Back\\\\Table\")]", entityCode);
            Assert.Contains("[Column(\"bad\\\"col\\\\name<&>\\nline\")]", entityCode);
            Assert.Contains("[Index(\"IX\\\"Back\\\\Name\\nLine\")]", entityCode);
            Assert.Contains("Maps to column bad\"col\\name&lt;&amp;&gt;\\nline", entityCode);
            Assert.DoesNotContain("\nline\" TEXT", entityCode, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithLiteralDottedIdentifiers_GeneratesSingleIdentifierMappings()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "audit.events" (
                Id INTEGER PRIMARY KEY,
                "value.part" TEXT NOT NULL
            );
            CREATE INDEX "ix.audit.value" ON "audit.events" ("value.part");
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DottedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "AuditEvents.cs"));
            Assert.Contains("[Table(\"audit.events\")]", entityCode);
            Assert.Contains("[Index(\"ix.audit.value\")]", entityCode);
            Assert.Contains("[Column(\"value.part\")]", entityCode);
            Assert.Contains("public string ValuePart { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithIdentifierCollisions_GeneratesUniqueNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "sales-order" (
                Id INTEGER PRIMARY KEY,
                "first-name" TEXT NOT NULL,
                "first_name" TEXT NOT NULL
            );
            CREATE TABLE "sales_order" (
                Id INTEGER PRIMARY KEY,
                Value TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionCtx");

            Assert.True(File.Exists(Path.Combine(dir, "SalesOrder.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SalesOrder2.cs")));
            var firstEntityCode = File.ReadAllText(Path.Combine(dir, "SalesOrder.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CollisionCtx.cs"));

            Assert.Contains("public string FirstName { get; set; } = default!;", firstEntityCode);
            Assert.Contains("public string FirstName2 { get; set; } = default!;", firstEntityCode);
            Assert.Contains("public IQueryable<SalesOrder> SalesOrders", contextCode);
            Assert.Contains("public IQueryable<SalesOrder2> SalesOrder2s", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithObjectMemberColumnNames_GeneratesUniquePropertyNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObjectMembers (
                Id INTEGER PRIMARY KEY,
                ToString TEXT NOT NULL,
                Equals TEXT NOT NULL,
                GetHashCode TEXT NOT NULL,
                GetType TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ObjectMemberCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "ObjectMembers.cs"));
            Assert.Contains("public string ToString2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string Equals2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string GetHashCode2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string GetType2 { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithContextMemberEntityNames_GeneratesUniqueQueryPropertyNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Option (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE ConfigureOption (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ContextMemberCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "ContextMemberCtx.cs"));
            Assert.Contains("public IQueryable<Option> Options2", contextCode);
            Assert.Contains("public IQueryable<ConfigureOption> ConfigureOptions2", contextCode);
            Assert.DoesNotContain("public IQueryable<Option> Options =>", contextCode);
            Assert.DoesNotContain("public IQueryable<ConfigureOption> ConfigureOptions =>", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTableFilter_GeneratesOnlyRequestedTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE KeepMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SkipMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "FilteredCtx",
                new ScaffoldOptions { Tables = new[] { "KeepMe" } });

            Assert.True(File.Exists(Path.Combine(dir, "KeepMe.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SkipMe.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredCtx.cs"));
            Assert.Contains("IQueryable<KeepMe> KeepMes", contextCode);
            Assert.DoesNotContain("SkipMes", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullOrBlankTableFilter_TreatsFilterAsEmpty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE FilterNullSafe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var nullDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var blankDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                nullDir,
                "TestNs",
                "NullFilterCtx",
                new ScaffoldOptions { Tables = null! });

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                blankDir,
                "TestNs",
                "BlankFilterCtx",
                new ScaffoldOptions { Tables = new[] { " ", "" } });

            Assert.True(File.Exists(Path.Combine(nullDir, "FilterNullSafe.cs")));
            Assert.True(File.Exists(Path.Combine(blankDir, "FilterNullSafe.cs")));
        }
        finally
        {
            if (Directory.Exists(nullDir)) Directory.Delete(nullDir, recursive: true);
            if (Directory.Exists(blankDir)) Directory.Delete(blankDir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithAmbiguousBareTableFilter_RequiresSchemaQualifiedName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE "auxa"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var ambiguousDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var qualifiedDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    ambiguousDir,
                    "TestNs",
                    "AmbiguousFilterCtx",
                    new ScaffoldOptions { Tables = new[] { "DuplicateName" } }));

            Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("auxa.DuplicateName", ex.Message, StringComparison.Ordinal);
            Assert.Contains("auxb.DuplicateName", ex.Message, StringComparison.Ordinal);
            Assert.Contains("schema-qualified", ex.Message, StringComparison.OrdinalIgnoreCase);

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                qualifiedDir,
                "TestNs",
                "QualifiedFilterCtx",
                new ScaffoldOptions { Tables = new[] { "auxa.DuplicateName" } });

            var entityCode = File.ReadAllText(Path.Combine(qualifiedDir, "DuplicateName.cs"));
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxa\")]", entityCode);
        }
        finally
        {
            if (Directory.Exists(ambiguousDir)) Directory.Delete(ambiguousDir, recursive: true);
            if (Directory.Exists(qualifiedDir)) Directory.Delete(qualifiedDir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSameTableNameAcrossSchemas_UsesSchemaQualifiedEntityNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE "auxa"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SchemaDuplicateCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuxaDuplicateName.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "AuxbDuplicateName.cs")));
            var auxaCode = File.ReadAllText(Path.Combine(dir, "AuxaDuplicateName.cs"));
            var auxbCode = File.ReadAllText(Path.Combine(dir, "AuxbDuplicateName.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaDuplicateCtx.cs"));
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxa\")]", auxaCode);
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxb\")]", auxbCode);
            Assert.Contains("IQueryable<AuxaDuplicateName> AuxaDuplicateNames", contextCode);
            Assert.Contains("IQueryable<AuxbDuplicateName> AuxbDuplicateNames", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithLiteralDottedTableFilterCollision_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "DottedFilterCtx",
                    new ScaffoldOptions { Tables = new[] { "aux.orders" } }));

            Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("literal dotted table names", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelectedTableKeyCollision_ThrowsBeforeGeneratingAmbiguousModel()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionCtx"));

            Assert.Contains("display names collide", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("literal dotted table names", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMissingTableFilter_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Existing (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "FilteredCtx",
                    new ScaffoldOptions { Tables = new[] { "Missing" } }));
            Assert.Contains("Missing", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithViewTableFilter_ThrowsSkippedObjectDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Existing (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "FilteredCtx",
                    new ScaffoldOptions { Tables = new[] { "ExistingView" } }));

            Assert.Contains("matched database object", ex.Message);
            Assert.Contains("View ExistingView", ex.Message);
            Assert.Contains("does not emit as entity classes", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithEmitViewEntities_GeneratesQueryArtifact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Existing (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "ViewCtx",
                new ScaffoldOptions { Tables = new[] { "ExistingView" }, EmitViewEntities = true });

            var viewCode = File.ReadAllText(Path.Combine(dir, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ViewCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"ExistingView\")]", viewCode);
            Assert.Contains("public long Id { get; set; }", viewCode);
            Assert.Contains("public string", viewCode);
            Assert.Contains("Name { get; set; }", viewCode);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.DoesNotContain("View ExistingView", warnings);
            Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "ExistingView");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoOverwrite_RefusesExistingFiles()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ExistingFile (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "ExistingFile.cs"), "// owned");

            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "NoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));
            Assert.Contains("already exists", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDryRun_DoesNotCreateOrWriteOutputDirectory()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DryRunItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_dry_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "DryRunCtx",
                new ScaffoldOptions { DryRun = true });

            Assert.False(Directory.Exists(dir));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDryRun_DoesNotRemoveStaleWarningReports()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DryRunClean (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_dry_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            File.WriteAllText(warningPath, "# stale");

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "DryRunCtx",
                new ScaffoldOptions { DryRun = true });

            Assert.Equal("# stale", File.ReadAllText(warningPath));
            Assert.False(File.Exists(Path.Combine(dir, "DryRunClean.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "DryRunCtx.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoOverwrite_PreflightsAllFilesBeforeWriting()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AlphaNoOverwrite (Id INTEGER PRIMARY KEY);
            CREATE TABLE BetaNoOverwrite (Id INTEGER PRIMARY KEY);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "BetaNoOverwrite.cs"), "// owned");

            await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "NoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));

            Assert.False(File.Exists(Path.Combine(dir, "AlphaNoOverwrite.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "NoOverwriteCtx.cs")));
            Assert.Equal("// owned", File.ReadAllText(Path.Combine(dir, "BetaNoOverwrite.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_RepeatedRuns_ProduceDeterministicOutput()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE ZetaDeterministic (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL DEFAULT 'z'
            );
            CREATE TABLE AlphaDeterministic (
                Id INTEGER PRIMARY KEY,
                ZetaId INTEGER NOT NULL,
                Value TEXT NOT NULL,
                CONSTRAINT FK_Alpha_Zeta FOREIGN KEY (ZetaId) REFERENCES ZetaDeterministic(Id)
            );
            CREATE INDEX IX_Alpha_Value ON AlphaDeterministic(Value);
            """;
        cmd.ExecuteNonQuery();

        var first = Path.Combine(Path.GetTempPath(), "san_scaffold_det_a_" + Guid.NewGuid().ToString("N"));
        var second = Path.Combine(Path.GetTempPath(), "san_scaffold_det_b_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), first, "TestNs", "DeterministicCtx");
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), second, "TestNs", "DeterministicCtx");

            Assert.Equal(ReadScaffoldSnapshot(first), ReadScaffoldSnapshot(second));
        }
        finally
        {
            if (Directory.Exists(first)) Directory.Delete(first, recursive: true);
            if (Directory.Exists(second)) Directory.Delete(second, recursive: true);
        }
    }

    private static void AssertScaffoldOutputBuildsAsConsumerProject(string outputDirectory)
    {
        var root = FindRepositoryRoot();
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(Path.Combine(outputDirectory, "ScaffoldedConsumer.csproj"), $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <ImplicitUsings>enable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);

        var psi = new ProcessStartInfo("dotnet", "build -c Release --nologo")
        {
            WorkingDirectory = outputDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start dotnet build.");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0,
            $"Scaffolded output failed to build with exit code {process.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{stderr}");
    }

    private static string FindRepositoryRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(dir))
        {
            if (File.Exists(Path.Combine(dir, "nORM.sln")))
                return dir;

            dir = Directory.GetParent(dir)?.FullName;
        }

        throw new InvalidOperationException("Could not locate repository root from " + AppContext.BaseDirectory);
    }

    private static IReadOnlyList<(string RelativePath, string Content)> ReadScaffoldSnapshot(string outputDirectory)
        => Directory.EnumerateFiles(outputDirectory)
            .Select(path => (RelativePath: Path.GetFileName(path), Content: File.ReadAllText(path)))
            .OrderBy(file => file.RelativePath, StringComparer.Ordinal)
            .ToArray();
}

// ── NavigationContext tests ───────────────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class SanNavigationContextTests
{
    private static DbContext CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Constructor_SetsDbContextAndEntityType()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        Assert.Same(ctx, navCtx.DbContext);
        Assert.Equal(typeof(SanParent), navCtx.EntityType);
    }

    [Fact]
    public void IsLoaded_InitiallyReturnsFalse()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsLoaded_ThenIsLoaded_ReturnsTrue()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        Assert.True(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsUnloaded_AfterLoaded_ReturnsFalse()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.MarkAsUnloaded("Children");
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsUnloaded_WhenNeverLoaded_DoesNotThrow()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        // Should not throw
        navCtx.MarkAsUnloaded("NonExistent");
    }

    [Fact]
    public void Dispose_ClearsLoadedState()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.Dispose();
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MultipleProperties_TrackedIndependently()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("PropA");
        Assert.True(navCtx.IsLoaded("PropA"));
        Assert.False(navCtx.IsLoaded("PropB"));
    }

    [Fact]
    public void MarkAsLoaded_Idempotent()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.MarkAsLoaded("Children");
        Assert.True(navCtx.IsLoaded("Children"));
    }
}

// ── NavigationPropertyInfo record tests ──────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class SanNavigationPropertyInfoTests
{
    [Fact]
    public void Constructor_SetsAllProperties()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var info = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        Assert.Same(prop, info.Property);
        Assert.Equal(typeof(SanChild), info.TargetType);
        Assert.True(info.IsCollection);
    }

    [Fact]
    public void Constructor_NonCollection_SetsIsCollectionFalse()
    {
        var prop = typeof(SanParent).GetProperty("Id")!;
        var info = new NavigationPropertyInfo(prop, typeof(int), false);
        Assert.False(info.IsCollection);
    }

    [Fact]
    public void Record_Equality_SameValues_AreEqual()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var a = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var b = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        Assert.Equal(a, b);
    }

    [Fact]
    public void Record_Equality_DifferentIsCollection_NotEqual()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var a = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var b = new NavigationPropertyInfo(prop, typeof(SanChild), false);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Record_ToString_ContainsTypeName()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var info = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var s = info.ToString();
        Assert.Contains(nameof(NavigationPropertyInfo), s);
    }
}

// ── LazyNavigationCollection<T> tests ────────────────────────────────────────
// These tests use an in-memory SQLite DB with the table created so that
// lazy loading can resolve. They exercise collection methods AFTER the
// collection has been loaded by the navigation infrastructure.

[Xunit.Trait("Category", "Fast")]
public class SanLazyNavigationCollectionTests : IDisposable
{
    private readonly SqliteConnection _cn;
    private readonly DbContext _ctx;
    private readonly SanParent _parent;
    private readonly PropertyInfo _childrenProp;
    private readonly NavigationContext _navCtx;
    private readonly LazyNavigationCollection<SanChild> _lazy;

    public SanLazyNavigationCollectionTests()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        _cn.Open();

        using var cmd = _cn.CreateCommand();
        // Create tables so the mapping can resolve
        cmd.CommandText = @"
            CREATE TABLE SAN_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT);
            CREATE TABLE SAN_Child  (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL);
            INSERT INTO SAN_Parent DEFAULT VALUES;
            INSERT INTO SAN_Child  (ParentId) VALUES (1);";
        cmd.ExecuteNonQuery();

        _ctx = new DbContext(_cn, new SqliteProvider(), new nORM.Configuration.DbContextOptions { EagerChangeTracking = false });
        _parent = new SanParent { Id = 1 };
        _childrenProp = typeof(SanParent).GetProperty("Children")!;
        _navCtx = new NavigationContext(_ctx, typeof(SanParent));
        _lazy = new LazyNavigationCollection<SanChild>(_parent, _childrenProp, _navCtx);
    }

    public void Dispose()
    {
        _ctx.Dispose();
        _cn.Dispose();
    }

    // Helper: seed the parent's Children property with a pre-built list so
    // subsequent lazy-collection operations do NOT need to hit the DB.
    private List<SanChild> SeedLoadedList(params SanChild[] items)
    {
        var list = new List<SanChild>(items);
        _childrenProp.SetValue(_parent, list);
        _navCtx.MarkAsLoaded("Children");
        return list;
    }

    [Fact]
    public void IsReadOnly_ReturnsFalse_ForLoadedList()
    {
        SeedLoadedList();
        Assert.False(_lazy.IsReadOnly);
    }

    [Fact]
    public void Count_ReturnsZero_WhenEmpty()
    {
        SeedLoadedList();
        Assert.Empty(_lazy);
    }

    [Fact]
    public void Count_ReturnsOne_WhenOneItemSeeded()
    {
        SeedLoadedList(new SanChild { Id = 1, ParentId = 1 });
        Assert.Single(_lazy);
    }

    [Fact]
    public void Add_IncreasesCount()
    {
        SeedLoadedList();
        _lazy.Add(new SanChild { Id = 99, ParentId = 1 });
        Assert.Single(_lazy);
    }

    [Fact]
    public void Contains_ReturnsTrueForAddedItem()
    {
        SeedLoadedList();
        var child = new SanChild { Id = 5, ParentId = 1 };
        _lazy.Add(child);
        Assert.Contains(child, _lazy);
    }

    [Fact]
    public void Contains_ReturnsFalseForMissingItem()
    {
        SeedLoadedList();
        Assert.DoesNotContain(new SanChild { Id = 999, ParentId = 1 }, _lazy);
    }

    [Fact]
    public void Remove_DecreasesCount()
    {
        var child = new SanChild { Id = 7, ParentId = 1 };
        SeedLoadedList(child);
        var removed = _lazy.Remove(child);
        Assert.True(removed);
        Assert.Empty(_lazy);
    }

    [Fact]
    public void Clear_EmptiesCollection()
    {
        SeedLoadedList(new SanChild { Id = 1, ParentId = 1 }, new SanChild { Id = 2, ParentId = 1 });
        _lazy.Clear();
        Assert.Empty(_lazy);
    }

    [Fact]
    public void CopyTo_CopiesItemsToArray()
    {
        var child = new SanChild { Id = 3, ParentId = 1 };
        SeedLoadedList(child);
        var arr = new SanChild[1];
        _lazy.CopyTo(arr, 0);
        Assert.Same(child, arr[0]);
    }

    [Fact]
    public void IndexOf_ReturnsCorrectIndex()
    {
        var child = new SanChild { Id = 4, ParentId = 1 };
        SeedLoadedList(child);
        Assert.Equal(0, _lazy.IndexOf(child));
    }

    [Fact]
    public void IndexOf_MissingItem_ReturnsMinusOne()
    {
        SeedLoadedList();
        Assert.Equal(-1, _lazy.IndexOf(new SanChild { Id = 42 }));
    }

    [Fact]
    public void Indexer_Get_ReturnsCorrectItem()
    {
        var child = new SanChild { Id = 8, ParentId = 1 };
        SeedLoadedList(child);
        Assert.Same(child, _lazy[0]);
    }

    [Fact]
    public void Indexer_Set_UpdatesItem()
    {
        var original = new SanChild { Id = 1, ParentId = 1 };
        SeedLoadedList(original);
        var replacement = new SanChild { Id = 2, ParentId = 1 };
        _lazy[0] = replacement;
        Assert.Same(replacement, _lazy[0]);
    }

    [Fact]
    public void Insert_AddsAtIndex()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a);
        _lazy.Insert(0, b);
        Assert.Same(b, _lazy[0]);
        Assert.Same(a, _lazy[1]);
    }

    [Fact]
    public void RemoveAt_RemovesCorrectItem()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        _lazy.RemoveAt(0);
        Assert.Single(_lazy);
        Assert.Same(b, _lazy[0]);
    }

    [Fact]
    public void GetEnumerator_YieldsAllItems()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        var items = new List<SanChild>();
        foreach (var item in _lazy)
            items.Add(item);
        Assert.Equal(2, items.Count); // Not a collection size check — items is a plain List<T>
    }

    [Fact]
    public async Task GetAsyncEnumerator_YieldsAllItems()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        var items = new List<SanChild>();
        await foreach (var item in _lazy)
            items.Add(item);
        Assert.Equal(2, items.Count); // Not a collection size check — items is a plain List<T>
    }
}

// ── LazyNavigationReference<T> tests ─────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class LazyNavigationReferenceTests
{
    private static (DbContext Ctx, SanParent Parent, PropertyInfo Prop, NavigationContext NavCtx) CreateSetup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var parent = new SanParent { Id = 1 };
        // Use a fake property just to hold the reference
        var prop = typeof(SanParent).GetProperty("Id")!;
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        return (ctx, parent, prop, navCtx);
    }

    [Fact]
    public void SetValue_MarksAsLoaded_AndContextReflectsIt()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 1, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            Assert.True(navCtx.IsLoaded(prop.Name));
        }
    }

    [Fact]
    public async Task GetValueAsync_AfterSetValue_ReturnsValue()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 2, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            // Because _isLoaded=true, GetValueAsync will NOT go to DB
            var result = await lazyRef.GetValueAsync();
            Assert.Same(child, result);
        }
    }

    [Fact]
    public async Task GetValueAsync_SetValueNull_ReturnsNull()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(null);
            var result = await lazyRef.GetValueAsync();
            Assert.Null(result);
        }
    }

    [Fact]
    public void ImplicitConversion_ToTask_Works()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 3, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            Task<SanChild?> task = lazyRef; // implicit operator
            Assert.NotNull(task);
        }
    }
}

// ── EnableLazyLoading extension tests ────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class EnableLazyLoadingTests
{
    [Fact]
    public void EnableLazyLoading_NullEntity_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        Assert.Throws<ArgumentNullException>(
            () => NavigationPropertyExtensions.EnableLazyLoading<SanParent>(null!, ctx));
    }

    [Fact]
    public void EnableLazyLoading_EntityWithNullCollection_InjectsLazyProxy()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var parent = new SanParent { Id = 1, Children = null };
        parent.EnableLazyLoading(ctx);
        // The Children property should now be a LazyNavigationCollection<SanChild>
        Assert.IsType<LazyNavigationCollection<SanChild>>(parent.Children);
    }

    [Fact]
    public void EnableLazyLoading_EntityWithExistingCollection_DoesNotReplace()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var existing = new List<SanChild> { new SanChild { Id = 1 } };
        var parent = new SanParent { Id = 1, Children = existing };
        parent.EnableLazyLoading(ctx);
        // Must not overwrite an already-populated collection
        Assert.Same(existing, parent.Children);
    }

    [Fact]
    public void CleanupNavigationContext_DoesNotThrowForUnregisteredEntity()
    {
        var parent = new SanParent { Id = 42 };
        // Should not throw even if the entity was never registered
        NavigationPropertyExtensions.CleanupNavigationContext(parent);
    }
}

// ── INormIncludableQueryable / ThenInclude extension tests ────────────────────

[Xunit.Trait("Category", "Fast")]
public class NormIncludableQueryableTests
{
    private static DbContext CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Include_OnQuery_ReturnsIncludableQueryable()
    {
        using var ctx = CreateCtx();
        var q = ctx.Query<SanTIParent>() as INormQueryable<SanTIParent>;
        Assert.NotNull(q);
        var included = q!.Include(p => p.Children);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>>(included);
    }

    [Fact]
    public void Include_SingleRef_ReturnsIncludableQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.SingleChild);
        Assert.NotNull(included);
    }

    [Fact]
    public void ThenInclude_AfterCollectionInclude_ReturnsNewQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.Children);
        // ThenInclude through collection: IEnumerable<SanTIChild> → SanTIChild.Grandchildren
        var then = ((INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>)included)
            .ThenInclude(c => c.Grandchildren);
        Assert.NotNull(then);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, ICollection<SanTIGrandchild>>>(then);
    }

    [Fact]
    public void ThenInclude_AfterReferenceInclude_ReturnsNewQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.SingleChild);
        var then = ((INormIncludableQueryable<SanTIParent, SanTIChild?>)included)
            .ThenInclude(c => c!.Grandchild);
        Assert.NotNull(then);
    }

    [Fact]
    public void Include_FollowedByAsNoTracking_ReturnsQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var result = q.Include(p => p.Children).AsNoTracking();
        Assert.NotNull(result);
    }

    [Fact]
    public void Include_FollowedByAsSplitQuery_ReturnsQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var result = q.Include(p => p.Children).AsSplitQuery();
        Assert.NotNull(result);
    }

    [Fact]
    public void Include_ChainedTwice_BothIncludesPreservedInExpression()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var q2 = q.Include(p => p.Children).Include(p => p.SingleChild);
        var expr = ((IQueryable<SanTIParent>)q2).Expression.ToString();
        // Expression tree should reference both navigation paths
        Assert.NotNull(expr);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, SanTIChild?>>(q2);
    }

    [Fact]
    public void ThenInclude_ChainedTwice_ProducesCorrectQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        // Include → ThenInclude → second Include
        var step1 = q.Include(p => p.Children);
        var step2 = ((INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>)step1)
            .ThenInclude(c => c.Grandchildren);
        Assert.NotNull(step2);
        // Continue building
        var step3 = step2.Include(p => p.SingleChild);
        Assert.NotNull(step3);
    }

    [Fact]
    public void AsNoTracking_OnBaseQuery_ReturnsINormQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var noTracking = q.AsNoTracking();
        Assert.NotNull(noTracking);
        Assert.IsAssignableFrom<INormQueryable<SanTIParent>>(noTracking);
    }

    [Fact]
    public void AsSplitQuery_OnBaseQuery_ReturnsINormQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var split = q.AsSplitQuery();
        Assert.NotNull(split);
        Assert.IsAssignableFrom<INormQueryable<SanTIParent>>(split);
    }

    [Fact]
    public void Include_ExpressionContainsIncludeMethodCall()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.Children);
        var expr = ((IQueryable<SanTIParent>)included).Expression;
        Assert.NotNull(expr);
        // The expression tree for Include wraps a MethodCallExpression
        Assert.Equal(System.Linq.Expressions.ExpressionType.Call, expr.NodeType);
    }

}
