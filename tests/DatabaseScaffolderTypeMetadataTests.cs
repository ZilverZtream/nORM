#nullable enable

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
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
        var result = ScaffoldSqlMetadataParser.TryParseDecimalPrecision(typeName, out var precision, out var scale);

        Assert.Equal(expected, result);
        Assert.Equal(expectedPrecision, precision);
        Assert.Equal(expectedScale, scale);
    }

    [Theory]
    [InlineData("VARCHAR(40)", true, 40, false)]
    [InlineData("CHAR(12)", true, 12, true)]
    [InlineData("NATIVE   CHARACTER(20)", true, 20, true)]
    [InlineData("VARBINARY(16)", true, 16, false)]
    [InlineData("BINARY(8)", true, 8, true)]
    [InlineData("BLOB", false, null, false)]
    [InlineData("VARCHAR(max)", false, null, false)]
    [InlineData("VARCHAR(0)", false, null, false)]
    public void SqliteDeclaredStringBinaryFacetParser_ParsesBoundedDeclaredTypes(
        string declaredType,
        bool expected,
        int? expectedMaxLength,
        bool expectedFixedLength)
    {
        var result = ScaffoldSqliteDdlParser.TryParseDeclaredStringBinaryFacet(declaredType, out var facet);

        Assert.Equal(expected, result);
        if (!expected)
            return;

        Assert.Equal(expectedMaxLength, facet.MaxLength);
        Assert.Null(facet.IsUnicode);
        Assert.Equal(expectedFixedLength, facet.IsFixedLength);
    }

    [Theory]
    [InlineData("DECIMAL(28,6)", true, 28, 6)]
    [InlineData("NUMERIC(10)", true, 10, null)]
    [InlineData("NOTNUMERIC(10,2)", false, null, null)]
    [InlineData("DECIMAL(4,5)", false, null, null)]
    public void SqliteDeclaredDecimalPrecisionParser_UsesNumericDeclaredTypes(
        string declaredType,
        bool expected,
        int? expectedPrecision,
        int? expectedScale)
    {
        var result = ScaffoldSqliteDdlParser.TryParseDeclaredDecimalPrecision(declaredType, out var precision);

        Assert.Equal(expected, result);
        if (!expected)
            return;

        Assert.Equal(expectedPrecision, precision.Precision);
        Assert.Equal(expectedScale, precision.Scale);
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

        bool ShouldMarkReadOnly(
            string tableKey,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes)
            => ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
                tableKey,
                emptyTables,
                emptyTables,
                emptyTables,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypes,
                primaryKeys);

        Assert.True(ShouldMarkReadOnly("dbo.Orders", identityStrategyTables, emptyTables, null));
        Assert.True(ShouldMarkReadOnly("dbo.AuditRows", emptyTables, defaultTables, null));
        Assert.False(ShouldMarkReadOnly("dbo.Orders", emptyTables, emptyTables, null));
        Assert.False(ShouldMarkReadOnly(
            "public.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Email"] = "DOMAIN (public.email_ci -> USER-DEFINED (citext))" }));
        Assert.False(ShouldMarkReadOnly(
            "public.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Email"] = "USER-DEFINED (citext)" }));
        Assert.False(ShouldMarkReadOnly(
            "public.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Status"] = "DOMAIN (public.customer_status_domain -> ENUM (public.customer_status: 'draft','active','archived'))" }));
        Assert.False(ShouldMarkReadOnly(
            "dbo.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Email"] = "user-defined type (dbo.EmailAddress -> nvarchar(320))" }));
        Assert.False(ShouldMarkReadOnly(
            "dbo.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Amount"] = "user-defined type (dbo.MoneyAmount -> decimal(18,4))" }));
        Assert.True(ShouldMarkReadOnly(
            "public.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Network"] = "DOMAIN (public.network_address -> inet)" }));
        Assert.True(ShouldMarkReadOnly(
            "dbo.Customers",
            emptyTables,
            emptyTables,
            new Dictionary<string, string> { ["Shape"] = "user-defined type (dbo.Shape -> geography)" }));
    }

    [Fact]
    public void ShouldMarkScaffoldedEntityReadOnly_UsesAggregatedProviderOwnedWriteBlockingSet()
    {
        var emptyTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var writeBlockedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "dbo.Orders"
        };
        var primaryKeys = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["dbo.Orders"] = new[] { "Id" },
            ["dbo.Customers"] = new[] { "Id" }
        };

        Assert.True(ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
            "dbo.Orders",
            emptyTables,
            writeBlockedTables,
            primaryKeys));
        Assert.False(ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
            "dbo.Customers",
            emptyTables,
            writeBlockedTables,
            primaryKeys));
    }

    [Theory]
    [InlineData("ARRAY (_int4)", typeof(int[]))]
    [InlineData("ARRAY (_text)", typeof(string[]))]
    [InlineData("ARRAY (_citext)", typeof(string[]))]
    [InlineData("ARRAY (_uuid)", typeof(Guid[]))]
    [InlineData("ARRAY (_bytea)", typeof(byte[][]))]
    [InlineData("ARRAY (_time)", typeof(TimeOnly[]))]
    [InlineData("ARRAY (_timetz)", typeof(DateTimeOffset[]))]
    [InlineData("ARRAY (_interval)", typeof(TimeSpan[]))]
    [InlineData("ARRAY (_timestamptz)", typeof(DateTimeOffset[]))]
    [InlineData("ARRAY (varchar(64))", typeof(string[]))]
    [InlineData("ARRAY (character varying(64))", typeof(string[]))]
    [InlineData("ARRAY (numeric(10,2))", typeof(decimal[]))]
    [InlineData("character varying(64)[]", typeof(string[]))]
    [InlineData("numeric(10,2)[]", typeof(decimal[]))]
    [InlineData("DOMAIN (public.score_values -> ARRAY (numeric(10,2)))", typeof(decimal[]))]
    [InlineData("DOMAIN (public.offset_times -> ARRAY (_timetz))", typeof(DateTimeOffset[]))]
    [InlineData("time with time zone[]", typeof(DateTimeOffset[]))]
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
        Assert.Equal(expected, ScaffoldSkippedObjectMetadataBuilder.IsTableLikeSqlServerSynonym(detail));
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
}
