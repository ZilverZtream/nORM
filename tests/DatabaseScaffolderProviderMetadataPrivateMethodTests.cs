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

public partial class DatabaseScaffolderPrivateMethodTests
{
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
    [InlineData("set ('read', 'write')", false)]
    [InlineData("set('a','b','c','d','e','f','g','h')", false)]
    [InlineData("set('a','b','c','d','e','f','g','h','i')", true)]
    [InlineData("set('read,write','admin')", true)]
    [InlineData(@"set('read\,write','admin')", true)]
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
    [InlineData("INT(10) UNSIGNED ZEROFILL", typeof(uint))]
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
    [InlineData("sqlserver", "date", typeof(DateTime), typeof(DateOnly))]
    [InlineData("sqlserver", "time", typeof(TimeSpan), typeof(TimeOnly))]
    [InlineData("sqlserver", "datetimeoffset(7)", typeof(DateTime), typeof(DateTimeOffset))]
    [InlineData("sqlserver", "uniqueidentifier", typeof(object), typeof(Guid))]
    [InlineData("postgres", "date", typeof(DateTime), typeof(DateOnly))]
    [InlineData("postgres", "time without time zone", typeof(TimeSpan), typeof(TimeOnly))]
    [InlineData("postgres", "timestamp with time zone", typeof(DateTime), typeof(DateTimeOffset))]
    [InlineData("postgres", "interval", typeof(TimeSpan), typeof(TimeSpan))]
    [InlineData("postgres", "uuid", typeof(object), typeof(Guid))]
    [InlineData("mysql", "date", typeof(DateTime), typeof(DateOnly))]
    [InlineData("mysql", "datetime(6)", typeof(DateTime), typeof(DateTime))]
    [InlineData("mysql", "timestamp", typeof(DateTime), typeof(DateTime))]
    [InlineData("sqlite", "DATE", typeof(string), typeof(DateOnly))]
    [InlineData("sqlite", "TIME", typeof(string), typeof(TimeOnly))]
    [InlineData("sqlite", "DATETIME", typeof(string), typeof(DateTime))]
    [InlineData("sqlite", "DATETIMEOFFSET", typeof(string), typeof(DateTimeOffset))]
    [InlineData("sqlite", "UUID", typeof(string), typeof(Guid))]
    public void NormalizeScaffoldClrType_MapsUnambiguousCatalogStoreTypes_StaticAndDynamic(
        string providerName,
        string storeType,
        Type rawClrType,
        Type expected)
    {
        var method = GetMethod(
            "NormalizeScaffoldClrType",
            new[] { typeof(DatabaseProvider), typeof(Type), typeof(bool), typeof(bool), typeof(bool), typeof(string), typeof(string), typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("NormalizeScaffoldClrType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(DbConnection), typeof(Type), typeof(bool), typeof(bool), typeof(bool), typeof(string), typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "NormalizeScaffoldClrType");

        using var dynamicConnection = CreateDynamicStoreTypeProbeConnection(providerName);
        var staticResult = (Type)method.Invoke(
            null,
            new object?[] { CreateStoreTypeProbeProvider(providerName), rawClrType, false, false, false, null, null, storeType })!;
        var dynamicResult = (Type)dynamicMethod.Invoke(
            null,
            new object?[] { dynamicConnection, rawClrType, false, false, false, null, storeType })!;

        Assert.Equal(expected, staticResult);
        Assert.Equal(expected, dynamicResult);
    }

    [Fact]
    public void NormalizeScaffoldClrType_DoesNotGuessAmbiguousMySqlTimeStoreType()
    {
        var method = GetMethod(
            "NormalizeScaffoldClrType",
            new[] { typeof(DatabaseProvider), typeof(Type), typeof(bool), typeof(bool), typeof(bool), typeof(string), typeof(string), typeof(string) });
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("NormalizeScaffoldClrType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(DbConnection), typeof(Type), typeof(bool), typeof(bool), typeof(bool), typeof(string), typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "NormalizeScaffoldClrType");

        using var dynamicConnection = CreateDynamicStoreTypeProbeConnection("mysql");
        var staticResult = (Type)method.Invoke(
            null,
            new object?[] { CreateStoreTypeProbeProvider("mysql"), typeof(TimeSpan), false, false, false, null, null, "time" })!;
        var dynamicResult = (Type)dynamicMethod.Invoke(
            null,
            new object?[] { dynamicConnection, typeof(TimeSpan), false, false, false, null, "time" })!;

        Assert.Equal(typeof(TimeSpan), staticResult);
        Assert.Equal(typeof(TimeSpan), dynamicResult);
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
    [InlineData("set ('read', 'write')", true, 2)]
    [InlineData("set('a','b','c','d','e','f','g','h')", true, 8)]
    [InlineData("set('a','b','c','d','e','f','g','h','i')", false, 0)]
    [InlineData("set('read,write','admin')", false, 0)]
    [InlineData(@"set('read\,write','admin')", false, 0)]
    [InlineData(@"set('read\'write','admin')", true, 2)]
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
    [InlineData("MACADDR8", true)]
    [InlineData("TSVECTOR", true)]
    [InlineData("TSQUERY", true)]
    [InlineData("TEXT_TSVECTOR", true)]
    [InlineData("QUERYTEXT", false)]
    [InlineData("VECTORCLOCK", false)]
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
    [InlineData("MACADDR8", true)]
    [InlineData("TSVECTOR", true)]
    [InlineData("TSQUERY", true)]
    [InlineData("TEXT_TSVECTOR", true)]
    [InlineData("QUERYTEXT", false)]
    [InlineData("VECTORCLOCK", false)]
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
    [InlineData("enum ('draft', 'paid')", true)]
    [InlineData(@"enum('draft','it\'s','comma,value')", true)]
    [InlineData("enum('draft' 'paid')", false)]
    [InlineData("enum('draft',)", false)]
    [InlineData("enum(,'draft')", false)]
    [InlineData("ENUM (public.customer_status: 'draft','active','archived')", true)]
    [InlineData("ENUM (public.customer_status: 'draft', 'active')", true)]
    [InlineData("ENUM (public.customer_status: 'draft' 'active')", false)]
    [InlineData("ENUM (public.customer_status: 'draft',)", false)]
    [InlineData("ENUM (public.customer_status: ,'draft')", false)]
    [InlineData("set('read','write','admin')", true)]
    [InlineData("set ('read', 'write')", true)]
    [InlineData(@"set('read\'write','admin')", true)]
    [InlineData("set('a','b','c','d','e','f','g','h')", true)]
    [InlineData("set('a','b','c','d','e','f','g','h','i')", false)]
    [InlineData("set('read,write','admin')", false)]
    [InlineData(@"set('read\,write','admin')", false)]
    [InlineData("ARRAY (_int4)", true)]
    [InlineData("ARRAY (varchar(64))", true)]
    [InlineData("ARRAY (numeric(10,2))", true)]
    [InlineData("integer[]", true)]
    [InlineData("int4[]", true)]
    [InlineData("uuid[]", true)]
    [InlineData("varchar[]", true)]
    [InlineData("character varying(64)[]", true)]
    [InlineData("numeric(10,2)[]", true)]
    [InlineData("inet[]", false)]
    [InlineData("DOMAIN (public.email_address -> character varying)", false)]
    [InlineData("DOMAIN (public.score_values -> ARRAY (_int4))", false)]
    [InlineData("DOMAIN (public.score_values -> ARRAY (numeric(10,2)))", false)]
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
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Scores"] = "DOMAIN (public.score_values -> ARRAY (numeric(10,2)))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Scores"] = "integer[]" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Scores"] = "int4[]" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Scores"] = "ARRAY (numeric(10,2))" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Scores"] = "numeric(10,2)[]" } })!);
        Assert.False((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Ids"] = "uuid[]" } })!);
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
        Assert.True((bool)m.Invoke(null, new object?[] { new Dictionary<string, string> { ["Addresses"] = "inet[]" } })!);
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

    [Theory]
    [InlineData(@"enum('draft','it\'s','comma,value')", "Status", "Enum", "Status IN ('draft', 'it''s', 'comma,value')")]
    [InlineData(@"set('read\'write','admin')", "Flags", "Set", "Flags IN ('', 'read''write', 'admin', 'read''write,admin')")]
    public void TryBuildProviderValueCheckSql_EscapesMySqlQuotedValues(string detail, string column, string expectedKind, string expectedSql)
    {
        var m = GetMethod("TryBuildProviderValueCheckSql", new[] { typeof(string), typeof(string), typeof(string).MakeByRefType(), typeof(string).MakeByRefType() });
        object?[] args = { column, detail, null, null };

        Assert.True((bool)m.Invoke(null, args)!);
        Assert.Equal(expectedKind, args[2]);
        Assert.Equal(expectedSql, args[3]);
    }

    [Fact]
    public void TryBuildProviderValueCheckSql_RejectsMySqlSetValuesWithEscapedCommas()
    {
        var m = GetMethod("TryBuildProviderValueCheckSql", new[] { typeof(string), typeof(string), typeof(string).MakeByRefType(), typeof(string).MakeByRefType() });
        object?[] args = { "Flags", @"set('read\,write','admin')", null, null };

        Assert.False((bool)m.Invoke(null, args)!);
        Assert.Equal(string.Empty, args[2]);
        Assert.Equal(string.Empty, args[3]);
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
    [InlineData("ARRAY (varchar(64))", "character varying[]")]
    [InlineData("ARRAY (numeric(10,2))", "numeric[]")]
    [InlineData("integer[]", "integer[]")]
    [InlineData("int4[]", "integer[]")]
    [InlineData("varchar[]", "character varying[]")]
    [InlineData("bpchar[]", "character[]")]
    [InlineData("character varying(64)[]", "character varying[]")]
    [InlineData("numeric(10,2)[]", "numeric[]")]
    [InlineData("uuid[]", "uuid[]")]
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

    private static DatabaseProvider CreateStoreTypeProbeProvider(string providerName)
        => providerName switch
        {
            "sqlserver" => new SqlServerProvider(),
            "postgres" => new PostgresProvider(new SqliteParameterFactory()),
            "mysql" => new MySqlProvider(new SqliteParameterFactory()),
            "sqlite" => new SqliteProvider(),
            _ => throw new ArgumentOutOfRangeException(nameof(providerName), providerName, null)
        };

    private static DbConnection CreateDynamicStoreTypeProbeConnection(string providerName)
        => providerName switch
        {
            "sqlserver" => new DynamicSqlConnectionSchemaProbeConnection(),
            "postgres" => new DynamicNpgsqlSchemaProbeConnection(),
            "mysql" => new DynamicMySqlMetadataProbeConnection(),
            "sqlite" => new SqliteConnection("Data Source=:memory:"),
            _ => throw new ArgumentOutOfRangeException(nameof(providerName), providerName, null)
        };

}
