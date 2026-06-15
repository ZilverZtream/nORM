#nullable enable

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using nORM.Core;
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
    public async Task StaticMySqlPrimaryKeyConstraintNameProbe_PreservesPrimaryConstraintName()
    {
        var connection = new DynamicMySqlMetadataProbeConnection(new[]
        {
            new Dictionary<string, object?>
            {
                ["TableSchema"] = DBNull.Value,
                ["TableName"] = "Orders",
                ["ConstraintName"] = "PRIMARY"
            },
            new Dictionary<string, object?>
            {
                ["TableSchema"] = DBNull.Value,
                ["TableName"] = "Ignored",
                ["ConstraintName"] = "PRIMARY"
            }
        });
        var tableInfos = new[] { new ScaffoldTableInfo("Orders", null) };

        var names = await ScaffoldKeyDiscovery.GetPrimaryKeyConstraintNamesAsync(
            connection,
            new MySqlProvider(new SqliteParameterFactory()),
            tableInfos);

        var name = Assert.Single(names);
        Assert.Equal("Orders", name.Key);
        Assert.Equal("PRIMARY", name.Value);
        Assert.Contains("information_schema.table_constraints", connection.LastCommandText, StringComparison.Ordinal);
        Assert.Contains("constraint_type = 'PRIMARY KEY'", connection.LastCommandText, StringComparison.Ordinal);
    }

    [Fact]
    public void DynamicMySqlStringBinaryFacetProbe_UsesSchemaQualifiedCatalogWhenProvided()
    {
        var (sql, parameters) = InvokeDynamicMySqlMetadataProbe("GetStringBinaryFacets");

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
}
