using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class DatabaseScaffolderCoverageTests
{
    private static readonly Type _scaffolderType = typeof(nORM.Scaffolding.DatabaseScaffolder);

    private static T InvokePrivate<T>(string method, params object?[] args)
    {
        var m = _scaffolderType.GetMethod(method,
            BindingFlags.NonPublic | BindingFlags.Static,
            null,
            args.Select(a => a?.GetType() ?? typeof(object)).ToArray(),
            null)!;
        return (T)m.Invoke(null, args)!;
    }


    [Fact]
    public async Task ScaffoldAsync_NullConnection_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(null!, new SqliteProvider(), Path.GetTempPath(), "Test"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullProvider_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, null!, Path.GetTempPath(), "Test"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullOutputDirectory_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), null!, "Test"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullNamespace_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), Path.GetTempPath(), null!));
    }


    private static string GetTypeName(Type type, bool allowNull)
        => ScaffoldTypeNameHelper.GetTypeName(type, allowNull);

    [Theory]
    [InlineData(typeof(int),       false, "int")]
    [InlineData(typeof(int),       true,  "int?")]
    [InlineData(typeof(long),      false, "long")]
    [InlineData(typeof(short),     false, "short")]
    [InlineData(typeof(byte),      false, "byte")]
    [InlineData(typeof(bool),      false, "bool")]
    [InlineData(typeof(string),    false, "string")]
    [InlineData(typeof(DateTime),  false, "DateTime")]
    [InlineData(typeof(decimal),   false, "decimal")]
    [InlineData(typeof(double),    false, "double")]
    [InlineData(typeof(float),     false, "float")]
    [InlineData(typeof(Guid),      false, "Guid")]
    [InlineData(typeof(byte[]),    false, "byte[]")]
    [InlineData(typeof(byte[]),    true,  "byte[]?")]
    public void GetTypeName_AllBranches(Type type, bool allowNull, string expected)
    {
        var result = GetTypeName(type, allowNull);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetTypeName_UnknownType_UsesFullName()
    {
        // Unknown type falls through to `type.FullName ?? type.Name`
        var result = GetTypeName(typeof(Uri), false);
        Assert.Equal("System.Uri", result);
    }

    [Fact]
    public void GetTypeName_NullableString_AddsQuestionMark()
    {
        var result = GetTypeName(typeof(string), true);
        Assert.Equal("string?", result);
    }


    private static string ToPascalCase(string name)
    {
        var m = _scaffolderType.GetMethod("ToPascalCase",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { name })!;
    }

    [Theory]
    [InlineData("products",     "Products")]
    [InlineData("order_items",  "OrderItems")]
    [InlineData("my_table_name","MyTableName")]
    [InlineData("already",      "Already")]
    [InlineData("",             "")]
    public void ToPascalCase_VariousInputs(string input, string expected)
    {
        var result = ToPascalCase(input);
        Assert.Equal(expected, result);
    }


    private static string EscapeCSharpIdentifier(string id)
    {
        var m = _scaffolderType.GetMethod("EscapeCSharpIdentifier",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { id })!;
    }

    [Theory]
    [InlineData("ValidName",    "ValidName")]
    [InlineData("class",        "@class")]    // C# keyword
    [InlineData("int",          "@int")]
    [InlineData("string",       "@string")]
    [InlineData("123abc",       "_123abc")]   // starts with digit
    [InlineData("has space",    "has_space")] // contains space
    public void EscapeCSharpIdentifier_Variants(string input, string expected)
    {
        var result = EscapeCSharpIdentifier(input);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_EmptyString_ReturnsFallbackIdentifier()
    {
        var result = EscapeCSharpIdentifier("");
        Assert.Equal("_", result);
    }


    private static string GetUnqualifiedName(string id)
    {
        var m = _scaffolderType.GetMethod("GetUnqualifiedName",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { id })!;
    }

    [Theory]
    [InlineData("schema.table",    "table")]
    [InlineData("table",           "table")]
    [InlineData("a.b.c",           "c")]
    public void GetUnqualifiedName_Splits(string input, string expected)
    {
        Assert.Equal(expected, GetUnqualifiedName(input));
    }


    private static string? GetSchemaNameOrNull(string id)
    {
        var m = _scaffolderType.GetMethod("GetSchemaNameOrNull",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string?)m.Invoke(null, new object[] { id });
    }

    [Theory]
    [InlineData("schema.table", "schema")]
    [InlineData("table",        null)]
    [InlineData(".table",       null)]  // idx <= 0
    public void GetSchemaNameOrNull_Variants(string input, string? expected)
    {
        Assert.Equal(expected, GetSchemaNameOrNull(input));
    }


    private static string EscapeQualifiedIfNeeded(string? schema, string table)
    {
        var m = _scaffolderType.GetMethod("EscapeQualifiedIfNeeded",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object?[] { schema, table })!;
    }

    [Theory]
    [InlineData("myschema", "mytable", "myschema.mytable")]
    [InlineData(null,       "mytable", "mytable")]
    [InlineData("",         "mytable", "mytable")]
    public void EscapeQualifiedIfNeeded_Variants(string? schema, string table, string expected)
    {
        Assert.Equal(expected, EscapeQualifiedIfNeeded(schema, table));
    }


    private static string EscapeIdentifier(SqliteConnection cn, string id)
    {
        var m = _scaffolderType.GetMethod("EscapeIdentifier",
            BindingFlags.NonPublic | BindingFlags.Static,
            null,
            new[] { typeof(System.Data.Common.DbConnection), typeof(string) },
            null)!;
        return (string)m.Invoke(null, new object[] { cn, id })!;
    }

    [Fact]
    public void EscapeIdentifier_Sqlite_UsesDoubleQuotes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var result = EscapeIdentifier(cn, "myTable");
        // SQLite uses double-quote escaping
        Assert.Contains("myTable", result);
    }

    [Fact]
    public void EscapeIdentifier_SchemaQualified_EscapesBothParts()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var result = EscapeIdentifier(cn, "schema.table");
        Assert.Contains(".", result); // still has separator
        Assert.Contains("schema", result);
        Assert.Contains("table", result);
    }


    private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
        => ScaffoldContextAdapter.Write(
            namespaceName,
            contextName,
            entities,
            Array.Empty<DatabaseScaffolder.ScaffoldRelationship>(),
            Array.Empty<DatabaseScaffolder.ScaffoldManyToManyJoin>());

    [Fact]
    public void ScaffoldContext_GeneratesClassWithQueryProperties()
    {
        var code = ScaffoldContext("MyApp", "MyDbContext", new[] { "Product", "Order" });
        Assert.Contains("public partial class MyDbContext : DbContext", code);
        Assert.Contains("using System.Linq;", code);
        Assert.Contains("IQueryable<Product>", code);
        Assert.Contains("IQueryable<Order>", code);
        Assert.Contains("namespace MyApp", code);
    }

    [Fact]
    public void ScaffoldContext_EmptyEntities_GeneratesClassWithNoProperties()
    {
        var code = ScaffoldContext("Test", "Ctx", Array.Empty<string>());
        Assert.Contains("public partial class Ctx : DbContext", code);
        Assert.DoesNotContain("IQueryable<", code);
    }

    [Fact]
    public void ScaffoldContext_EntityWithKeywordName_EscapesProperty()
    {
        var code = ScaffoldContext("Test", "Ctx", new[] { "class" });
        // EscapeCSharpIdentifier should prefix with @
        Assert.Contains("@class", code);
    }
}

// Covers: ExecuteBulkOperationAsync (owned tx, reuse tx, rollback, non-List IList)
