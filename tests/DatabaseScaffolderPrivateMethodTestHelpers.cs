#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using nORM.Scaffolding;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
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
        var m = GetMethod("GetTypeName", new[] { typeof(Type), typeof(bool), typeof(bool) });
        return (string)m.Invoke(null, new object[] { type, allowNull, true })!;
    }

    private static (string Sql, bool Stored) InvokeNormalizeScaffoldComputedSql(string raw)
        => ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(raw);

    private static string InvokeNormalizeScaffoldCheckSql(string raw)
        => ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(raw);

    private static (bool Normalized, string Sql) InvokeTryNormalizeScaffoldDefaultSql(string? raw)
    {
        var normalized = ScaffoldSqlMetadataParser.TryNormalizeScaffoldDefaultSql(raw, out var sql);
        return (normalized, sql);
    }

    private static (bool Normalized, string Sql) InvokeTryNormalizeDynamicDefaultSql(string? raw)
    {
        var m = typeof(DynamicEntityTypeGenerator)
            .GetMethod("TryNormalizeDynamicDefaultSql", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string), typeof(string).MakeByRefType() }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "TryNormalizeDynamicDefaultSql");
        object?[] args = { raw, string.Empty };
        var normalized = (bool)m.Invoke(null, args)!;
        return (normalized, (string)args[1]!);
    }

    private static (bool Parsed, long Seed, long Increment) InvokeTryParseIdentityOptions(string? detail)
    {
        var parsed = ScaffoldSqlMetadataParser.TryParseIdentityOptions(detail, out var seed, out var increment);
        return (parsed, seed, increment);
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

    private static IReadOnlyDictionary<string, (string Sql, bool Stored)> InvokeDynamicExtractSqliteGeneratedColumns(string createTableSql)
    {
        var m = typeof(DynamicEntityTypeGenerator)
            .GetMethod("ExtractSqliteGeneratedColumns", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "ExtractSqliteGeneratedColumns");
        var result = (System.Collections.IDictionary)m.Invoke(null, new object?[] { createTableSql })!;
        var columns = new Dictionary<string, (string Sql, bool Stored)>(StringComparer.OrdinalIgnoreCase);
        foreach (System.Collections.DictionaryEntry entry in result)
        {
            var value = entry.Value!;
            columns[(string)entry.Key] = (
                (string)value.GetType().GetProperty("Sql")!.GetValue(value)!,
                (bool)value.GetType().GetProperty("Stored")!.GetValue(value)!);
        }

        return columns;
    }

    private static Array CreateDynamicDecimalColumnInfoArray(int precision, int? scale)
    {
        var generatorType = typeof(DynamicEntityTypeGenerator);
        var columnInfoType = generatorType.GetNestedType("ColumnInfo", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(nameof(DynamicEntityTypeGenerator), "ColumnInfo");
        var decimalPrecisionType = generatorType.GetNestedType("ScaffoldDecimalPrecision", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(nameof(DynamicEntityTypeGenerator), "ScaffoldDecimalPrecision");
        var decimalPrecision = Activator.CreateInstance(decimalPrecisionType, new object?[] { precision, scale })!;
        var ctor = columnInfoType.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Single(constructor => constructor.GetParameters().Length == 15);
        var column = ctor.Invoke(new object?[]
        {
            "Amount",
            "Amount",
            typeof(decimal),
            false,
            false,
            0,
            0,
            false,
            false,
            null,
            false,
            null,
            null,
            false,
            decimalPrecision
        });
        var columns = Array.CreateInstance(columnInfoType, 1);
        columns.SetValue(column, 0);
        return columns;
    }

    private static Type InvokeDynamicBuildTypeWithDecimalPrecision(int precision, int? scale)
    {
        var columns = CreateDynamicDecimalColumnInfoArray(precision, scale);
        var m = typeof(DynamicEntityTypeGenerator)
            .GetMethod("BuildDynamicType", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "BuildDynamicType");
        return (Type)m.Invoke(null, new object?[] { null, "DynamicDecimalPrecision", columns, false })!;
    }

    private static string InvokeDynamicSchemaDescriptorWithDecimalPrecision(int precision, int? scale)
    {
        var columns = CreateDynamicDecimalColumnInfoArray(precision, scale);
        var m = typeof(DynamicEntityTypeGenerator)
            .GetMethod("BuildSchemaDescriptor", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "BuildSchemaDescriptor");
        return (string)m.Invoke(null, new object?[] { null, "DynamicDecimalPrecision", columns, false })!;
    }

    private static (bool Mapped, Type Type) InvokeTryMapMySqlUnsignedType(Type declaringType, string detail)
    {
        var m = declaringType
            .GetMethod("TryMapMySqlUnsignedType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string), typeof(Type).MakeByRefType() }, null)
            ?? throw new MissingMethodException(declaringType.Name, "TryMapMySqlUnsignedType");
        object?[] args = { detail, typeof(object) };
        var mapped = (bool)m.Invoke(null, args)!;
        return (mapped, (Type)args[1]!);
    }

    private static (bool Parsed, string[] Values) InvokeTryParseBoundedMySqlSetValues(Type declaringType, string detail)
    {
        var m = declaringType
            .GetMethod("TryParseBoundedMySqlSetValues", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string), typeof(string[]).MakeByRefType() }, null)
            ?? throw new MissingMethodException(declaringType.Name, "TryParseBoundedMySqlSetValues");
        object?[] args = { detail, Array.Empty<string>() };
        var parsed = (bool)m.Invoke(null, args)!;
        return (parsed, (string[])args[1]!);
    }

    private static (string Sql, IReadOnlyDictionary<string, object?> Parameters) InvokeDynamicMySqlMetadataProbe(string methodName)
    {
        var connection = new DynamicMySqlMetadataProbeConnection();
        var method = typeof(DynamicEntityTypeGenerator)
            .GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(DbConnection), typeof(string), typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), methodName);

        method.Invoke(null, new object?[] { connection, "tenant_catalog", "Orders" });

        return (
            connection.LastCommandText,
            connection.LastParameters.ToDictionary(pair => pair.Key, pair => pair.Value));
    }

    private static bool InvokeDynamicHasWriteBlockingProviderSpecificColumns(DbConnection connection)
    {
        var method = typeof(DynamicEntityTypeGenerator)
            .GetMethod("HasWriteBlockingProviderSpecificColumns", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(DbConnection), typeof(string), typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "HasWriteBlockingProviderSpecificColumns");

        return (bool)method.Invoke(null, new object?[] { connection, "tenant_catalog", "Orders" })!;
    }

    private static string? InvokeResolveUniqueUnqualifiedSchema(DbConnection connection, string tableName)
    {
        var method = typeof(DynamicEntityTypeGenerator)
            .GetMethod("ResolveUniqueUnqualifiedSchema", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(DbConnection), typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "ResolveUniqueUnqualifiedSchema");

        return (string?)method.Invoke(null, new object?[] { connection, tableName });
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

}
