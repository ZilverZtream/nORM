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
        var m = GetMethod("GetTypeName", new[] { typeof(Type), typeof(bool), typeof(bool) });
        return (string)m.Invoke(null, new object[] { type, allowNull, true })!;
    }

    private static (string Sql, bool Stored) InvokeNormalizeScaffoldComputedSql(string raw)
    {
        var m = GetMethod("NormalizeScaffoldComputedSql", new[] { typeof(string) });
        return ((string Sql, bool Stored))m.Invoke(null, new object[] { raw })!;
    }

    private static string InvokeNormalizeScaffoldCheckSql(string raw)
    {
        var m = GetMethod("NormalizeScaffoldCheckSql", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { raw })!;
    }

    private static (bool Normalized, string Sql) InvokeTryNormalizeScaffoldDefaultSql(string? raw)
    {
        var m = GetMethod("TryNormalizeScaffoldDefaultSql", new[] { typeof(string), typeof(string).MakeByRefType() });
        object?[] args = { raw, string.Empty };
        var normalized = (bool)m.Invoke(null, args)!;
        return (normalized, (string)args[1]!);
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
        var m = GetMethod("TryParseIdentityOptions", new[] { typeof(string), typeof(long).MakeByRefType(), typeof(long).MakeByRefType() });
        object?[] args = { detail, 0L, 0L };
        var parsed = (bool)m.Invoke(null, args)!;
        return (parsed, (long)args[1]!, (long)args[2]!);
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

    private sealed class DynamicMySqlMetadataProbeConnection : DbConnection
    {
        private readonly IReadOnlyList<string> _columnTypes;
        private ConnectionState _state = ConnectionState.Open;

        public DynamicMySqlMetadataProbeConnection(params string[] columnTypes)
            => _columnTypes = columnTypes;

        public string LastCommandText { get; private set; } = string.Empty;
        public Dictionary<string, object?> LastParameters { get; } = new(StringComparer.OrdinalIgnoreCase);

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get; set; } = "Server=localhost;Database=current_catalog;";
        public override string Database => "current_catalog";
        public override string DataSource => "metadata-probe";
        public override string ServerVersion => "8.0";
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) { }
        public override void Close() => _state = ConnectionState.Closed;
        public override void Open() => _state = ConnectionState.Open;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand()
            => new DynamicMySqlMetadataProbeCommand(this);

        internal void Capture(string commandText, DbParameterCollection parameters)
        {
            LastCommandText = commandText;
            LastParameters.Clear();
            foreach (DbParameter parameter in parameters)
                LastParameters[parameter.ParameterName] = parameter.Value;
        }

        internal DbDataReader ExecuteReader(string commandText, DbParameterCollection parameters)
        {
            Capture(commandText, parameters);
            return _columnTypes.Count == 0
                ? new EmptyMetadataReader()
                : new SingleColumnMetadataReader("ColumnType", _columnTypes);
        }
    }

    private sealed class DynamicMySqlMetadataProbeCommand : DbCommand
    {
        private readonly DynamicMySqlMetadataProbeConnection _connection;
        private readonly SqliteCommand _parameterSource = new();

        public DynamicMySqlMetadataProbeCommand(DynamicMySqlMetadataProbeConnection connection)
            => _connection = connection;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; } = CommandType.Text;
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set { }
        }

        protected override DbParameterCollection DbParameterCollection => _parameterSource.Parameters;
        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }

        protected override DbParameter CreateDbParameter()
            => new SqliteParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return _connection.ExecuteReader(CommandText, _parameterSource.Parameters);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _parameterSource.Dispose();
            base.Dispose(disposing);
        }
    }

    private abstract class DynamicSchemaProbeConnection : DbConnection
    {
        private readonly IReadOnlyList<string> _schemas;
        private ConnectionState _state = ConnectionState.Open;

        protected DynamicSchemaProbeConnection(params string[] schemas)
            => _schemas = schemas;

        public string LastCommandText { get; private set; } = string.Empty;
        public Dictionary<string, object?> LastParameters { get; } = new(StringComparer.OrdinalIgnoreCase);

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get; set; } = "Server=localhost;Database=current_catalog;";
        public override string Database => "current_catalog";
        public override string DataSource => "schema-probe";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) { }
        public override void Close() => _state = ConnectionState.Closed;
        public override void Open() => _state = ConnectionState.Open;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand()
            => new DynamicSchemaProbeCommand(this);

        internal DbDataReader Execute(string commandText, DbParameterCollection parameters)
        {
            LastCommandText = commandText;
            LastParameters.Clear();
            foreach (DbParameter parameter in parameters)
                LastParameters[parameter.ParameterName] = parameter.Value;

            return new SingleColumnMetadataReader("SchemaName", _schemas);
        }
    }

    private sealed class DynamicSqlConnectionSchemaProbeConnection : DynamicSchemaProbeConnection
    {
        public DynamicSqlConnectionSchemaProbeConnection(params string[] schemas)
            : base(schemas) { }
    }

    private sealed class DynamicNpgsqlSchemaProbeConnection : DynamicSchemaProbeConnection
    {
        public DynamicNpgsqlSchemaProbeConnection(params string[] schemas)
            : base(schemas) { }
    }

    private sealed class DynamicSchemaProbeCommand : DbCommand
    {
        private readonly DynamicSchemaProbeConnection _connection;
        private readonly SqliteCommand _parameterSource = new();

        public DynamicSchemaProbeCommand(DynamicSchemaProbeConnection connection)
            => _connection = connection;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; } = CommandType.Text;
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set { }
        }

        protected override DbParameterCollection DbParameterCollection => _parameterSource.Parameters;
        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }

        protected override DbParameter CreateDbParameter()
            => new SqliteParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => _connection.Execute(CommandText, _parameterSource.Parameters);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _parameterSource.Dispose();
            base.Dispose(disposing);
        }
    }

    private sealed class SingleColumnMetadataReader : DbDataReader
    {
        private readonly string _columnName;
        private readonly IReadOnlyList<string> _values;
        private int _index = -1;

        public SingleColumnMetadataReader(string columnName, IReadOnlyList<string> values)
        {
            _columnName = columnName;
            _values = values;
        }

        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => string.Equals(name, _columnName, StringComparison.OrdinalIgnoreCase)
            ? GetValue(0)
            : throw new IndexOutOfRangeException(name);
        public override int Depth => 0;
        public override int FieldCount => 1;
        public override bool HasRows => _values.Count > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;

        public override void Close() { }
        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
        public override string GetDataTypeName(int ordinal) => "string";
        public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override System.Collections.IEnumerator GetEnumerator() => _values.GetEnumerator();
        public override Type GetFieldType(int ordinal) => typeof(string);
        public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));
        public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);
        public override string GetName(int ordinal) => ordinal == 0 ? _columnName : throw new IndexOutOfRangeException(nameof(ordinal));
        public override int GetOrdinal(string name) => string.Equals(name, _columnName, StringComparison.OrdinalIgnoreCase) ? 0 : -1;
        public override string GetString(int ordinal) => Convert.ToString(GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
        public override object GetValue(int ordinal)
        {
            if (ordinal != 0)
                throw new IndexOutOfRangeException(nameof(ordinal));

            return _index >= 0 && _index < _values.Count ? _values[_index] : DBNull.Value;
        }
        public override int GetValues(object[] values)
        {
            if (values.Length > 0)
                values[0] = GetValue(0);
            return Math.Min(values.Length, 1);
        }
        public override bool IsDBNull(int ordinal) => GetValue(ordinal) is DBNull;
        public override bool NextResult() => false;
        public override bool Read()
        {
            if (_index + 1 >= _values.Count)
                return false;

            _index++;
            return true;
        }
    }

    private sealed class EmptyMetadataReader : DbDataReader
    {
        public override object this[int ordinal] => DBNull.Value;
        public override object this[string name] => DBNull.Value;
        public override int Depth => 0;
        public override int FieldCount => 0;
        public override bool HasRows => false;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;

        public override void Close() { }
        public override bool GetBoolean(int ordinal) => false;
        public override byte GetByte(int ordinal) => 0;
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => '\0';
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
        public override string GetDataTypeName(int ordinal) => "object";
        public override DateTime GetDateTime(int ordinal) => default;
        public override decimal GetDecimal(int ordinal) => 0m;
        public override double GetDouble(int ordinal) => 0d;
        public override System.Collections.IEnumerator GetEnumerator() => Enumerable.Empty<object>().GetEnumerator();
        public override Type GetFieldType(int ordinal) => typeof(object);
        public override float GetFloat(int ordinal) => 0f;
        public override Guid GetGuid(int ordinal) => Guid.Empty;
        public override short GetInt16(int ordinal) => 0;
        public override int GetInt32(int ordinal) => 0;
        public override long GetInt64(int ordinal) => 0;
        public override string GetName(int ordinal) => string.Empty;
        public override int GetOrdinal(string name) => -1;
        public override string GetString(int ordinal) => string.Empty;
        public override object GetValue(int ordinal) => DBNull.Value;
        public override int GetValues(object[] values) => 0;
        public override bool IsDBNull(int ordinal) => true;
        public override bool NextResult() => false;
        public override bool Read() => false;
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

    private static string InvokeScaffoldContext(string ns, string ctxName, IEnumerable<string> entities, bool pluralizeQueryProperties)
    {
        var m = GetMethod("ScaffoldContext", new[] { typeof(string), typeof(string), typeof(IEnumerable<string>), typeof(bool) });
        return (string)m.Invoke(null, new object[] { ns, ctxName, entities, pluralizeQueryProperties })!;
    }

    private static string InvokeScaffoldContextWithNamedRelationship()
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var relationship = Activator.CreateInstance(
            relationshipType,
            "Book",
            "Author",
            "Book",
            "Author",
            "AuthorId",
            "Id",
            "Author",
            "Books",
            false,
            false,
            "NO ACTION",
            "NO ACTION",
            "FK_Book_Author_Custom")!;
        var relationships = Array.CreateInstance(relationshipType, 1);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        relationships.SetValue(relationship, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "Author", "Book" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    private static string InvokeScaffoldContextWithPrimaryKeyConstraintName()
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var primaryKey = Activator.CreateInstance(
            primaryKeyType,
            "User",
            new[] { "Id" },
            "PK_User_Custom")!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 1);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        primaryKeys.SetValue(primaryKey, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    private static string InvokeScaffoldContextWithRoutineStub()
        => InvokeScaffoldContextWithRoutine("dbo", "GetRevenue", "SQL Server stored procedure; parameters=3; outputParameters=2; parameterModes=@tenantId:IN:int,@total:OUT:decimal(18,2),@message:INOUT:nvarchar(32); resultColumns=Id:int:0|Name:nvarchar(40):0");

    private static string InvokeScaffoldContextWithRoutineReturnStub()
        => InvokeScaffoldContextWithRoutine("dbo", "ApplyDiscount", "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@orderId:IN:int,return:RETURN:int");

    private static string InvokeScaffoldContextWithRoutine(string? schema, string name, string detail, bool useDatabaseNames = false)
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            schema,
            name,
            "Routine",
            detail,
            null)!;
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
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        routines.SetValue(routine, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, useDatabaseNames })!;
    }

    private static string InvokeScaffoldContextWithSequence(string? schema, string name, string detail, bool useDatabaseNames = false)
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var sequence = Activator.CreateInstance(
            skippedObjectType,
            schema,
            name,
            "Sequence",
            detail,
            null)!;
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
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        sequences.SetValue(sequence, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, useDatabaseNames })!;
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
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
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        identityOptions.SetValue(identity, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    // ── ToPascalCase ────────────────────────────────────────────────────────

    private static string InvokeScaffoldContextWithPrecision(int? scale = 6)
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var precision = Activator.CreateInstance(
            precisionType,
            new object?[]
            {
                "dbo.Invoices",
                "Invoice",
                "Amount",
                "Amount",
                28,
                scale
            })!;
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
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 1);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        precisions.SetValue(precision, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "Invoice" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    private static string InvokeScaffoldContextWithColumnFacets()
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
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var codeFacet = Activator.CreateInstance(
            columnFacetType,
            new object?[]
            {
                "dbo.Customers",
                "Customer",
                "Code",
                "Code",
                40,
                false,
                true
            })!;
        var tokenFacet = Activator.CreateInstance(
            columnFacetType,
            new object?[]
            {
                "dbo.Customers",
                "Customer",
                "Token",
                "Token",
                16,
                null,
                true
            })!;
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
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 2);
        columnFacets.SetValue(codeFacet, 0);
        columnFacets.SetValue(tokenFacet, 1);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "Customer" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

}
