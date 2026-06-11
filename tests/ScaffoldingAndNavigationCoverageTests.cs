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
    public async Task ScaffoldAsync_WithUnnamedSqliteUniqueConstraint_UsesStableIndexName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE UniqueConstraintWidget (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Code TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "UniqueConstraintIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "UniqueConstraintWidget.cs"));
            Assert.Contains("[Index(\"UX_UniqueConstraintWidget_Code\", IsUnique = true)]", entityCode);
            Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
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
            CREATE INDEX IX_IndexedProviderSpecific_LowerName_Active ON IndexedProviderSpecific(lower(Name)) WHERE Active = 1;
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
            Assert.Contains("mb.Entity<IndexedProviderSpecific>().HasExpressionIndex(\"IX_IndexedProviderSpecific_LowerName_Active\", \"lower(Name)\", filterSql: \"Active = 1\");", contextCode);
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
    public async Task ScaffoldAsync_WithDescendingPartialExpressionIndex_EmitsExpressionIndexMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedExpressionWarning (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedExpressionWarning_LowerName_Desc
                ON IndexedExpressionWarning(lower(Name) DESC)
                WHERE Active = 1;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ExpressionWarningIndexCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "ExpressionWarningIndexCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.Contains(
                "mb.Entity<IndexedExpressionWarning>().HasExpressionIndex(\"IX_IndexedExpressionWarning_LowerName_Desc\", \"lower(Name) DESC\", isUnique: true, filterSql: \"Active = 1\");",
                contextCode,
                StringComparison.Ordinal);
            Assert.False(File.Exists(warningJsonPath), "Supported descending expression indexes should be emitted instead of warned.");
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
            Assert.Contains("public partial class BadTable", entityCode);
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
    public async Task ScaffoldAsync_WithUseDatabaseNames_PreservesLegalDatabaseNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE customer (
                customer_id INTEGER PRIMARY KEY,
                display_name TEXT NOT NULL
            );
            CREATE TABLE order_line (
                order_id INTEGER PRIMARY KEY,
                billing_customer_id INTEGER NOT NULL REFERENCES customer(customer_id),
                shipping_customer_id INTEGER NULL REFERENCES customer(customer_id),
                SKU TEXT NOT NULL,
                "class" TEXT NULL,
                "has space" TEXT NULL
            );
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
                "DatabaseNamesCtx",
                new ScaffoldOptions { UseDatabaseNames = true });

            var entityPath = Path.Combine(dir, "order_line.cs");
            Assert.True(File.Exists(entityPath));
            var entityCode = File.ReadAllText(entityPath);
            var customerCode = File.ReadAllText(Path.Combine(dir, "customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "DatabaseNamesCtx.cs"));

            Assert.Contains("public partial class order_line", entityCode);
            Assert.Contains("public long order_id { get; set; }", entityCode);
            Assert.Contains("public long billing_customer_id { get; set; }", entityCode);
            Assert.Contains("public long? shipping_customer_id { get; set; }", entityCode);
            Assert.Contains("public string SKU { get; set; } = default!;", entityCode);
            Assert.Contains("public string? @class { get; set; }", entityCode);
            Assert.Contains("public string? has_space { get; set; }", entityCode);
            Assert.Contains("public customer BillingCustomer { get; set; } = default!;", entityCode);
            Assert.Contains("public customer? ShippingCustomer { get; set; }", entityCode);
            Assert.Contains("public List<order_line> OrderLinesByBillingCustomerId { get; set; } = new();", customerCode);
            Assert.Contains("public List<order_line> OrderLinesByShippingCustomerId { get; set; } = new();", customerCode);
            Assert.Contains("IQueryable<customer> customers", contextCode);
            Assert.Contains("IQueryable<order_line> order_lines", contextCode);
            Assert.Contains(".HasMany(p => p.OrderLinesByBillingCustomerId)", contextCode);
            Assert.Contains(".WithOne(d => d.BillingCustomer)", contextCode);
            Assert.Contains(".HasMany(p => p.OrderLinesByShippingCustomerId)", contextCode);
            Assert.Contains(".WithOne(d => d.ShippingCustomer)", contextCode);
            Assert.DoesNotContain("OrderLine", entityCode);
            Assert.DoesNotContain("OrderId", entityCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
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
            Assert.Contains("public partial class _1BadContext : DbContext", contextCode);
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
    public async Task ScaffoldAsync_WithSchemaFilter_GeneratesRequestedSchemasAndExplicitTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE MainKeep (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE MainSkip (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxa"."SchemaKeepOne" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxa"."SchemaKeepTwo" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."SchemaSkip" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
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
                "SchemaFilteredCtx",
                new ScaffoldOptions
                {
                    Schemas = new[] { "auxa" },
                    Tables = new[] { "MainKeep" }
                });

            Assert.True(File.Exists(Path.Combine(dir, "MainKeep.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SchemaKeepOne.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SchemaKeepTwo.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "MainSkip.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SchemaSkip.cs")));

            var schemaEntityCode = File.ReadAllText(Path.Combine(dir, "SchemaKeepOne.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaFilteredCtx.cs"));
            Assert.Contains("[Table(\"SchemaKeepOne\", Schema = \"auxa\")]", schemaEntityCode);
            Assert.Contains("IQueryable<MainKeep> MainKeeps", contextCode);
            Assert.Contains("IQueryable<SchemaKeepOne> SchemaKeepOnes", contextCode);
            Assert.Contains("IQueryable<SchemaKeepTwo> SchemaKeepTwos", contextCode);
            Assert.DoesNotContain("MainSkips", contextCode);
            Assert.DoesNotContain("SchemaSkips", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMissingSchemaFilter_ThrowsNormConfigurationException()
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
                    "MissingSchemaCtx",
                    new ScaffoldOptions { Schemas = new[] { "missing_schema" } }));

            Assert.Contains("schema filter did not match", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("missing_schema", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTableFilter_SuppressesRelationshipsToUnselectedTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE SkipPrincipal (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE KeepDependent (
                Id INTEGER PRIMARY KEY,
                PrincipalId INTEGER NOT NULL,
                CONSTRAINT FK_KeepDependent_SkipPrincipal
                    FOREIGN KEY (PrincipalId) REFERENCES SkipPrincipal(Id)
            );
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
                "FilteredRelationshipCtx",
                new ScaffoldOptions { Tables = new[] { "KeepDependent" } });

            Assert.True(File.Exists(Path.Combine(dir, "KeepDependent.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SkipPrincipal.cs")));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "KeepDependent.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredRelationshipCtx.cs"));
            Assert.DoesNotContain("[ForeignKey(", dependentCode);
            Assert.DoesNotContain("SkipPrincipal", dependentCode);
            Assert.DoesNotContain("SkipPrincipals", contextCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
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
    public async Task ScaffoldAsync_WithViewTableFilter_GeneratesQueryArtifact()
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
                "FilteredCtx",
                new ScaffoldOptions { Tables = new[] { "ExistingView" } });

            var viewCode = File.ReadAllText(Path.Combine(dir, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));

            Assert.Contains("[Table(\"ExistingView\")]", viewCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.DoesNotContain("View ExistingView", warnings);
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
            Assert.Contains("[ReadOnlyEntity]", viewCode);
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
    public async Task ScaffoldAsync_WithSchemaFilter_IncludesQueryArtifactsInSchema()
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
                "SchemaViewCtx",
                new ScaffoldOptions { Schemas = new[] { "main" } });

            var tableCode = File.ReadAllText(Path.Combine(dir, "Existing.cs"));
            var viewCode = File.ReadAllText(Path.Combine(dir, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaViewCtx.cs"));

            Assert.Contains("[Table(\"Existing\")]", tableCode);
            Assert.Contains("[Table(\"ExistingView\")]", viewCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("IQueryable<Existing> Existings", contextCode);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode);
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
                <ImplicitUsings>disable</ImplicitUsings>
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
