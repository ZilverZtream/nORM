#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    private sealed class DynamicMySqlMetadataProbeConnection : DbConnection
    {
        private readonly IReadOnlyList<string> _columnTypes;
        private readonly IReadOnlyList<IReadOnlyDictionary<string, object?>> _rows;
        private ConnectionState _state = ConnectionState.Open;

        public DynamicMySqlMetadataProbeConnection(params string[] columnTypes)
        {
            _columnTypes = columnTypes;
            _rows = Array.Empty<IReadOnlyDictionary<string, object?>>();
        }

        public DynamicMySqlMetadataProbeConnection(IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
        {
            _columnTypes = Array.Empty<string>();
            _rows = rows;
        }

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
            if (_rows.Count > 0)
                return CreateRowReader(_rows);

            return _columnTypes.Count == 0
                ? new EmptyMetadataReader()
                : new SingleColumnMetadataReader("ColumnType", _columnTypes);
        }

        private static DbDataReader CreateRowReader(IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
        {
            var table = new DataTable();
            var columns = rows
                .SelectMany(static row => row.Keys)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
            foreach (var column in columns)
                table.Columns.Add(column, typeof(object));

            foreach (var sourceRow in rows)
            {
                var row = table.NewRow();
                foreach (var column in columns)
                    row[column] = sourceRow.TryGetValue(column, out var value) && value is not null ? value : DBNull.Value;
                table.Rows.Add(row);
            }

            return table.CreateDataReader();
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
}
