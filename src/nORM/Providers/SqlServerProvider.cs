using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using System.Data.Common;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using System.Globalization;

#nullable enable

namespace nORM.Providers
{
    public sealed class SqlServerProvider : BulkOperationProvider
    {
        private static readonly ConcurrentLruCache<Type, DataTable> _keyTableSchemas = new(maxSize: 100);
        public override int MaxSqlLength => 8_000;
        public override int MaxParameters => 2_100;
        public override string Escape(string id) => $"[{id}]";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            if (limitParameterName != null || offsetParameterName != null)
            {
                if (!sb.ToString().Contains("ORDER BY")) sb.Append(" ORDER BY (SELECT NULL)");
                sb.Append(" OFFSET ");
                sb.Append(offsetParameterName ?? "0");
                sb.Append(" ROWS");
                if (limitParameterName != null)
                {
                    sb.Append(" FETCH NEXT ").Append(limitParameterName).Append(" ROWS ONLY");
                }
            }
        }
        
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT SCOPE_IDENTITY();";

        public override System.Data.Common.DbParameter CreateParameter(string name, object? value)
        {
            var param = new SqlParameter(name, value ?? DBNull.Value);
            return param;
        }

        public override string EscapeLikePattern(string value)
        {
            var escaped = base.EscapeLikePattern(value);
            var esc = LikeEscapeChar.ToString();
            return escaped
                .Replace("[", esc + "[")
                .Replace("]", esc + "]")
                .Replace("^", esc + "^");
        }

        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"LEN({args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"YEAR({args[0]})",
                    nameof(DateTime.Month) => $"MONTH({args[0]})",
                    nameof(DateTime.Day) => $"DAY({args[0]})",
                    nameof(DateTime.Hour) => $"DATEPART(hour, {args[0]})",
                    nameof(DateTime.Minute) => $"DATEPART(minute, {args[0]})",
                    nameof(DateTime.Second) => $"DATEPART(second, {args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEILING({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    _ => null
                };
            }

            return null;
        }

        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
            => $"JSON_VALUE({columnName}, '{jsonPath}')";

        public override string BuildContainsClause(DbCommand cmd, string columnName, IReadOnlyList<object?> values)
        {
            var pName = ParamPrefix + "p0";
            var joined = string.Join(",", values.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));
            cmd.AddParam(pName, joined);
            return $"{columnName} IN (SELECT value FROM STRING_SPLIT({pName}, ','))";
        }

        public override string GenerateCreateHistoryTableSql(TableMapping mapping)
        {
            var historyTable = Escape(mapping.TableName + "_History");
            var columns = string.Join(",\n    ", mapping.Columns.Select(c => $"{Escape(c.PropName)} {GetSqlType(c.Prop.PropertyType)}"));

            return $@"
CREATE TABLE {historyTable} (
    [__VersionId] BIGINT IDENTITY(1,1) PRIMARY KEY,
    [__ValidFrom] DATETIME2 NOT NULL,
    [__ValidTo] DATETIME2 NOT NULL,
    [__Operation] CHAR(1) NOT NULL,
    {columns}
);";
        }

        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.PropName)));
            var insertedColumns = string.Join(", ", mapping.Columns.Select(c => "i." + Escape(c.PropName)));
            var deletedColumns = string.Join(", ", mapping.Columns.Select(c => "d." + Escape(c.PropName)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h.{Escape(c.PropName)} = d.{Escape(c.PropName)}"));
            var keyConditionH2 = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h2.{Escape(c.PropName)} = d.{Escape(c.PropName)}"));

            return $@"
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalInsert")} ON {table} AFTER INSERT AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'I', {insertedColumns} FROM inserted i;
END;
GO

CREATE TRIGGER {Escape(mapping.TableName + "_TemporalUpdate")} ON {table} AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    UPDATE h SET h.__ValidTo = @Now
    FROM {history} h
    JOIN deleted d ON {keyCondition}
    WHERE h.__ValidTo = '9999-12-31'
      AND NOT EXISTS (
        SELECT 1 FROM {history} h2
        WHERE h2.__ValidFrom = @Now AND {keyConditionH2}
    );
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'U', {insertedColumns} FROM inserted i;
END;
GO

CREATE TRIGGER {Escape(mapping.TableName + "_TemporalDelete")} ON {table} AFTER DELETE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    UPDATE h SET h.__ValidTo = @Now
    FROM {history} h
    JOIN deleted d ON {keyCondition}
    WHERE h.__ValidTo = '9999-12-31'
      AND NOT EXISTS (
        SELECT 1 FROM {history} h2
        WHERE h2.__ValidFrom = @Now AND {keyConditionH2}
    );
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, @Now, 'D', {deletedColumns} FROM deleted d;
END;";
        }

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqlConnection)
                throw new InvalidOperationException("A SqlConnection is required for SqlServerProvider.");
        }

        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString =
                "Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=True;Connect Timeout=1";
            try
            {
                await cn.OpenAsync();
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
                var result = await cmd.ExecuteScalarAsync();
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var parts = versionStr.Split('.');
                var version = new Version(int.Parse(parts[0]), int.Parse(parts[1]));
                return version >= new Version(13, 0);
            }
            catch
            {
                return false;
            }
        }

        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction is SqlTransaction sqlTransaction)
            {
                sqlTransaction.Save(name);
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }

        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction is SqlTransaction sqlTransaction)
            {
                sqlTransaction.Rollback(name);
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }

        #region SQL Server Bulk Operations
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();

            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var operationKey = $"SqlServer_BulkInsert_{m.Type.Name}";
            // Logger does not expose informational log, so we skip logging batch strategy here.

            var totalInserted = await ExecuteBulkOperationAsync(ctx, m, entityList, operationKey,
                async (batch, tx, token) =>
                {
                    using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection, SqlBulkCopyOptions.Default, (SqlTransaction)tx)
                    {
                        DestinationTableName = m.EscTable,
                        BatchSize = batch.Count,
                        EnableStreaming = true,
                        BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds
                    };

                    foreach (var col in insertableCols)
                        bulkCopy.ColumnMappings.Add(col.PropName, col.EscCol.Trim('[', ']'));

                    using var reader = new EntityDataReader<T>(batch, insertableCols);
                    await bulkCopy.WriteToServerAsync(reader, token).ConfigureAwait(false);
                    return reader.RecordsProcessed;
                }, ct).ConfigureAwait(false);

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
            return totalInserted;
        }

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"#BulkUpdate_{Guid.NewGuid():N}";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();

            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TABLE {tempTableName} ({colDefs})";
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }

            await BulkInsertInternalAsync(ctx, m, entities, tempTableName, ct).ConfigureAwait(false);

            var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"UPDATE T1 SET {setClause} FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                var updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
                return updatedCount;
            }
        }

        private async Task<int> BulkInsertInternalAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, string destinationTableName, CancellationToken ct) where T : class
        {
            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var operationKey = $"SqlServer_BulkInsert_{destinationTableName}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);

            var totalInserted = 0;
            for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
            {
                var batch = entityList.Skip(i).Take(sizing.OptimalBatchSize).ToList();
                using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection)
                {
                    DestinationTableName = destinationTableName,
                    BatchSize = batch.Count,
                    EnableStreaming = true,
                    BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds
                };

                foreach (var col in insertableCols)
                    bulkCopy.ColumnMappings.Add(col.PropName, col.EscCol.Trim('[', ']'));

                using var reader = new EntityDataReader<T>(batch, insertableCols);
                var batchSw = Stopwatch.StartNew();
                await bulkCopy.WriteToServerAsync(reader, ct).ConfigureAwait(false);
                batchSw.Stop();
                totalInserted += reader.RecordsProcessed;
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            return totalInserted;
        }

        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"#BulkDelete_{Guid.NewGuid():N}";
            var keyColDefs = string.Join(", ", m.KeyColumns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));

            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TABLE {tempTableName} ({keyColDefs})";
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }

            using (var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection)
            {
                DestinationTableName = tempTableName,
                BatchSize = ctx.Options.BulkBatchSize,
                EnableStreaming = true
            })
            {
                using var table = GetKeyTable(m);
                var batchCount = 0;
                foreach (var entity in entities)
                {
                    table.Rows.Add(m.KeyColumns.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                    batchCount++;
                    if (batchCount >= ctx.Options.BulkBatchSize)
                    {
                        await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
                        table.Clear();
                        batchCount = 0;
                    }
                }
                if (batchCount > 0)
                    await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
            }

            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"DELETE T1 FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                var deletedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, deletedCount, sw.Elapsed);
                return deletedCount;
            }
        }

        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(string)) return "NVARCHAR(MAX)";
            if (t == typeof(DateTime)) return "DATETIME2";
            if (t == typeof(bool)) return "BIT";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (t == typeof(byte[])) return "VARBINARY(MAX)";
            return "NVARCHAR(MAX)";
        }

        private static DataTable GetKeyTable(TableMapping m)
        {
            var schema = _keyTableSchemas.GetOrAdd(m.Type, _ =>
            {
                var dt = new DataTable();
                foreach (var c in m.KeyColumns)
                {
                    var propType = c.Prop.PropertyType;
                    dt.Columns.Add(c.PropName, Nullable.GetUnderlyingType(propType) ?? propType);
                }
                return dt;
            });
            return schema.Clone();
        }

        private sealed class EntityDataReader<T> : IDataReader where T : class
        {
            private readonly IEnumerator<T> _enumerator;
            private readonly List<Column> _columns;
            private bool _disposed;
            private T? _current;

            public int RecordsProcessed { get; private set; }

            public EntityDataReader(IEnumerable<T> entities, List<Column> columns)
            {
                _enumerator = entities.GetEnumerator();
                _columns = columns;
            }

            public bool Read()
            {
                if (_enumerator.MoveNext())
                {
                    _current = _enumerator.Current;
                    RecordsProcessed++;
                    return true;
                }
                return false;
            }

            public int FieldCount => _columns.Count;
            public object this[int i] => GetValue(i);
            public object this[string name] => GetValue(GetOrdinal(name));

            public object GetValue(int i)
            {
                if (_current == null) throw new InvalidOperationException("No current record");
                return _columns[i].Getter(_current) ?? DBNull.Value;
            }

            public string GetName(int i) => _columns[i].PropName;
            public string GetDataTypeName(int i) => GetFieldType(i).Name;
            public Type GetFieldType(int i) => _columns[i].Prop.PropertyType;
            public int GetOrdinal(string name) => _columns.FindIndex(c => c.PropName == name);
            public bool IsDBNull(int i) => GetValue(i) == DBNull.Value;

            public int GetValues(object[] values)
            {
                var count = Math.Min(values.Length, FieldCount);
                for (var i = 0; i < count; i++)
                    values[i] = GetValue(i);
                return count;
            }

            public bool NextResult() => false;
            public int Depth => 0;
            public bool IsClosed => _disposed;
            public int RecordsAffected => -1;
            public DataTable? GetSchemaTable() => null;
            public void Close() => Dispose();

            public bool GetBoolean(int i) => (bool)GetValue(i);
            public byte GetByte(int i) => (byte)GetValue(i);
            public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
            public char GetChar(int i) => (char)GetValue(i);
            public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
            public IDataReader GetData(int i) => throw new NotSupportedException();
            public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
            public decimal GetDecimal(int i) => (decimal)GetValue(i);
            public double GetDouble(int i) => (double)GetValue(i);
            public float GetFloat(int i) => (float)GetValue(i);
            public Guid GetGuid(int i) => (Guid)GetValue(i);
            public short GetInt16(int i) => (short)GetValue(i);
            public int GetInt32(int i) => (int)GetValue(i);
            public long GetInt64(int i) => (long)GetValue(i);
            public string GetString(int i) => (string)GetValue(i);

            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;
                _enumerator.Dispose();
            }
        }
        #endregion
    }
}