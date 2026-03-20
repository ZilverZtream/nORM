using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Database provider implementation for Microsoft SQL Server.
    /// Responsible for dialect translation, bulk operations and temporal table support.
    /// </summary>
    public sealed class SqlServerProvider : BulkOperationProvider
    {
        /// <summary>Maximum number of cached DataTable schemas used for bulk-delete key tables.</summary>
        private const int KeyTableSchemaCacheSize = 100;

        /// <summary>
        /// Minimum SQL Server version required (13.0 = SQL Server 2016).
        /// SQL Server 2016 introduced JSON_VALUE, STRING_SPLIT, and other features used by this provider.
        /// </summary>
        private static readonly Version MinimumSqlServerVersion = new(13, 0);

        /// <summary>Number of entities sampled for dynamic batch sizing heuristics.</summary>
        private const int BatchSizingSampleCount = 100;

        /// <summary>SQL Server error number for "Invalid object name" (table/view does not exist).</summary>
        private const int SqlErrorObjectNotFound = 208;

        private static readonly ConcurrentLruCache<Type, DataTable> _keyTableSchemas = new(maxSize: KeyTableSchemaCacheSize);

        /// <summary>
        /// SQL Server uses TOP(n)/OFFSET-FETCH paging syntax rather than LIMIT.
        /// </summary>
        public override bool UsesFetchOffsetPaging => true;

        /// <summary>
        /// Maximum length of a single SQL statement supported by SQL Server.
        /// </summary>
        /// <remarks>
        /// SQL Server supports batch sizes up to 65,536 x 4,096 bytes of network packet data.
        /// The previous value of 8,000 was incorrectly derived from the max row size, not the
        /// max query text size. SQL Server's actual query text limit is governed by
        /// max_recursion and memory, not a fixed character count. Using 256 MB as a safe ceiling
        /// avoids rejecting valid wide or complex queries.
        /// </remarks>
        public override int MaxSqlLength => 268_435_456;

        /// <summary>
        /// Maximum number of parameters allowed in a single SQL Server command.
        /// </summary>
        public override int MaxParameters => 2_100;

        /// <summary>
        /// Escapes an identifier such as a table or column name using SQL Server brackets.
        /// Handles multi-part identifiers (schema.table) correctly by escaping each part.
        /// </summary>
        /// <param name="id">The identifier to escape (e.g., "table" or "schema.table").</param>
        /// <returns>The escaped identifier (e.g., "[table]" or "[schema].[table]").</returns>
        /// <remarks>
        /// Properly escapes multi-part identifiers.
        /// "schema.table" becomes "[schema].[table]", not "[schema.table]".
        /// </remarks>
        public override string Escape(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return id;

            // Split by dot and escape each part individually to support schema.table notation
            if (id.Contains('.'))
            {
                // ID-7: Double embedded ] characters to prevent SQL injection via identifiers
                return string.Join(".", id.Split('.').Select(part => $"[{part.Replace("]", "]]")}]"));
            }

            // ID-7: Double embedded ] characters to prevent SQL injection via identifiers
            return $"[{id.Replace("]", "]]")}]";
        }

        /// <summary>
        /// Adds SQL Server paging clauses to the SQL builder using <c>OFFSET</c> and <c>FETCH</c>.
        /// </summary>
        /// <param name="sb">The SQL builder to append to.</param>
        /// <param name="limit">Maximum number of rows to return.</param>
        /// <param name="offset">Number of rows to skip before starting to return rows.</param>
        /// <param name="limitParameterName">Parameter name for the limit value.</param>
        /// <param name="offsetParameterName">Parameter name for the offset value.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            bool hasLimit = limitParameterName != null || limit.HasValue;
            bool hasOffset = offsetParameterName != null || offset.HasValue;
            if (hasLimit || hasOffset)
            {
                // Use case-insensitive comparison so that lower-case or mixed-case
                // ORDER BY clauses (e.g. "order by name") are detected correctly and a
                // duplicate ORDER BY is not appended.
                if (!HasTopLevelOrderBy(sb.ToString())) sb.Append(" ORDER BY (SELECT NULL)");
                sb.Append(" OFFSET ");
                if (offsetParameterName != null) sb.Append(offsetParameterName);
                else if (offset.HasValue) sb.Append(offset.Value);
                else sb.Append("0");
                sb.Append(" ROWS");
                if (limitParameterName != null)
                    sb.Append(" FETCH NEXT ").Append(limitParameterName).Append(" ROWS ONLY");
                else if (limit.HasValue)
                    sb.Append(" FETCH NEXT ").Append(limit.Value).Append(" ROWS ONLY");
            }
        }

        /// <summary>
        /// Returns <c>true</c> if the SQL string contains an <c>ORDER BY</c> clause at the
        /// top-level scope (depth-0 parentheses). Uses a mini-lexer that skips:
        /// <list type="bullet">
        ///   <item>single-quoted string literals (<c>'...'</c>, <c>''</c> escape),</item>
        ///   <item>double-quoted identifiers (<c>"..."</c>),</item>
        ///   <item>bracket-quoted identifiers (<c>[...]</c>),</item>
        ///   <item>line comments (<c>-- comment</c>),</item>
        ///   <item>block comments (<c>/* comment */</c>).</item>
        /// </list>
        /// This prevents an <c>ORDER BY</c> appearing inside a comment or literal from being
        /// mistaken for a real ORDER BY clause (Q1 fix).
        /// </summary>
        private static bool HasTopLevelOrderBy(string sql)
        {
            int depth = 0;
            int i = 0;
            int len = sql.Length;
            while (i < len)
            {
                var ch = sql[i];

                // Skip line comments: -- skip to end of line
                if (ch == '-' && i + 1 < len && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < len && sql[i] != '\n' && sql[i] != '\r') i++;
                    continue;
                }

                // Skip block comments: /* skip to matching */
                if (ch == '/' && i + 1 < len && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < len && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    if (i + 1 < len) i += 2; // consume */
                    continue;
                }

                // Skip single-quoted string literals ('...', '' escape)
                if (ch == '\'')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '\'') { i++; if (i < len && sql[i] == '\'') i++; else break; }
                        else i++;
                    }
                    continue;
                }

                // Skip double-quoted identifiers ("...", "" escape)
                if (ch == '"')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '"') { i++; if (i < len && sql[i] == '"') i++; else break; }
                        else i++;
                    }
                    continue;
                }

                // Skip bracket-quoted identifiers ([...]) -- SQL Server specific
                // Q1 fix: handle escaped ]] inside brackets (]] represents a literal ] character).
                // Without this, [a]]ORDER BYb] would terminate at the first ] and expose
                // "ORDER BYb]" to the ORDER BY scanner, producing a false positive.
                if (ch == '[')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == ']')
                        {
                            i++;
                            // ]] is an escaped literal ] -- continue scanning inside the identifier
                            if (i < len && sql[i] == ']') { i++; continue; }
                            // Single ] closes the identifier
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                if (ch == '(') { depth++; i++; continue; }
                if (ch == ')') { depth--; i++; continue; }

                if (depth == 0 && i + 8 <= len &&
                    (sql[i] == 'O' || sql[i] == 'o') &&
                    string.Compare(sql, i, "ORDER BY", 0, 8, StringComparison.OrdinalIgnoreCase) == 0)
                    return true;

                i++;
            }
            return false;
        }

        /// <summary>
        /// Returns SQL for retrieving the last identity value generated in the current scope.
        /// </summary>
        /// <param name="m">The table mapping (unused for SQL Server; SCOPE_IDENTITY() is table-agnostic).</param>
        /// <returns>SQL fragment appended after the insert to obtain the identity value.</returns>
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT SCOPE_IDENTITY();";

        /// <summary>SQL Server uses IF NOT EXISTS for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"IF NOT EXISTS (SELECT 1 FROM {escTable} WHERE {escC1} = {p1} AND {escC2} = {p2}) INSERT INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2})";

        /// <summary>
        /// Creates a SQL Server specific <see cref="DbParameter"/> instance.
        /// </summary>
        /// <param name="name">Parameter name including prefix.</param>
        /// <param name="value">Value to assign to the parameter; <c>null</c> becomes <see cref="DBNull.Value"/>.</param>
        /// <returns>A configured <see cref="SqlParameter"/>.</returns>
        public override DbParameter CreateParameter(string name, object? value)
            => new SqlParameter(name, value ?? DBNull.Value);

        /// <summary>
        /// Escapes special characters in a pattern used with SQL Server's <c>LIKE</c> operator.
        /// </summary>
        /// <param name="value">The pattern to escape.</param>
        /// <returns>The escaped pattern.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="value"/> is null.</exception>
        public override string EscapeLikePattern(string value)
        {
            ArgumentNullException.ThrowIfNull(value);
            var escaped = base.EscapeLikePattern(value);
            var esc = LikeEscapeChar.ToString();
            return escaped
                .Replace("[", esc + "[")
                .Replace("]", esc + "]")
                .Replace("^", esc + "^");
        }

        /// <summary>
        /// Translates a subset of .NET methods into their SQL Server equivalents.
        /// </summary>
        /// <param name="name">Name of the method being translated.</param>
        /// <param name="declaringType">Type that defines the method.</param>
        /// <param name="args">SQL representations of the method arguments.</param>
        /// <returns>The SQL translation or <c>null</c> if unsupported.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="args"/> is null.</exception>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);

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
                    nameof(Math.Round) => $"ROUND({args[0]}, 0)",
                    _ => null
                };
            }

            return null;
        }

        /// <summary>
        /// Translates a JSON value access expression using SQL Server's <c>JSON_VALUE</c> function.
        /// </summary>
        /// <param name="columnName">The JSON column to access.</param>
        /// <param name="jsonPath">JSON path pointing to the desired element.</param>
        /// <returns>SQL fragment that retrieves the JSON value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="columnName"/> or <paramref name="jsonPath"/> is null.</exception>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            return $"JSON_VALUE({columnName}, '{jsonPath}')";
        }

        /// <summary>
        /// SQL Server does not support CREATE TABLE IF NOT EXISTS.
        /// Uses an OBJECT_ID check to create the tags table only when absent.
        /// Uses NVARCHAR (not TEXT) and DATETIME2 (not TEXT) for correct SQL Server types.
        /// </summary>
        public override string GetCreateTagsTableSql()
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $@"IF OBJECT_ID(N'__NormTemporalTags', N'U') IS NULL
CREATE TABLE {table} ({tagCol} NVARCHAR(450) NOT NULL, {tsCol} DATETIME2 NOT NULL, PRIMARY KEY ({tagCol}))";
        }

        /// <summary>
        /// SQL Server uses SELECT TOP 1, not LIMIT 1.
        /// </summary>
        public override string GetHistoryTableExistsProbeSql(string escapedHistoryTable)
            => $"SELECT TOP 1 1 FROM {escapedHistoryTable}";

        /// <summary>
        /// SQL Server error 208 = "Invalid object name" = object/table does not exist.
        /// This is a definitive schema error, not a permission or connectivity failure.
        /// </summary>
        public override bool IsObjectNotFoundError(DbException ex)
            => ex is SqlException sqlEx && sqlEx.Number == SqlErrorObjectNotFound;

        /// <summary>
        /// Introspects live column definitions via INFORMATION_SCHEMA.COLUMNS.
        /// Reconstructs full type strings including precision/scale/length.
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                var (schema, bareTable) = SplitSchemaTable(tableName, "dbo");
                cmd.CommandText = @"
SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
       c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = @t AND c.TABLE_SCHEMA = @s
ORDER BY c.ORDINAL_POSITION";
                var p = cmd.CreateParameter(); p.ParameterName = "@t"; p.Value = bareTable; cmd.Parameters.Add(p);
                var ps = cmd.CreateParameter(); ps.ParameterName = "@s"; ps.Value = schema; cmd.Parameters.Add(ps);
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(0);
                    var dataType = rdr.GetString(1).ToLowerInvariant();
                    var charMax = rdr.IsDBNull(2) ? (int?)null : rdr.GetInt32(2);
                    var numPrec = rdr.IsDBNull(3) ? (byte?)null : rdr.GetByte(3);
                    var numScale = rdr.IsDBNull(4) ? (int?)null : rdr.GetInt32(4);
                    var isNullable = rdr.GetString(5).Equals("YES", StringComparison.OrdinalIgnoreCase);

                    var sqlType = dataType switch
                    {
                        "nvarchar" or "varchar" or "char" or "nchar" or "varbinary" or "binary" when charMax == -1 =>
                            $"{dataType}(max)",
                        "nvarchar" or "varchar" or "char" or "nchar" or "varbinary" or "binary" when charMax.HasValue =>
                            $"{dataType}({charMax})",
                        "nvarchar" or "varchar" or "char" or "nchar" or "varbinary" or "binary" =>
                            dataType, // null charMax: legacy types (text/ntext/image) or unknown length
                        "decimal" or "numeric" =>
                            (numPrec.HasValue && numScale.HasValue) ? $"{dataType}({numPrec},{numScale})" : dataType,
                        _ => dataType
                    };
                    result.Add(new LiveColumnInfo(name, sqlType, isNullable));
                }
            }
            catch (DbException dbEx) when (IsObjectNotFoundError(dbEx))
            {
                // Table does not exist yet -- return empty list.
            }
            return result;
        }

        private static (string Schema, string Table) SplitSchemaTable(string tableName, string defaultSchema)
        {
            var dot = tableName.IndexOf('.');
            if (dot < 0)
                return (defaultSchema, tableName.Trim('[', ']'));
            return (tableName[..dot].Trim('[', ']'), tableName[(dot + 1)..].Trim('[', ']'));
        }

        /// <summary>
        /// Generates SQL to create a history table used for temporal tracking.
        /// When liveColumns are supplied, column types are taken from the live DB schema.
        /// </summary>
        /// <param name="mapping">The mapping describing the entity table.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use CLR defaults.</param>
        /// <returns>DDL statement that creates the history table.</returns>
        public override string GenerateCreateHistoryTableSql(
            TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            var historyTable = Escape(mapping.TableName + "_History");
            var liveMap = liveColumns?
                .ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase)
                ?? new Dictionary<string, LiveColumnInfo>(0);

            var columns = string.Join(",\n    ", mapping.Columns.Select(c =>
            {
                if (liveMap.TryGetValue(c.Name, out var live))
                    return $"{Escape(c.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}";
                var sqlType = GetSqlType(c.Prop.PropertyType);
                return $"{Escape(c.Name)} {sqlType}";
            }));

            return $@"CREATE TABLE {historyTable} (
    {Escape("__VersionId")} BIGINT IDENTITY(1,1) PRIMARY KEY,
    {Escape("__ValidFrom")} DATETIME2 NOT NULL,
    {Escape("__ValidTo")} DATETIME2 NOT NULL,
    {Escape("__Operation")} CHAR(1) NOT NULL,
    {columns}
);";
        }

        /// <summary>
        /// Generates SQL Server triggers that maintain the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the target table.</param>
        /// <returns>DDL statements creating the temporal triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var insertedColumns = string.Join(", ", mapping.Columns.Select(c => "i." + Escape(c.Name)));
            var deletedColumns = string.Join(", ", mapping.Columns.Select(c => "d." + Escape(c.Name)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h.{Escape(c.Name)} = d.{Escape(c.Name)}"));
            var keyConditionH2 = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h2.{Escape(c.Name)} = d.{Escape(c.Name)}"));

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

        /// <summary>
        /// Ensures that the provided <see cref="DbConnection"/> is a SQL Server
        /// connection compatible with this provider.
        /// </summary>
        /// <param name="connection">The connection instance to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not a <see cref="SqlConnection"/>.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqlConnection)
                throw new InvalidOperationException("A SqlConnection is required for SqlServerProvider.");
        }

        /// <summary>
        /// Checks whether the SQL Server provider can operate by connecting to a local instance
        /// and verifying the server version meets the minimum requirement (SQL Server 2016+).
        /// </summary>
        /// <returns><c>true</c> if SQL Server is reachable and meets the minimum version; otherwise, <c>false</c>.</returns>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString =
                "Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=True;Connect Timeout=1";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var parts = versionStr.Split('.');
                var version = new Version(int.Parse(parts[0]), int.Parse(parts[1]));
                return version >= MinimumSqlServerVersion;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Creates a transaction savepoint using SQL Server's <see cref="SqlTransaction.Save(string)"/> API.
        /// Checks the CancellationToken before and after executing so that pre-cancelled
        /// tokens correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The transaction on which to create the savepoint.</param>
        /// <param name="name">Name of the savepoint.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqlTransaction sqlTransaction)
            {
                sqlTransaction.Save(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Rolls back the transaction to the specified savepoint.
        /// Checks the CancellationToken before and after executing so that pre-cancelled
        /// tokens correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqlTransaction sqlTransaction)
            {
                sqlTransaction.Rollback(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqlTransaction.", nameof(transaction));
        }

        #region SQL Server Bulk Operations
        /// <summary>
        /// Performs a high-throughput bulk insert using the SQL Server <c>SqlBulkCopy</c> API when available,
        /// falling back to batched inserts otherwise.
        /// </summary>
        /// <typeparam name="T">Entity type being inserted.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> driving the operation.</param>
        /// <param name="m">Mapping metadata for the entity.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total rows inserted.</returns>
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();

            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var operationKey = $"SqlServer_BulkInsert_{m.Type.Name}";

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
                    {
                        var unescapedName = UnescapeSqlServerIdentifier(col.EscCol);
                        bulkCopy.ColumnMappings.Add(col.PropName, unescapedName);
                    }

                    using var reader = new EntityDataReader<T>(batch, insertableCols);
                    await bulkCopy.WriteToServerAsync(reader, token).ConfigureAwait(false);
                    return reader.RecordsProcessed;
                }, ct).ConfigureAwait(false);

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2: Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
            return totalInserted;
        }

        /// <summary>
        /// Updates multiple entities in batches using parameterized <c>UPDATE</c> statements.
        /// </summary>
        /// <typeparam name="T">Type of entity being updated.</typeparam>
        /// <param name="ctx">Database context providing connection and options.</param>
        /// <param name="m">Mapping information for the entity.</param>
        /// <param name="entities">Entities containing updated values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();
            if (nonKeyCols.Count == 0) return 0;
            var tempTableName = $"#BulkUpdate_{Guid.NewGuid():N}";
            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));

            var tempCreated = false;
            var updatedCount = 0;
            try
            {
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    cmd.CommandText = $"CREATE TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                tempCreated = true;

                await BulkInsertInternalAsync(ctx, m, entities, tempTableName, ct).ConfigureAwait(false);

                var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
                var joinConditions = m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}").ToList();
                // X3: Include timestamp in join to enforce OCC semantics
                if (m.TimestampColumn != null)
                    joinConditions.Add($"T1.{m.TimestampColumn.EscCol} = T2.{m.TimestampColumn.EscCol}");
                var joinClause = string.Join(" AND ", joinConditions);

                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    // X1: Add tenant predicate to prevent cross-tenant bulk updates
                    if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                    {
                        cmd.AddParam("@__tenant_bulk", ctx.Options.TenantProvider.GetCurrentTenantId());
                        cmd.CommandText = $"UPDATE T1 SET {setClause} FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause} WHERE T1.{m.TenantColumn.EscCol} = @__tenant_bulk";
                    }
                    else
                    {
                        cmd.CommandText = $"UPDATE T1 SET {setClause} FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                    }
                    updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                // X4: Always drop temp table to prevent resource leak
                if (tempCreated)
                {
                    try
                    {
                        await using var dropCmd = ctx.CreateCommand();
                        dropCmd.CommandText = $"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}";
                        await dropCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceWarning($"SqlServerProvider: Failed to drop temp table {tempTableName} during BulkUpdate cleanup: {ex.Message}");
                    }
                }
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2: Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
            return updatedCount;
        }

        private async Task<int> BulkInsertInternalAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, string destinationTableName, CancellationToken ct) where T : class
        {
            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var operationKey = $"SqlServer_BulkInsert_{destinationTableName}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);

            var totalInserted = 0;
            for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
            {
                var batch = entityList.GetRange(i, Math.Min(sizing.OptimalBatchSize, entityList.Count - i));
                using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection)
                {
                    DestinationTableName = destinationTableName,
                    BatchSize = batch.Count,
                    EnableStreaming = true,
                    BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds
                };

                foreach (var col in insertableCols)
                    bulkCopy.ColumnMappings.Add(col.PropName, UnescapeSqlServerIdentifier(col.EscCol));

                using var reader = new EntityDataReader<T>(batch, insertableCols);
                var batchSw = Stopwatch.StartNew();
                await bulkCopy.WriteToServerAsync(reader, ct).ConfigureAwait(false);
                batchSw.Stop();
                totalInserted += reader.RecordsProcessed;
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            return totalInserted;
        }

        /// <summary>
        /// Deletes entities in batches using parameterized <c>DELETE</c> statements based on primary keys.
        /// </summary>
        /// <typeparam name="T">Type of entity to delete.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping describing the table and key columns.</param>
        /// <param name="entities">Entities identifying rows to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"#BulkDelete_{Guid.NewGuid():N}";
            var keyColDefs = string.Join(", ", m.KeyColumns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));

            var tempCreated = false;
            int deletedCount = 0;
            try
            {
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    cmd.CommandText = $"CREATE TABLE {tempTableName} ({keyColDefs})";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                tempCreated = true;

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
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    // X1: Add tenant predicate to prevent cross-tenant bulk deletes
                    if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                    {
                        cmd.AddParam("@__tenant_bulk", ctx.Options.TenantProvider.GetCurrentTenantId());
                        cmd.CommandText = $"DELETE T1 FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause} WHERE T1.{m.TenantColumn.EscCol} = @__tenant_bulk";
                    }
                    else
                    {
                        cmd.CommandText = $"DELETE T1 FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                    }
                    deletedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                // X4: Always drop temp table to prevent resource leak
                if (tempCreated)
                {
                    try
                    {
                        await using var dropCmd = ctx.CreateCommand();
                        dropCmd.CommandText = $"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}";
                        await dropCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceWarning($"SqlServerProvider: Failed to drop temp table {tempTableName} during BulkDelete cleanup: {ex.Message}");
                    }
                }
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2: Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, deletedCount, sw.Elapsed);
            return deletedCount;
        }

        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t.IsEnum) t = Enum.GetUnderlyingType(t);
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(short)) return "SMALLINT";
            if (t == typeof(byte)) return "TINYINT";
            if (t == typeof(string)) return "NVARCHAR(MAX)";
            if (t == typeof(DateTime)) return "DATETIME2";
            if (t == typeof(DateOnly)) return "DATE";
            if (t == typeof(TimeOnly)) return "TIME";
            if (t == typeof(DateTimeOffset)) return "DATETIMEOFFSET";
            if (t == typeof(TimeSpan)) return "TIME";
            if (t == typeof(bool)) return "BIT";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(double)) return "FLOAT";
            if (t == typeof(float)) return "REAL";
            if (t == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (t == typeof(byte[])) return "VARBINARY(MAX)";
            if (t == typeof(char)) return "NCHAR(1)";
            if (t == typeof(sbyte)) return "SMALLINT";
            if (t == typeof(ushort)) return "INT";
            if (t == typeof(uint)) return "BIGINT";
            if (t == typeof(ulong)) return "DECIMAL(20,0)";
            return "NVARCHAR(MAX)";
        }

        /// <summary>
        /// Unescapes a SQL Server bracket-quoted identifier by removing outer brackets
        /// and reversing the <c>]]</c> to <c>]</c> escape applied by <see cref="Escape"/>.
        /// </summary>
        /// <param name="escapedIdentifier">The escaped identifier (e.g., <c>[ColumnName]</c> or <c>[My]]Column]</c>).</param>
        /// <returns>The unescaped identifier (e.g., <c>ColumnName</c> or <c>My]Column</c>).</returns>
        private static string UnescapeSqlServerIdentifier(string escapedIdentifier)
        {
            if (string.IsNullOrEmpty(escapedIdentifier))
                return escapedIdentifier;

            // Remove outer brackets only if present
            if (escapedIdentifier.StartsWith("[") && escapedIdentifier.EndsWith("]") && escapedIdentifier.Length >= 2)
            {
                var inner = escapedIdentifier.Substring(1, escapedIdentifier.Length - 2);
                // Reverse the ]] escape applied by Escape()
                return inner.Replace("]]", "]");
            }

            return escapedIdentifier;
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

            /// <summary>
            /// Advances the reader to the next record in the underlying entity sequence.
            /// </summary>
            /// <returns><c>true</c> if another record is available; otherwise, <c>false</c>.</returns>
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

            /// <summary>
            /// Gets the number of fields exposed by the data reader.
            /// </summary>
            public int FieldCount => _columns.Count;

            /// <summary>
            /// Gets the value of the specified field by ordinal position.
            /// </summary>
            public object this[int i] => GetValue(i);

            /// <summary>
            /// Gets the value of the specified field by column name.
            /// </summary>
            public object this[string name] => GetValue(GetOrdinal(name));

            /// <summary>
            /// Retrieves the value of the field at the given ordinal.
            /// </summary>
            /// <param name="i">Zero-based column ordinal.</param>
            /// <returns>The boxed value of the column or <see cref="DBNull"/>.</returns>
            public object GetValue(int i)
            {
                if (_current == null) throw new InvalidOperationException("No current record");
                return _columns[i].Getter(_current) ?? DBNull.Value;
            }

            /// <summary>
            /// Gets the name of the column at the specified ordinal.
            /// </summary>
            public string GetName(int i) => _columns[i].PropName;

            /// <summary>
            /// Gets the data type name of the column at the specified ordinal.
            /// </summary>
            public string GetDataTypeName(int i) => GetFieldType(i).Name;

            /// <summary>
            /// Gets the <see cref="Type"/> of the column at the specified ordinal.
            /// </summary>
            public Type GetFieldType(int i) => _columns[i].Prop.PropertyType;

            /// <summary>
            /// Returns the zero-based column ordinal given the column name.
            /// </summary>
            /// <exception cref="IndexOutOfRangeException">Thrown when the column name is not found.</exception>
            public int GetOrdinal(string name)
            {
                var index = _columns.FindIndex(c => c.PropName == name);
                if (index < 0)
                    throw new IndexOutOfRangeException($"Column '{name}' not found in the reader.");
                return index;
            }

            /// <summary>
            /// Determines whether the column at the specified ordinal is set to <see cref="DBNull"/>.
            /// </summary>
            public bool IsDBNull(int i) => GetValue(i) == DBNull.Value;

            /// <summary>
            /// Copies field values into the provided array.
            /// </summary>
            /// <param name="values">Destination array.</param>
            /// <returns>The number of values copied.</returns>
            public int GetValues(object[] values)
            {
                var count = Math.Min(values.Length, FieldCount);
                for (var i = 0; i < count; i++)
                    values[i] = GetValue(i);
                return count;
            }

            /// <summary>
            /// Advances to the next result set. Always returns <c>false</c> as only a single
            /// result set is supported.
            /// </summary>
            public bool NextResult() => false;

            /// <summary>
            /// Gets the depth of nesting for the current row. Always <c>0</c> for this reader.
            /// </summary>
            public int Depth => 0;

            /// <summary>
            /// Gets a value indicating whether the reader is closed.
            /// </summary>
            public bool IsClosed => _disposed;

            /// <summary>
            /// Gets the number of rows affected. Always <c>-1</c> for this reader.
            /// </summary>
            public int RecordsAffected => -1;

            /// <summary>
            /// Returns a <see cref="DataTable"/> describing the column metadata for this reader.
            /// Implemented to support SqlBulkCopy scenarios that rely on schema information.
            /// </summary>
            public DataTable? GetSchemaTable()
            {
                var schemaTable = new DataTable("SchemaTable");

                // Add standard schema columns used by SqlBulkCopy and other data readers
                schemaTable.Columns.Add("ColumnName", typeof(string));
                schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
                schemaTable.Columns.Add("ColumnSize", typeof(int));
                schemaTable.Columns.Add("DataType", typeof(Type));
                schemaTable.Columns.Add("AllowDBNull", typeof(bool));
                schemaTable.Columns.Add("IsKey", typeof(bool));
                schemaTable.Columns.Add("IsUnique", typeof(bool));
                schemaTable.Columns.Add("IsReadOnly", typeof(bool));

                // Populate schema rows from column metadata
                for (int i = 0; i < _columns.Count; i++)
                {
                    var column = _columns[i];
                    var row = schemaTable.NewRow();

                    row["ColumnName"] = column.PropName;
                    row["ColumnOrdinal"] = i;
                    row["ColumnSize"] = -1; // Unknown/variable size
                    row["DataType"] = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
                    row["AllowDBNull"] = Nullable.GetUnderlyingType(column.Prop.PropertyType) != null || !column.Prop.PropertyType.IsValueType;
                    row["IsKey"] = column.IsKey;
                    row["IsUnique"] = column.IsKey;
                    row["IsReadOnly"] = false;

                    schemaTable.Rows.Add(row);
                }

                return schemaTable;
            }

            /// <summary>
            /// Closes the reader and releases the underlying enumerator.
            /// </summary>
            public void Close() => Dispose();

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="bool"/>.
            /// </summary>
            public bool GetBoolean(int i) => (bool)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="byte"/>.
            /// </summary>
            public byte GetByte(int i) => (byte)GetValue(i);

            /// <summary>
            /// Reads a stream of bytes from the specified column.
            /// Supports bulk copying entities with byte[] properties.
            /// </summary>
            public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
            {
                if (_current == null)
                    throw new InvalidOperationException("No current record");

                var value = _columns[i].Getter(_current);
                if (value == null || value == DBNull.Value)
                    return 0;

                if (value is not byte[] bytes)
                    throw new InvalidCastException($"Column {i} is not a byte array");

                // If buffer is null, return the total length of the data
                if (buffer == null)
                    return bytes.Length;

                // Calculate how many bytes to copy
                var bytesToCopy = Math.Min(length, bytes.Length - (int)fieldOffset);
                if (bytesToCopy <= 0)
                    return 0;

                // Copy the data
                Array.Copy(bytes, (int)fieldOffset, buffer, bufferoffset, bytesToCopy);
                return bytesToCopy;
            }

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="char"/>.
            /// </summary>
            public char GetChar(int i) => (char)GetValue(i);

            /// <summary>
            /// Reads a stream of characters from the specified column.
            /// Supports bulk copying entities with char[] or string properties.
            /// </summary>
            public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
            {
                if (_current == null)
                    throw new InvalidOperationException("No current record");

                var value = _columns[i].Getter(_current);
                if (value == null || value == DBNull.Value)
                    return 0;

                // Use string.CopyTo to avoid allocating char[] for entire string.
                // ToCharArray() allocates even if SqlBulkCopy only needs a small chunk.
                if (value is string str)
                {
                    // If buffer is null, return the total length of the string
                    if (buffer == null)
                        return str.Length;

                    // Calculate how many characters to copy from the string
                    var startIndex = (int)fieldoffset;
                    if (startIndex >= str.Length)
                        return 0;

                    var charsToCopy = Math.Min(length, str.Length - startIndex);
                    if (charsToCopy <= 0)
                        return 0;

                    // Copy directly from string to buffer without intermediate allocation
                    str.CopyTo(startIndex, buffer, bufferoffset, charsToCopy);
                    return charsToCopy;
                }
                else if (value is char[] charArray)
                {
                    // If buffer is null, return the total length of the char array
                    if (buffer == null)
                        return charArray.Length;

                    // Calculate how many characters to copy from the char array
                    var charsToCopy = Math.Min(length, charArray.Length - (int)fieldoffset);
                    if (charsToCopy <= 0)
                        return 0;

                    // Copy the data from char array
                    Array.Copy(charArray, (int)fieldoffset, buffer, bufferoffset, charsToCopy);
                    return charsToCopy;
                }
                else
                {
                    throw new InvalidCastException($"Column {i} is not a string or char array");
                }
            }

            /// <summary>
            /// Gets an <see cref="IDataReader"/> for the specified column. Not supported in this reader.
            /// </summary>
            public IDataReader GetData(int i) => throw new NotSupportedException();

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="DateTime"/>.
            /// </summary>
            public DateTime GetDateTime(int i) => (DateTime)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="decimal"/>.
            /// </summary>
            public decimal GetDecimal(int i) => (decimal)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="double"/>.
            /// </summary>
            public double GetDouble(int i) => (double)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="float"/>.
            /// </summary>
            public float GetFloat(int i) => (float)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="Guid"/>.
            /// </summary>
            public Guid GetGuid(int i) => (Guid)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="short"/>.
            /// </summary>
            public short GetInt16(int i) => (short)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="int"/>.
            /// </summary>
            public int GetInt32(int i) => (int)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="long"/>.
            /// </summary>
            public long GetInt64(int i) => (long)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="string"/>.
            /// </summary>
            public string GetString(int i) => (string)GetValue(i);

            /// <summary>
            /// Releases resources associated with the reader.
            /// </summary>
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
