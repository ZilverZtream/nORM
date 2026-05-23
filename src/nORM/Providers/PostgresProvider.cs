using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Database provider implementation optimized for PostgreSQL.
    /// Handles SQL dialect translation, bulk operations and temporal table support.
    /// </summary>
    public sealed partial class PostgresProvider : BulkOperationProvider
    {
        internal override bool SupportsFastPathPreparedCommandCache => true;
        internal override bool PrefersSyncFastPathExecution => true;

        internal override bool PrefersSyncQueryPlanExecution => true;

        /// <summary>
        /// Minimum PostgreSQL version required (9.5 introduced ON CONFLICT, UPSERT, and row-level security).
        /// </summary>
        private static readonly Version MinimumPostgresVersion = new(9, 5);

        /// <summary>
        /// PostgreSQL error code for "undefined_table" (error class 42P01).
        /// </summary>
        private const string PgErrorUndefinedTable = "42P01";

        /// <summary>Factory used to create Npgsql-compatible <see cref="DbParameter"/> instances.</summary>
        private readonly IDbParameterFactory _parameterFactory;

        /// <summary>Shared pool of <see cref="StringBuilder"/> instances to reduce allocations during SQL generation.</summary>
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresProvider"/> class.
        /// </summary>
        /// <remarks>
        /// Uses a reflection-based Npgsql parameter factory. Consumer applications must
        /// reference the <c>Npgsql</c> package when executing PostgreSQL commands.
        /// </remarks>
        public PostgresProvider()
            : this(new ReflectionNpgsqlParameterFactory())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresProvider"/> class.
        /// </summary>
        /// <param name="parameterFactory">Factory responsible for creating PostgreSQL parameters.</param>
        /// <remarks>
        /// <para>
        /// <b>Runtime dependency:</b> This provider uses PostgreSQL via reflection and requires
        /// <c>Npgsql</c> to be installed in the consuming application.
        /// Add it to your project:
        /// <code>
        /// dotnet add package Npgsql
        /// </code>
        /// A clear <see cref="InvalidOperationException"/> is thrown at runtime if Npgsql is not present.
        /// </para>
        /// </remarks>
        public PostgresProvider(IDbParameterFactory parameterFactory)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
        }

        private sealed class ReflectionNpgsqlParameterFactory : IDbParameterFactory
        {
            private readonly Type? _parameterType = Type.GetType("Npgsql.NpgsqlParameter, Npgsql");

            public DbParameter CreateParameter(string name, object? value)
            {
                if (_parameterType == null)
                {
                    throw new InvalidOperationException(
                        "Npgsql package is required for PostgreSQL support. Add PackageReference Include=\"Npgsql\" to the consuming project.");
                }

                return (DbParameter)Activator.CreateInstance(_parameterType, name, value ?? DBNull.Value)!;
            }
        }

        /// <summary>
        /// Maximum length in characters of a single SQL statement supported by PostgreSQL.
        /// </summary>
        public override int MaxSqlLength => int.MaxValue;

        /// <inheritdoc />
        /// <remarks>PostgreSQL requires <c>true</c>/<c>false</c> literals; <c>1</c>/<c>0</c> cause type errors in boolean comparisons.</remarks>
        public override string BooleanTrueLiteral => "true";

        /// <inheritdoc />
        /// <remarks>PostgreSQL uses <c>false</c> rather than the default <c>0</c>.</remarks>
        public override string BooleanFalseLiteral => "false";

        /// <summary>
        /// Maximum number of parameters permitted in a prepared statement.
        /// </summary>
        public override int MaxParameters => 32_767;

        /// <inheritdoc />
        public override ProviderCapabilities Capabilities => new(
            "PostgreSQL",
            MinimumPostgresVersion,
            MaxParameters,
            true,
            true,
            true,
            true,
            "Requires Npgsql and PostgreSQL 9.5 or newer.");

        /// <summary>PostgreSQL supports <c>IS NOT DISTINCT FROM</c> for index-friendly null-safe equality.</summary>
        public override string NullSafeEqual(string left, string right)
            => $"{left} IS NOT DISTINCT FROM {right}";

        /// <summary>PostgreSQL supports <c>IS DISTINCT FROM</c> for index-friendly null-safe inequality.</summary>
        public override string NullSafeNotEqual(string left, string right)
            => $"{left} IS DISTINCT FROM {right}";

        /// <inheritdoc />
        /// <param name="id">Identifier to escape (e.g., <c>"table"</c> or <c>"schema.table"</c>).</param>
        /// <returns>The escaped identifier with each segment wrapped in double quotes.</returns>
        /// <remarks>
        /// Doubles embedded double-quote characters to prevent SQL injection via identifiers.
        /// Handles schema-qualified identifiers (schema.table) by escaping each part separately.
        /// </remarks>
        public override string Escape(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return id;
            if (id.Contains('.'))
                return string.Join(".", id.Split('.').Select(part => $"\"{part.Replace("\"", "\"\"")}\""));
            return $"\"{id.Replace("\"", "\"\"")}\"";
        }

        /// <summary>
        /// Adds PostgreSQL-specific <c>LIMIT</c> and <c>OFFSET</c> clauses to the SQL builder.
        /// </summary>
        /// <param name="sb">Builder receiving the clauses.</param>
        /// <param name="limit">Maximum number of rows to return.</param>
        /// <param name="offset">Number of rows to skip.</param>
        /// <param name="limitParameterName">Name of the parameter supplying the limit.</param>
        /// <param name="offsetParameterName">Name of the parameter supplying the offset.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            if (limitParameterName != null)
                sb.Append(" LIMIT ").Append(limitParameterName);
            else if (limit.HasValue)
                sb.Append(" LIMIT ").Append(limit.Value);

            if (offsetParameterName != null)
                sb.Append(" OFFSET ").Append(offsetParameterName);
            else if (offset.HasValue)
                sb.Append(" OFFSET ").Append(offset.Value);
        }

        /// <summary>
        /// Generates SQL for retrieving the value of an identity column after an insert.
        /// Uses PostgreSQL's <c>RETURNING</c> clause for single-statement identity retrieval.
        /// </summary>
        /// <param name="m">The mapping for the table being inserted into.</param>
        /// <returns>A <c>RETURNING</c> clause if a DB-generated key exists; otherwise <see cref="string.Empty"/>.</returns>
        public override string GetIdentityRetrievalString(TableMapping m)
        {
            var keyCol = m.KeyColumns.FirstOrDefault(c => c.IsDbGenerated);
            return keyCol != null ? $" RETURNING {keyCol.EscCol}" : string.Empty;
        }

        /// <summary>PostgreSQL supports ON CONFLICT DO NOTHING for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2}) ON CONFLICT DO NOTHING";

        /// <summary>
        /// Creates a <see cref="DbParameter"/> instance for use with Npgsql commands.
        /// </summary>
        /// <param name="name">Parameter name including prefix.</param>
        /// <param name="value">Parameter value; <c>null</c> is passed through to the factory.</param>
        /// <returns>A configured <see cref="DbParameter"/> from the Npgsql factory.</returns>
        public override DbParameter CreateParameter(string name, object? value) =>
            _parameterFactory.CreateParameter(name, value);

        /// <summary>
        /// Attempts to translate a .NET method invocation into its PostgreSQL equivalent.
        /// </summary>
        /// <param name="name">Name of the .NET method being translated.</param>
        /// <param name="declaringType">Type that declares the method.</param>
        /// <param name="args">SQL fragments representing the method arguments.</param>
        /// <returns>The translated SQL expression or <c>null</c> if the method is not supported.</returns>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);

            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"LENGTH({args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"EXTRACT(YEAR FROM {args[0]})",
                    nameof(DateTime.Month) => $"EXTRACT(MONTH FROM {args[0]})",
                    nameof(DateTime.Day) => $"EXTRACT(DAY FROM {args[0]})",
                    nameof(DateTime.Hour) => $"EXTRACT(HOUR FROM {args[0]})",
                    nameof(DateTime.Minute) => $"EXTRACT(MINUTE FROM {args[0]})",
                    nameof(DateTime.Second) => $"EXTRACT(SECOND FROM {args[0]})",
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

        /// <summary>
        /// Produces a SQL fragment that accesses a JSON value using PostgreSQL's <c>jsonb_extract_path_text</c>.
        /// </summary>
        /// <param name="columnName">The JSON column being accessed.</param>
        /// <param name="jsonPath">The JSON path expression (dot-delimited with optional array indices).</param>
        /// <returns>SQL fragment that retrieves the JSON value as text.</returns>
        /// <remarks>
        /// Supports both dot-notation and array accessors.
        /// Supported patterns:
        /// - Simple dot-notation: "user.address.city"
        /// - Array accessors: "items[0]", "items[0].name", "data[1].users[2].id"
        /// - Mixed: "order.items[0].product.name"
        /// NOT supported: JSONPath filter expressions like "items[*]", "items[?(@.active)]"
        ///
        /// Uses a pooled StringBuilder to reduce allocations; sb.ToString() still allocates for the returned string.
        /// </remarks>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);

            var sb = _stringBuilderPool.Get();
            try
            {
                sb.Append("jsonb_extract_path_text(");
                sb.Append(columnName);
                sb.Append(", ");

                // Skip first segment (root '$') if present
                int startIndex = jsonPath.StartsWith("$.") ? 2 : (jsonPath.StartsWith("$") ? 1 : 0);
                if (startIndex > 0 && startIndex < jsonPath.Length && jsonPath[startIndex] == '.')
                    startIndex++;

                // Root-only path "$" has no path segments after stripping the $.
                // PostgreSQL's jsonb_extract_path_text requires at least one path argument.
                // For root access, cast the column to text directly instead of using the function.
                if (startIndex >= jsonPath.Length)
                {
                    sb.Clear();
                    sb.Append(columnName);
                    sb.Append(" #>> '{}'");
                    return sb.ToString();
                }

                bool isFirst = true;

                while (startIndex < jsonPath.Length)
                {
                    if (!isFirst)
                    {
                        sb.Append(", ");
                    }
                    isFirst = false;

                    // Find next delimiter: either '.' or '['
                    int dotIndex = jsonPath.IndexOf('.', startIndex);
                    int bracketIndex = jsonPath.IndexOf('[', startIndex);

                    int nextDelimiter;
                    if (dotIndex == -1 && bracketIndex == -1)
                    {
                        // No more delimiters, consume rest of string
                        nextDelimiter = jsonPath.Length;
                    }
                    else if (dotIndex == -1)
                    {
                        nextDelimiter = bracketIndex;
                    }
                    else if (bracketIndex == -1)
                    {
                        nextDelimiter = dotIndex;
                    }
                    else
                    {
                        nextDelimiter = Math.Min(dotIndex, bracketIndex);
                    }

                    // Extract property name before delimiter
                    if (nextDelimiter > startIndex)
                    {
                        sb.Append('\'');
                        sb.Append(jsonPath, startIndex, nextDelimiter - startIndex);
                        sb.Append('\'');

                        startIndex = nextDelimiter;
                    }

                    // Handle array index if present
                    if (startIndex < jsonPath.Length && jsonPath[startIndex] == '[')
                    {
                        int closeBracketIndex = jsonPath.IndexOf(']', startIndex);
                        if (closeBracketIndex == -1)
                        {
                            throw new ArgumentException($"Invalid JSON path: unclosed bracket at position {startIndex}", nameof(jsonPath));
                        }

                        // Extract array index (between '[' and ']')
                        int indexStart = startIndex + 1;
                        int indexLength = closeBracketIndex - indexStart;

                        if (indexLength > 0)
                        {
                            sb.Append(", '");
                            sb.Append(jsonPath, indexStart, indexLength);
                            sb.Append('\'');
                        }

                        startIndex = closeBracketIndex + 1;
                    }

                    // Skip dot delimiter
                    if (startIndex < jsonPath.Length && jsonPath[startIndex] == '.')
                    {
                        startIndex++;
                    }
                }

                sb.Append(')');
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Builds an optimized <c>ANY</c> expression for arrays to implement a <c>Contains</c> filter.
        /// </summary>
        /// <param name="cmd">Command to which parameters are added.</param>
        /// <param name="columnName">Name of the column being filtered.</param>
        /// <param name="values">Values to check for containment.</param>
        /// <returns>SQL fragment implementing the containment check.</returns>
        public override string BuildContainsClause(DbCommand cmd, string columnName, IReadOnlyList<object?> values)
        {
            ArgumentNullException.ThrowIfNull(cmd);
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(values);

            if (values.Count == 0)
                return "(1=0)";

            var hasNulls = false;
            List<object?>? nonNullValues = null;
            for (var i = 0; i < values.Count; i++)
            {
                if (values[i] == null)
                {
                    hasNulls = true;
                    nonNullValues ??= values.Take(i).ToList();
                }
                else
                {
                    nonNullValues?.Add(values[i]);
                }
            }

            if (hasNulls && (nonNullValues == null || nonNullValues.Count == 0))
                return $"{columnName} IS NULL";

            var arrayValues = nonNullValues ?? values;
            var pName = ParamPrefix + "p0";
            var p = cmd.CreateParameter();
            p.ParameterName = pName;
            // Create a typed array so Npgsql can infer NpgsqlDbType correctly.
            // An untyped object[] causes binding failures for Guid, int, enum, nullable types on live PostgreSQL.
            p.Value = CreateTypedArray(arrayValues);
            cmd.Parameters.Add(p);
            return hasNulls
                ? $"({columnName} = ANY({pName}) OR {columnName} IS NULL)"
                : $"{columnName} = ANY({pName})";
        }

        /// <summary>
        /// Builds the strongest common-type array from the supplied values for Npgsql type inference.
        /// </summary>
        /// <param name="values">Values to convert into a typed array.</param>
        /// <returns>A strongly-typed array when all non-null values share the same type; otherwise an <c>object[]</c>.</returns>
        private static Array CreateTypedArray(IReadOnlyList<object?> values)
        {
            Type? commonType = null;
            bool hasNull = false;
            foreach (var v in values)
            {
                if (v == null) { hasNull = true; continue; }
                var t = v.GetType();
                if (commonType == null) { commonType = t; }
                else if (commonType != t) { commonType = null; break; }
            }
            if (commonType == null)
                return values.ToArray(); // mixed or all-null -- object[] fallback

            // When the collection contains nulls AND the common type is a non-nullable
            // value type (e.g., int), use Nullable<T> as the array element type. Otherwise
            // Array.SetValue(null, i) throws InvalidCastException on value-type arrays.
            var elementType = (hasNull && commonType.IsValueType && Nullable.GetUnderlyingType(commonType) == null)
                ? typeof(Nullable<>).MakeGenericType(commonType)
                : commonType;

            var arr = Array.CreateInstance(elementType, values.Count);
            for (int i = 0; i < values.Count; i++)
                arr.SetValue(values[i], i);
            return arr;
        }

        /// <summary>Splits "schema.table" into parts; defaults to the specified schema when unqualified.</summary>
        /// <param name="tableName">Table name that may include a schema prefix separated by a dot.</param>
        /// <param name="defaultSchema">Schema name to use when <paramref name="tableName"/> is unqualified.</param>
        /// <returns>A tuple of (Schema, Table) with surrounding double-quote characters stripped.</returns>
        private static (string Schema, string Table) SplitSchemaTable(string tableName, string defaultSchema)
        {
            var dot = tableName.IndexOf('.');
            if (dot < 0)
                return (defaultSchema, tableName.Trim('"'));
            return (tableName[..dot].Trim('"'), tableName[(dot + 1)..].Trim('"'));
        }

        /// <summary>
        /// Introspects live column definitions via information_schema.columns.
        /// Reconstructs full type strings including numeric precision/scale and character length.
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                var (schema, bareTable) = SplitSchemaTable(tableName, "public");
                cmd.CommandText = @"
SELECT column_name, data_type, character_maximum_length,
       numeric_precision, numeric_scale, is_nullable
FROM information_schema.columns
WHERE table_name = @t AND table_schema = @s
ORDER BY ordinal_position";
                var p = cmd.CreateParameter(); p.ParameterName = "@t"; p.Value = bareTable; cmd.Parameters.Add(p);
                var ps = cmd.CreateParameter(); ps.ParameterName = "@s"; ps.Value = schema; cmd.Parameters.Add(ps);
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(0);
                    var dataType = rdr.GetString(1).ToLowerInvariant();
                    var charMax = rdr.IsDBNull(2) ? (int?)null : rdr.GetInt32(2);
                    var numPrec = rdr.IsDBNull(3) ? (int?)null : rdr.GetInt32(3);
                    var numScale = rdr.IsDBNull(4) ? (int?)null : rdr.GetInt32(4);
                    var isNullable = rdr.GetString(5).Equals("YES", StringComparison.OrdinalIgnoreCase);

                    var sqlType = dataType switch
                    {
                        "character varying" or "varchar" =>
                            charMax.HasValue ? $"varchar({charMax})" : "text",
                        "character" or "char" =>
                            charMax.HasValue ? $"char({charMax})" : "char",
                        "numeric" or "decimal" =>
                            (numPrec.HasValue && numScale.HasValue) ? $"numeric({numPrec},{numScale})" : "numeric",
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

        /// <summary>
        /// Generates the SQL definition for the temporal history table corresponding to the entity mapping.
        /// When liveColumns are supplied, column types are taken from the live DB schema.
        /// </summary>
        /// <param name="mapping">The entity mapping to create history storage for.</param>
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
                return $"{Escape(c.Name)} {GetPostgresType(c.Prop.PropertyType)}";
            }));

            return $@"
CREATE TABLE {historyTable} (
    ""__VersionId"" BIGSERIAL PRIMARY KEY,
    ""__ValidFrom"" TIMESTAMP NOT NULL,
    ""__ValidTo"" TIMESTAMP NOT NULL,
    ""__Operation"" CHAR(1) NOT NULL,
    {columns}
);";
        }

        /// <summary>
        /// Produces the trigger definitions required to track changes in the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the target table.</param>
        /// <returns>DDL statements that create the temporal triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.Name)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.Name)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.Name)} = OLD.{Escape(c.Name)}"));
            var keyConditionH2 = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h2.{Escape(c.Name)} = OLD.{Escape(c.Name)}"));
            var functionName = Escape(mapping.TableName + "_TemporalFunction");

            return $@"
CREATE OR REPLACE FUNCTION {functionName}() RETURNS TRIGGER AS $$
DECLARE v_now TIMESTAMP := (now() at time zone 'utc');
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'I', {newColumns});
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition}
          AND NOT EXISTS (
             SELECT 1 FROM {history} h2 WHERE h2.""__ValidFrom"" = v_now AND {keyConditionH2}
          );
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'U', {newColumns});
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition}
          AND NOT EXISTS (
             SELECT 1 FROM {history} h2 WHERE h2.""__ValidFrom"" = v_now AND {keyConditionH2}
          );
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, v_now, 'D', {oldColumns});
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalTrigger")} ON {table};
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalTrigger")}
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW EXECUTE FUNCTION {functionName}();";
        }

        /// <summary>
        /// Determines whether a <see cref="DbException"/> represents a "table does not exist" error.
        /// PostgreSQL uses SQLSTATE <c>42P01</c> ("undefined_table") for this condition.
        /// Falls back to the base message-based heuristic when SqlState is unavailable.
        /// </summary>
        /// <param name="ex">The exception to inspect.</param>
        /// <returns><c>true</c> when the error definitively indicates the table is missing; otherwise <c>false</c>.</returns>
        public override bool IsObjectNotFoundError(DbException ex)
        {
            if (string.Equals(ex.SqlState, PgErrorUndefinedTable, StringComparison.Ordinal))
                return true;

            return base.IsObjectNotFoundError(ex);
        }

        /// <summary>
        /// Ensures that the provided connection is compatible with the PostgreSQL provider
        /// by verifying its runtime type.
        /// </summary>
        /// <param name="connection">The connection instance to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not an Npgsql connection.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection.GetType().FullName != "Npgsql.NpgsqlConnection")
                throw new InvalidOperationException("An NpgsqlConnection is required for PostgresProvider.");
        }

        /// <summary>
        /// Determines whether the PostgreSQL provider can be used by attempting to load
        /// the Npgsql driver and connect to a local database.
        /// </summary>
        /// <returns><c>true</c> if PostgreSQL is reachable and meets version requirements; otherwise, <c>false</c>.</returns>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Host=localhost;Database=postgres;Username=postgres;Password=;Timeout=1";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SHOW server_version";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr.Split(' ')[0]);
                return version >= MinimumPostgresVersion;
            }
            catch (DbException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        /// <inheritdoc />
        protected override async Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SHOW server_version";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SHOW server_version";
            return cmd.ExecuteScalar() as string;
        }

        /// <summary>
        /// Creates a transaction savepoint using Npgsql's save or savepoint APIs if available.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The transaction on which to create the savepoint.</param>
        /// <param name="name">Identifier for the savepoint.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("CreateSavepoint", new[] { typeof(string) });
            if (saveMethod != null)
            {
                try
                {
                    saveMethod.Invoke(transaction, new object[] { name });
                    ct.ThrowIfCancellationRequested();
                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Save indicates the transaction type
                    // does not support savepoints — map to NormUnsupportedFeatureException for a stable API.
                    if (ex.InnerException is NotSupportedException)
                        throw new NormUnsupportedFeatureException(
                            $"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.", ex.InnerException);
                    if (ex.InnerException != null)
                        throw ex.InnerException;
                    throw;
                }
            }
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Rolls back a transaction to the specified savepoint.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">Transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Cancellation token.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken -- a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var rollbackMethod = transaction.GetType().GetMethod("Rollback", new[] { typeof(string) }) ??
                                 transaction.GetType().GetMethod("RollbackToSavepoint", new[] { typeof(string) });
            if (rollbackMethod != null)
            {
                try
                {
                    rollbackMethod.Invoke(transaction, new object[] { name });
                    ct.ThrowIfCancellationRequested();
                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // NotSupportedException from a base DbTransaction.Rollback indicates the transaction type
                    // does not support savepoints — map to NormUnsupportedFeatureException for a stable API.
                    if (ex.InnerException is NotSupportedException)
                        throw new NormUnsupportedFeatureException(
                            $"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.", ex.InnerException);
                    if (ex.InnerException != null)
                        throw ex.InnerException;
                    throw;
                }
            }
            throw new NormUnsupportedFeatureException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

    }
}
