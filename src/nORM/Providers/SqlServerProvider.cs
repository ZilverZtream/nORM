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
    public sealed partial class SqlServerProvider : BulkOperationProvider
    {
        /// <summary>Maximum number of cached DataTable schemas used for bulk-delete key tables.</summary>
        private const int KeyTableSchemaCacheSize = 100;

        /// <summary>
        /// Minimum SQL Server version required (13.0 = SQL Server 2016).
        /// SQL Server 2016 introduced JSON_VALUE, STRING_SPLIT, and other features used by this provider.
        /// </summary>
        private static readonly Version MinimumSqlServerVersion = new(13, 0);

        /// <summary>SQL Server error number for "Invalid object name" (table/view does not exist).</summary>
        private const int SqlErrorObjectNotFound = 208;

        private const int SqlBulkCopySmallBatchThreshold = 512;

        private static readonly ConcurrentLruCache<Type, DataTable> _keyTableSchemas = new(maxSize: KeyTableSchemaCacheSize);

        /// <summary>
        /// SQL Server uses TOP(n)/OFFSET-FETCH paging syntax rather than LIMIT.
        /// </summary>
        public override bool UsesFetchOffsetPaging => true;

        /// <summary>
        /// Microsoft.Data.SqlClient's async reader path is measurably slower for the small,
        /// already-buffered result shapes nORM's runtime query fast paths target. Prefer the
        /// synchronous reader path there while leaving the broader async pipeline unchanged.
        /// </summary>
        internal override bool PrefersSyncFastPathExecution => true;

        internal override bool PrefersSyncCompiledQueryExecution => true;

        internal override bool PrefersSyncQueryPlanExecution => true;

        private readonly IDbParameterFactory? _parameterFactory;
        private readonly bool _isDialectOnly;

        /// <summary>Default constructor using the built-in Microsoft.Data.SqlClient parameter factory.</summary>
        public SqlServerProvider()
        {
            _parameterFactory = null;
            _isDialectOnly = false;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerProvider"/> class with an injected
        /// parameter factory. Pass a non-SqlClient factory to run the provider in dialect-only mode -
        /// useful for cross-provider parity tests where SQL Server dialect is exercised against a
        /// foreign engine such as SQLite. Native connection-type and server-version validation is
        /// skipped in dialect-only mode.
        /// </summary>
        /// <param name="parameterFactory">Factory responsible for creating database parameters.</param>
        public SqlServerProvider(IDbParameterFactory parameterFactory)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
            _isDialectOnly = true;
        }

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

        /// <inheritdoc />
        public override ProviderCapabilities Capabilities => new(
            "SQL Server",
            MinimumSqlServerVersion,
            MaxParameters,
            true,
            true,
            true,
            true,
            "Requires Microsoft.Data.SqlClient and SQL Server 2016 or newer.");

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
        /// SQL Server uses <c>OUTPUT INSERTED.[col]</c> (placed between the column list and VALUES)
        /// rather than a postfix <c>SELECT SCOPE_IDENTITY()</c>, which only supports numeric identity
        /// columns and cannot propagate GUID, string, or other non-numeric generated keys.
        /// <see cref="GetIdentityRetrievalPrefix"/> returns the OUTPUT clause; this method returns empty.
        /// </summary>
        /// <param name="m">The table mapping for the insert.</param>
        /// <returns>An empty string — retrieval is handled by <see cref="GetIdentityRetrievalPrefix"/>.</returns>
        public override string GetIdentityRetrievalString(TableMapping m) => string.Empty;

        /// <summary>
        /// Returns an <c>OUTPUT INSERTED.[keyCol]</c> clause placed between the column list and VALUES.
        /// This works for any column type (int, bigint, uniqueidentifier, varchar, …) unlike SCOPE_IDENTITY()
        /// which is restricted to numeric auto-increment columns.
        /// </summary>
        /// <param name="m">The table mapping for the insert.</param>
        /// <returns>SQL clause e.g. <c> OUTPUT INSERTED.[Id]</c>, or empty when no db-generated key exists.</returns>
        public override string GetIdentityRetrievalPrefix(TableMapping m)
        {
            var keyCol = m.KeyColumns.FirstOrDefault(c => c.IsDbGenerated);
            return keyCol != null ? $" OUTPUT INSERTED.{keyCol.EscCol}" : string.Empty;
        }

        /// <summary>SQL Server uses NVARCHAR(MAX) for unbounded textual conversion.</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS NVARCHAR(MAX))";

        /// <summary>SQL Server uses FLOAT for double-precision and DECIMAL(38,10) for fixed-precision.</summary>
        public override string GetRealCastSql(string innerSql, bool asDecimal = false)
            => asDecimal
                ? $"CAST({innerSql} AS DECIMAL(38, 10))"
                : $"CAST({innerSql} AS FLOAT)";

        /// <summary>SQL Server uses IF NOT EXISTS for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"IF NOT EXISTS (SELECT 1 FROM {escTable} WHERE {escC1} = {p1} AND {escC2} = {p2}) INSERT INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2})";

        /// <summary>
        /// Creates a SQL Server specific <see cref="DbParameter"/> instance, or delegates to an
        /// injected <see cref="IDbParameterFactory"/> when the provider is in dialect-only mode.
        /// </summary>
        /// <param name="name">Parameter name including prefix.</param>
        /// <param name="value">Value to assign to the parameter; <c>null</c> becomes <see cref="DBNull.Value"/>.</param>
        /// <returns>A configured <see cref="DbParameter"/>.</returns>
        public override DbParameter CreateParameter(string name, object? value)
            => _parameterFactory is not null
                ? _parameterFactory.CreateParameter(name, value)
                : new SqlParameter(name, value ?? DBNull.Value);

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
        /// DATEDIFF(SECOND) overflows around 68 years; for the LINQ use-case (TotalHours,
        /// TotalDays) that's well within range. CAST the result to FLOAT so subsequent
        /// divisions (e.g. TotalHours = sec / 3600) stay fractional.
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"CAST(DATEDIFF(SECOND, {startSql}, {endSql}) AS FLOAT)";

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
                    nameof(string.Trim) when args.Length == 1 => $"LTRIM(RTRIM({args[0]}))",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // SQL Server SUBSTRING is 1-indexed; .NET Substring is 0-indexed, add 1.
                    // The 2-arg form needs an explicit large length because SUBSTRING requires
                    // a length parameter; LEN of the source is always >= what we need.
                    nameof(string.Substring) when args.Length == 2 => $"SUBSTRING({args[0]}, {args[1]} + 1, LEN({args[0]}))",
                    nameof(string.Substring) when args.Length == 3 => $"SUBSTRING({args[0]}, {args[1]} + 1, {args[2]})",
                    nameof(string.Replace) when args.Length == 3 => $"REPLACE({args[0]}, {args[1]}, {args[2]})",
                    // CHARINDEX is 1-based and returns 0 if not found; .NET IndexOf is 0-based
                    // and returns -1, so subtract 1 from CHARINDEX.
                    nameof(string.IndexOf) when args.Length == 2 => $"(CHARINDEX({args[1]}, {args[0]}) - 1)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"YEAR({args[0]})",
                    nameof(DateTime.Month) => $"MONTH({args[0]})",
                    nameof(DateTime.Day) => $"DAY({args[0]})",
                    nameof(DateTime.Hour) => $"DATEPART(hour, {args[0]})",
                    nameof(DateTime.Minute) => $"DATEPART(minute, {args[0]})",
                    nameof(DateTime.Second) => $"DATEPART(second, {args[0]})",
                    nameof(DateTime.DayOfYear) => $"DATEPART(dayofyear, {args[0]})",
                    nameof(DateTime.Date) => $"CAST({args[0]} AS DATE)",
                    nameof(DateTime.AddDays) when args.Length == 2 => $"DATEADD(day, {args[1]}, {args[0]})",
                    nameof(DateTime.AddMonths) when args.Length == 2 => $"DATEADD(month, {args[1]}, {args[0]})",
                    nameof(DateTime.AddYears) when args.Length == 2 => $"DATEADD(year, {args[1]}, {args[0]})",
                    nameof(DateTime.AddHours) when args.Length == 2 => $"DATEADD(hour, {args[1]}, {args[0]})",
                    nameof(DateTime.AddMinutes) when args.Length == 2 => $"DATEADD(minute, {args[1]}, {args[0]})",
                    nameof(DateTime.AddSeconds) when args.Length == 2 => $"DATEADD(second, {args[1]}, {args[0]})",
                    // T-SQL DATEPART(weekday) depends on @@DATEFIRST; subtract @@DATEFIRST so that
                    // Sunday=0..Saturday=6 always, matching System.DayOfWeek.
                    nameof(DateTime.DayOfWeek) => $"((DATEPART(weekday, {args[0]}) + @@DATEFIRST - 1) % 7)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateOnly))
            {
                return name switch
                {
                    nameof(DateOnly.Year) => $"YEAR({args[0]})",
                    nameof(DateOnly.Month) => $"MONTH({args[0]})",
                    nameof(DateOnly.Day) => $"DAY({args[0]})",
                    nameof(DateOnly.DayOfYear) => $"DATEPART(dayofyear, {args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(TimeOnly))
            {
                return name switch
                {
                    nameof(TimeOnly.Hour) => $"DATEPART(hour, {args[0]})",
                    nameof(TimeOnly.Minute) => $"DATEPART(minute, {args[0]})",
                    nameof(TimeOnly.Second) => $"DATEPART(second, {args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(NormFunctions))
            {
                return name switch
                {
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
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
                    nameof(Math.Sqrt) when args.Length == 1 => $"SQRT({args[0]})",
                    nameof(Math.Pow) when args.Length == 2 => $"POWER({args[0]}, {args[1]})",
                    nameof(Math.Exp) when args.Length == 1 => $"EXP({args[0]})",
                    nameof(Math.Log) when args.Length == 1 => $"LOG({args[0]})",
                    nameof(Math.Log) when args.Length == 2 => $"LOG({args[0]}, {args[1]})",
                    nameof(Math.Log10) when args.Length == 1 => $"LOG10({args[0]})",
                    nameof(Math.Sign) when args.Length == 1 => $"SIGN({args[0]})",
                    // T-SQL ROUND with truncate flag (1) drops fractional digits instead of rounding.
                    nameof(Math.Truncate) when args.Length == 1 => $"ROUND({args[0]}, 0, 1)",
                    // T-SQL has no scalar Min/Max, only the aggregate. Emit a CASE.
                    nameof(Math.Min) when args.Length == 2 => $"(CASE WHEN {args[0]} < {args[1]} THEN {args[0]} ELSE {args[1]} END)",
                    nameof(Math.Max) when args.Length == 2 => $"(CASE WHEN {args[0]} > {args[1]} THEN {args[0]} ELSE {args[1]} END)",
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
            ValidateJsonPath(jsonPath);
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
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalInsert")};
GO
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalInsert")} ON {table} AFTER INSERT AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'I', {insertedColumns} FROM inserted i;
END;
GO

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalUpdate")};
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

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalDelete")};
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
            if (_isDialectOnly) return; // foreign parameter factory => dialect-only mode, foreign connection is intentional
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
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20))";
            return cmd.ExecuteScalar() as string;
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

    }
}
