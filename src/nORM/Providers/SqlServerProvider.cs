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

        /// <summary>
        /// SQL Server uses <c>FORMAT(x, 'FN', 'en-US')</c> for fixed-decimal text.
        /// The invariant 'en-US' culture forces a decimal point separator regardless
        /// of server locale.
        /// </summary>
        public override string FormatFixedDecimalSql(string sql, int digits)
            => $"FORMAT({sql}, 'F{digits}', 'en-US')";

        /// <summary>
        /// SQL Server's <c>FORMAT(x, fmt, 'en-US')</c> accepts .NET-style date
        /// patterns directly. The 'en-US' culture forces invariant separators
        /// regardless of server locale. Returns null if the format contains a
        /// single-quote that would need escaping in an unusual way -- the
        /// common yyyy/MM/dd/HH/mm/ss patterns escape cleanly.
        /// </summary>
        public override string? FormatDateUsingDotNetPattern(string sql, string dotNetFormat)
        {
            var escaped = dotNetFormat.Replace("'", "''");
            return $"FORMAT({sql}, '{escaped}', 'en-US')";
        }

        /// <summary>SQL Server uses <c>DATEADD(SECOND, N, col)</c> for date arithmetic.</summary>
        public override string? AddSecondsToDateTimeSql(string dateTimeSql, string secondsSqlFragment)
            => $"DATEADD(SECOND, {secondsSqlFragment}, {dateTimeSql})";

        /// <summary>SQL Server uses UNICODE() for the BMP code point.</summary>
        public override string GetCharCodeSql(string charSql) => $"UNICODE({charSql})";

        /// <summary>
        /// SQL Server TimeSpan columns map to TIME(7). Convert to seconds via
        /// DATEDIFF from midnight, then DATEADD to the DateTime.
        /// </summary>
        public override string? AddTimeSpanColumnToDateTimeSql(string dateTimeSql, string timeSpanColumnSql, bool subtract)
        {
            var sign = subtract ? "-" : "";
            return $"DATEADD(SECOND, {sign}DATEDIFF(SECOND, CAST('00:00:00' AS TIME), {timeSpanColumnSql}), {dateTimeSql})";
        }

        /// <summary>
        /// SQL Server stores TimeSpan as TIME(7). DATEDIFF_BIG with microsecond
        /// granularity returns the count from midnight, divided by 1e6 to get
        /// fractional seconds.
        /// </summary>
        public override string GetTimeSpanColumnSecondsSql(string timeSpanColumnSql)
            => $"(CAST(DATEDIFF_BIG(MICROSECOND, CAST('00:00:00' AS TIME), {timeSpanColumnSql}) AS FLOAT) / 1000000.0)";

        /// <summary>SQL Server uses DATEADD(DAY, N, col) for date arithmetic on DATE columns.</summary>
        public override string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment)
            => $"DATEADD(DAY, {daysSqlFragment}, {dateOnlySql})";

        /// <summary>SQL Server uses DATEADD(MONTH, N, col) for date arithmetic on DATE columns.</summary>
        public override string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment)
            => $"DATEADD(MONTH, {monthsSqlFragment}, {dateOnlySql})";

        /// <summary>SQL Server uses DATEADD(YEAR, N, col) for date arithmetic on DATE columns.</summary>
        public override string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment)
            => $"DATEADD(YEAR, {yearsSqlFragment}, {dateOnlySql})";

        /// <summary>
        /// SQL Server DATEADD on TIME returns DATETIME -- CAST back to TIME so
        /// the materializer reads a TimeOnly-compatible value.
        /// </summary>
        public override string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment)
            => $"CAST(DATEADD(SECOND, {secondsSqlFragment}, {timeOnlySql}) AS TIME)";

        /// <summary>
        /// SQL Server: convert TimeSpan column to seconds via DATEDIFF from
        /// midnight, then DATEADD on the TIME. CAST back to TIME because
        /// DATEADD promotes to DATETIME.
        /// </summary>
        public override string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract)
        {
            var sign = subtract ? "-" : "";
            return $"CAST(DATEADD(SECOND, {sign}DATEDIFF(SECOND, CAST('00:00:00' AS TIME), {timeSpanColumnSql}), {timeOnlySql}) AS TIME)";
        }

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

        /// <summary>T-SQL DATETIME2FROMPARTS with precision 7 matches .NET DateTime ticks.</summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"DATETIME2FROMPARTS({yearSql}, {monthSql}, {daySql}, 0, 0, 0, 0, 7)";

        /// <summary>6-arg DATETIME2FROMPARTS with non-zero h/m/s and 0 fractional.</summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql)
            => $"DATETIME2FROMPARTS({yearSql}, {monthSql}, {daySql}, {hourSql}, {minuteSql}, {secondSql}, 0, 7)";

        /// <summary>
        /// 7-arg DATETIME2FROMPARTS: fractions arg is in 100ns ticks at precision 7,
        /// so ms * 10000 yields the right value.
        /// </summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"DATETIME2FROMPARTS({yearSql}, {monthSql}, {daySql}, {hourSql}, {minuteSql}, {secondSql}, ({millisecondSql}) * 10000, 7)";

        /// <summary>T-SQL DATEFROMPARTS builds a DATE from int parts.</summary>
        public override string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"DATEFROMPARTS({yearSql}, {monthSql}, {daySql})";

        /// <summary>T-SQL TIMEFROMPARTS(h, m, s, fraction, precision); 0/0 = no sub-second.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => $"TIMEFROMPARTS({hourSql}, {minuteSql}, {secondSql}, 0, 0)";

        /// <summary>4-arg TIMEFROMPARTS: fractions=ms, precision=3.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"TIMEFROMPARTS({hourSql}, {minuteSql}, {secondSql}, {millisecondSql}, 3)";

        /// <summary>
        /// T-SQL has no native regex primitive. The supported workarounds are
        /// a CLR scalar function (sql_clr assembly providing RegExMatch) or
        /// a rewrite to LIKE for simple patterns. nORM cannot detect either
        /// at translation time, so surface a clear unsupported-feature
        /// exception rather than emitting broken SQL.
        /// </summary>
        public override string GetRegexMatchSql(string inputSql, string patternLiteral)
            => throw new NormUnsupportedFeatureException(
                "Regex.IsMatch is not translatable on SQL Server: T-SQL has no built-in regex " +
                "primitive. Workarounds: (a) deploy a CLR scalar function (RegExMatch) and call " +
                "it via [SqlFunction], (b) rewrite the predicate as a LIKE pattern when the " +
                "shape allows, or (c) materialise the rows first and filter in memory.");

        /// <summary>
        /// T-SQL has no regexp_replace primitive. Same workarounds as GetRegexMatchSql.
        /// </summary>
        public override string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => throw new NormUnsupportedFeatureException(
                "Regex.Replace is not translatable on SQL Server: T-SQL has no built-in regex " +
                "primitive. Workarounds: (a) deploy a CLR scalar function (RegExReplace) and " +
                "call it via [SqlFunction], (b) materialise the rows first and apply Regex.Replace " +
                "in memory.");

        /// <summary>
        /// SQL Server stores TimeOnly as TIME(7). DATEDIFF(SECOND, t1, t2) on
        /// two TIMEs returns the signed second diff in (-86400, 86400). Wrap
        /// with +86400 then % 86400 to match TimeOnly's [0, 24h) semantics.
        /// </summary>
        public override string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
            => $"CAST(((DATEDIFF(SECOND, {startSql}, {endSql}) + 86400) % 86400) AS FLOAT)";

        /// <summary>
        /// Overload-aware Math.Round / decimal.Round handling so MidpointRounding
        /// arguments dispatch to the correct emit instead of being silently
        /// coerced to a digit-count integer (the int value of the enum).
        /// SqlServer-native ROUND(x [, n]) is AwayFromZero; ROUND(x, n, 1) is
        /// the truncate-toward-zero form via the truncate flag.
        /// </summary>
        public override string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args)
            => TryTranslateMathRoundWithMode(node, args,
                awayFromZero: (x, digits) => digits == null ? $"ROUND({x}, 0)" : $"ROUND({x}, {digits})",
                truncateTowardZero: (x, digits) => digits == null ? $"ROUND({x}, 0, 1)" : $"ROUND({x}, {digits}, 1)")
            ?? TryTranslateIeee754Predicate(node, args)
            ?? TryTranslateTimeSpanFactory(node, args);

        /// <summary>
        /// double / float IEEE 754 predicates (IsNaN / IsInfinity / IsFinite /
        /// IsPositiveInfinity / IsNegativeInfinity). T-SQL has no native
        /// primitives; emit the algebraic form (x != x for IsNaN, CAST('Infinity'
        /// AS FLOAT) comparison for IsInfinity) so the predicate works on
        /// computed float expressions (e.g. division results) even though SQL
        /// Server's FLOAT type itself usually rejects NaN/Infinity at insert.
        /// </summary>
        private static string? TryTranslateIeee754Predicate(System.Linq.Expressions.MethodCallExpression node, string[] args)
        {
            var dt = node.Method.DeclaringType;
            if ((dt != typeof(double) && dt != typeof(float)) || args.Length != 1) return null;
            const string pInf = "CAST('Infinity' AS FLOAT)";
            const string nInf = "CAST('-Infinity' AS FLOAT)";
            return node.Method.Name switch
            {
                "IsNaN" => $"({args[0]} != {args[0]})",
                "IsInfinity" => $"({args[0]} = {pInf} OR {args[0]} = {nInf})",
                "IsFinite" => $"({args[0]} = {args[0]} AND {args[0]} != {pInf} AND {args[0]} != {nInf})",
                "IsPositiveInfinity" => $"({args[0]} = {pInf})",
                "IsNegativeInfinity" => $"({args[0]} = {nInf})",
                // Normal: finite, non-zero, |x| >= min normal positive double.
                "IsNormal" => $"({args[0]} = {args[0]} AND {args[0]} != {pInf} AND {args[0]} != {nInf} " +
                              $"AND {args[0]} != 0 AND ABS({args[0]}) >= 2.2250738585072014E-308)",
                // Subnormal: non-zero AND |x| < min normal positive.
                "IsSubnormal" => $"({args[0]} != 0 AND ABS({args[0]}) < 2.2250738585072014E-308)",
                _ => null
            };
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
                    nameof(string.Trim) when args.Length == 1 => $"LTRIM(RTRIM({args[0]}))",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // T-SQL has no LPAD/RPAD primitive. Build via REPLICATE
                    // + concatenation, with a CASE gate so a longer-than-
                    // target input passes through unchanged (matching .NET
                    // PadLeft/PadRight's "never truncates" contract).
                    nameof(string.PadLeft) when args.Length == 2 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE REPLICATE(' ', {args[1]} - LEN({args[0]})) + {args[0]} END)",
                    nameof(string.PadLeft) when args.Length == 3 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE REPLICATE({args[2]}, {args[1]} - LEN({args[0]})) + {args[0]} END)",
                    nameof(string.PadRight) when args.Length == 2 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE {args[0]} + REPLICATE(' ', {args[1]} - LEN({args[0]})) END)",
                    nameof(string.PadRight) when args.Length == 3 =>
                        $"(CASE WHEN LEN({args[0]}) >= {args[1]} THEN {args[0]} " +
                        $"ELSE {args[0]} + REPLICATE({args[2]}, {args[1]} - LEN({args[0]})) END)",
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

            if (declaringType == typeof(DateTimeOffset))
            {
                // DateTimeOffset members specific to the offset-aware type. Handle
                // these BEFORE the shared DateTime branch so DateTime.X never
                // accidentally swallows them via name collision.
                var dtoMatch = name switch
                {
                    // SWITCHOFFSET(col, 0) shifts the wall-clock to UTC; CAST AS
                    // DATETIME2 drops the offset suffix so the materializer reads
                    // a plain DATETIME2 -> DateTime(Kind=Unspecified).
                    nameof(DateTimeOffset.UtcDateTime) => $"CAST(SWITCHOFFSET({args[0]}, 0) AS DATETIME2)",
                    // The wall-clock portion (ignoring the offset) is just the
                    // DATETIME2 cast of the value -- matches .NET DateTimeOffset.
                    // DateTime returning Kind=Unspecified.
                    nameof(DateTimeOffset.DateTime) => $"CAST({args[0]} AS DATETIME2)",
                    // DATEPART(TZoffset, x) returns the offset in MINUTES as int;
                    // multiply by 60 to get seconds and CAST to FLOAT so the
                    // materializer's "double -> TimeSpan.FromSeconds" path picks
                    // it up correctly.
                    nameof(DateTimeOffset.Offset) => $"CAST((DATEPART(TZoffset, {args[0]}) * 60) AS FLOAT)",
                    _ => null
                };
                if (dtoMatch != null) return dtoMatch;
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
                return name switch
                {
                    nameof(DateTime.Millisecond) => $"DATEPART(millisecond, {args[0]})",
                    // Microsecond returns the microsecond within the current
                    // millisecond (0..999), not the whole-second microsecond.
                    // T-SQL DATEPART(microsecond) returns 0..999999; modulo
                    // 1000 yields the microsecond-within-millisecond.
                    nameof(DateTime.Microsecond) => $"(DATEPART(microsecond, {args[0]}) % 1000)",
                    // SQL Server DATEPART(nanosecond) returns 0..999999900 with
                    // 100ns precision; modulo 1000 yields the 100ns-within-
                    // microsecond, matching .NET's 0..900 in 100 steps.
                    nameof(DateTime.Nanosecond) => $"(DATEPART(nanosecond, {args[0]}) % 1000)",
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
                    // AddMilliseconds: DATEADD supports the `millisecond` datepart.
                    nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"DATEADD(millisecond, {args[1]}, {args[0]})",
                    // AddTicks: a tick = 100ns. SqlServer DATEADD(NANOSECOND, ...) exists
                    // in 2008+ but the underlying DATETIME2 has 100ns precision so sub-tick
                    // precision is dropped. The argument range matters: ticks * 100 can
                    // overflow long. Split into seconds (whole ticks/1e7) + nanosecond
                    // remainder ((ticks % 1e7) * 100) so each component stays in int32 range.
                    nameof(DateTime.AddTicks) when args.Length == 2 =>
                        $"DATEADD(nanosecond, (({args[1]}) % 10000000) * 100, DATEADD(second, ({args[1]}) / 10000000, {args[0]}))",
                    // T-SQL DATEPART(weekday) depends on @@DATEFIRST; subtract @@DATEFIRST so that
                    // Sunday=0..Saturday=6 always, matching System.DayOfWeek.
                    nameof(DateTime.DayOfWeek) => $"((DATEPART(weekday, {args[0]}) + @@DATEFIRST - 1) % 7)",
                    // Compare / CompareTo: return the signed -1/0/1 sentinel
                    // matching .NET IComparable. Static Compare(a, b) gets two
                    // arg slots (both pre-translated to SQL fragments);
                    // instance CompareTo collapses to the same shape since
                    // node.Object is passed as args[0].
                    nameof(DateTime.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.IsLeapYear) when args.Length == 1 => BuildIsLeapYearSql(args[0]),
                    nameof(DateTime.DaysInMonth) when args.Length == 2 => BuildDaysInMonthSql(args[0], args[1]),
                    _ => null
                };
            }

            if (declaringType == typeof(TimeSpan))
            {
                return name switch
                {
                    nameof(TimeSpan.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(TimeSpan.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
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
                    // T-SQL DATEPART(weekday) depends on @@DATEFIRST; subtract @@DATEFIRST
                    // so Sunday=0..Saturday=6 always, matching System.DayOfWeek.
                    nameof(DateOnly.DayOfWeek) => $"((DATEPART(weekday, {args[0]}) + @@DATEFIRST - 1) % 7)",
                    nameof(DateOnly.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    // DateOnly.DayNumber: days since DateOnly.MinValue (0001-01-01).
                    // SQL Server DATEDIFF(DAY, '0001-01-01', x) returns int days
                    // matching .NET's day-count semantics across the full range.
                    nameof(DateOnly.DayNumber) => $"DATEDIFF(DAY, CAST('0001-01-01' AS DATE), {args[0]})",
                    nameof(DateOnly.FromDayNumber) when args.Length == 1 =>
                        $"DATEADD(DAY, {args[0]}, CAST('0001-01-01' AS DATE))",
                    nameof(DateOnly.FromDateTime) when args.Length == 1 =>
                        $"CAST({args[0]} AS DATE)",
                    // DateOnly.ToDateTime(TimeOnly) combines the two parts into a
                    // wall-clock DATETIME2. Add the time-as-seconds to a midnight
                    // datetime built from the date portion. SQL Server doesn't
                    // accept native (DATE + TIME) addition, so use DATEADD.
                    nameof(DateOnly.ToDateTime) when args.Length == 2 =>
                        $"DATEADD(SECOND, DATEDIFF(SECOND, CAST('00:00:00' AS TIME), {args[1]}), CAST({args[0]} AS DATETIME2))",
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
                    nameof(TimeOnly.Millisecond) => $"DATEPART(millisecond, {args[0]})",
                    // CAST DATETIME2 -> TIME extracts the wall-clock TIME portion.
                    nameof(TimeOnly.FromDateTime) when args.Length == 1 => $"CAST({args[0]} AS TIME)",
                    // TimeSpan is bound as TIME on SqlServer (column-side); the
                    // CAST is a no-op for TIME but explicit for clarity.
                    nameof(TimeOnly.FromTimeSpan) when args.Length == 1 => $"CAST({args[0]} AS TIME)",
                    nameof(TimeOnly.Microsecond) => $"(DATEPART(microsecond, {args[0]}) % 1000)",
                    nameof(TimeOnly.Nanosecond) => $"(DATEPART(nanosecond, {args[0]}) % 1000)",
                    // Ticks = 100ns since midnight; DATEDIFF_BIG(NANOSECOND)
                    // returns ns count, /100 yields ticks.
                    nameof(TimeOnly.Ticks) => $"(DATEDIFF_BIG(NANOSECOND, CAST('00:00:00' AS TIME), {args[0]}) / 100)",
                    // IsBetween(start, end) wraps around midnight when start > end.
                    // Matches .NET's TimeOnly.IsBetween semantics.
                    nameof(TimeOnly.IsBetween) when args.Length == 3 =>
                        $"(CASE WHEN {args[1]} <= {args[2]} THEN ({args[0]} >= {args[1]} AND {args[0]} < {args[2]}) " +
                        $"ELSE ({args[0]} >= {args[1]} OR {args[0]} < {args[2]}) END)",
                    nameof(TimeOnly.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    _ => null
                };
            }

            if (declaringType == typeof(NormFunctions))
            {
                return name switch
                {
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
                    // Server-side primitives matching .NET semantics: GETUTCDATE
                    // returns DATETIME (UTC); NEWID is a Guid-shaped uniqueidentifier;
                    // RAND() returns float in [0, 1).
                    nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "GETUTCDATE()",
                    nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => "NEWID()",
                    nameof(NormFunctions.ServerRandom) when args.Length == 0 => "RAND()",
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
                    // Basic trig + inverse trig. T-SQL spells two-argument
                    // arctangent ATN2 (legacy from SQL Server's Sybase
                    // ancestry), NOT ATAN2 -- this is the only per-provider
                    // wrinkle in the trig set.
                    nameof(Math.Sin) when args.Length == 1 => $"SIN({args[0]})",
                    nameof(Math.Cos) when args.Length == 1 => $"COS({args[0]})",
                    nameof(Math.Tan) when args.Length == 1 => $"TAN({args[0]})",
                    nameof(Math.Asin) when args.Length == 1 => $"ASIN({args[0]})",
                    nameof(Math.Acos) when args.Length == 1 => $"ACOS({args[0]})",
                    nameof(Math.Atan) when args.Length == 1 => $"ATAN({args[0]})",
                    nameof(Math.Atan2) when args.Length == 2 => $"ATN2({args[0]}, {args[1]})",
                    // Hyperbolic + inverse hyperbolic: T-SQL has no native
                    // functions for these. Emit the algebraic identities via
                    // EXP / LOG (natural log) / SQRT. Identical math to .NET's
                    // managed implementation; precision matches double for the
                    // safe input domain.
                    //   sinh(x) = (exp(x) - exp(-x)) / 2
                    //   cosh(x) = (exp(x) + exp(-x)) / 2
                    //   tanh(x) = (exp(2x) - 1) / (exp(2x) + 1)
                    //     (cleaner than sinh/cosh ratio; avoids cosh-divide
                    //     numerical instability for large |x|).
                    //   asinh(x) = ln(x + sqrt(x^2 + 1))
                    //   acosh(x) = ln(x + sqrt(x^2 - 1))  (domain x >= 1)
                    //   atanh(x) = 0.5 * ln((1+x) / (1-x))  (domain |x| < 1)
                    nameof(Math.Sinh) when args.Length == 1 =>
                        $"((EXP({args[0]}) - EXP(-({args[0]}))) / 2)",
                    nameof(Math.Cosh) when args.Length == 1 =>
                        $"((EXP({args[0]}) + EXP(-({args[0]}))) / 2)",
                    nameof(Math.Tanh) when args.Length == 1 =>
                        $"((EXP(2 * ({args[0]})) - 1) / (EXP(2 * ({args[0]})) + 1))",
                    nameof(Math.Asinh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POWER({args[0]}, 2) + 1))",
                    nameof(Math.Acosh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POWER({args[0]}, 2) - 1))",
                    nameof(Math.Atanh) when args.Length == 1 =>
                        $"(0.5 * LOG((1 + ({args[0]})) / (1 - ({args[0]}))))",
                    // Extended Math methods present on SQLite but not in T-SQL.
                    // Cbrt: POWER(x, 1.0/3.0); for x >= 0 matches Math.Cbrt.
                    nameof(Math.Cbrt) when args.Length == 1 => $"POWER({args[0]}, 1.0/3.0)",
                    // T-SQL LOG(value, base): second arg is the base.
                    nameof(Math.Log2) when args.Length == 1 => $"LOG({args[0]}, 2)",
                    nameof(Math.MaxMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) >= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.MinMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) <= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.ScaleB) when args.Length == 2 =>
                        $"({args[0]} * POWER(CAST(2 AS FLOAT), {args[1]}))",
                    // BigMul(int, int) -> long. Widen one operand to BIGINT
                    // before multiplying or the product overflows when
                    // |a * b| > 2^31 - 1.
                    nameof(Math.BigMul) when args.Length == 2 =>
                        $"(CAST({args[0]} AS BIGINT) * {args[1]})",
                    nameof(Math.CopySign) when args.Length == 2 =>
                        $"(ABS({args[0]}) * SIGN({args[1]}))",
                    // Math.Clamp(value, min, max) = MIN(MAX(value, min), max).
                    // T-SQL has no scalar Min/Max so emit nested CASE.
                    nameof(Math.Clamp) when args.Length == 3 =>
                        $"(CASE WHEN (CASE WHEN {args[0]} > {args[1]} THEN {args[0]} ELSE {args[1]} END) < {args[2]} " +
                        $"THEN (CASE WHEN {args[0]} > {args[1]} THEN {args[0]} ELSE {args[1]} END) ELSE {args[2]} END)",
                    // IEEERemainder(x, y) = x - y * round(x/y, ToEven). Same banker's
                    // CASE algebra as SQLite's emit since T-SQL's ROUND is also
                    // half-away-from-zero, not ToEven.
                    nameof(Math.IEEERemainder) when args.Length == 2 =>
                        $"({args[0]} - {args[1]} * ((CASE WHEN ({args[0]})/({args[1]}) >= 0 THEN 1 ELSE -1 END) * " +
                        $"(CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) + " +
                        $"CASE " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) > 0.5 THEN 1 " +
                        $"WHEN ABS(({args[0]})/({args[1]})) - CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) < 0.5 THEN 0 " +
                        $"ELSE (CAST(ABS(({args[0]})/({args[1]})) AS BIGINT) % 2) END)))",
                    _ => null
                };
            }

            if (declaringType == typeof(decimal))
            {
                return name switch
                {
                    // T-SQL ROUND with truncate flag (1) drops fractional digits.
                    nameof(decimal.Truncate) when args.Length == 1 => $"ROUND({args[0]}, 0, 1)",
                    nameof(decimal.Floor) when args.Length == 1 => $"FLOOR({args[0]})",
                    nameof(decimal.Ceiling) when args.Length == 1 => $"CEILING({args[0]})",
                    nameof(decimal.Abs) when args.Length == 1 => $"ABS({args[0]})",
                    nameof(decimal.Negate) when args.Length == 1 => $"(-({args[0]}))",
                    nameof(decimal.Add) when args.Length == 2 => $"({args[0]} + {args[1]})",
                    nameof(decimal.Subtract) when args.Length == 2 => $"({args[0]} - {args[1]})",
                    nameof(decimal.Multiply) when args.Length == 2 => $"({args[0]} * {args[1]})",
                    nameof(decimal.Divide) when args.Length == 2 => $"({args[0]} / {args[1]})",
                    nameof(decimal.Remainder) when args.Length == 2 => $"({args[0]} % {args[1]})",
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
