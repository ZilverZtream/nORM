using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Database provider tailored for MySQL and MariaDB.
    /// Implements SQL dialect nuances, bulk operations and connection features
    /// that leverage MySQL capabilities for high throughput data access.
    /// </summary>
    public sealed class MySqlProvider : DatabaseProvider
    {
        internal override bool ParameterizeFastPathBooleanPredicates => true;

        internal override bool SupportsFastPathPreparedCommandCache => true;

        internal override bool SupportsCommandGeneratedKeyRetrieval => true;

        internal override bool PrefersSyncFastPathExecution => true;

        internal override bool PrefersSyncQueryPlanExecution => true;

        internal override bool SupportsQueryPlanPreparedCommandCache => true;

        /// <summary>Maximum number of cached DataTable schemas used for bulk insert data tables.</summary>
        private const int TableSchemaCacheSize = 100;

        private static readonly ConcurrentLruCache<Type, DataTable> _tableSchemas = new(maxSize: TableSchemaCacheSize);
        private static readonly ConcurrentDictionary<string, bool> _bulkCopyUnavailable = new(StringComparer.Ordinal);
        private static readonly ConcurrentDictionary<Type, Func<DbCommand, object?>> _lastInsertedIdAccessors = new();
        private readonly IDbParameterFactory _parameterFactory;
        private readonly bool _useAffectedRowsSemantics;

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <remarks>
        /// Uses a reflection-based MySQL parameter factory. Consumer applications must
        /// reference either <c>MySqlConnector</c> or <c>MySql.Data</c> when executing
        /// MySQL commands.
        /// </remarks>
        public MySqlProvider()
            : this(new ReflectionMySqlParameterFactory(), useAffectedRowsSemantics: true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <param name="useAffectedRowsSemantics">
        /// <see langword="true"/> when the MySQL driver reports changed rows for UPDATE statements.
        /// Use <see langword="false"/> only when the connection string is configured for matched-row
        /// semantics, for example <c>UseAffectedRows=false</c> with MySqlConnector.
        /// </param>
        public MySqlProvider(bool useAffectedRowsSemantics)
            : this(new ReflectionMySqlParameterFactory(), useAffectedRowsSemantics)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <param name="parameterFactory">Factory used to create provider-specific parameters.</param>
        /// <remarks>
        /// <para>
        /// <b>Runtime dependency:</b> This provider uses MySQL via reflection and requires either
        /// <c>MySqlConnector</c> or <c>MySql.Data</c> to be installed in the consuming application.
        /// Add one of the following to your project:
        /// <code>
        /// dotnet add package MySqlConnector
        /// </code>
        /// or
        /// <code>
        /// dotnet add package MySql.Data
        /// </code>
        /// A clear <see cref="InvalidOperationException"/> is thrown at runtime if neither package is present.
        /// </para>
        /// </remarks>
        public MySqlProvider(IDbParameterFactory parameterFactory)
            : this(parameterFactory, useAffectedRowsSemantics: true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlProvider"/> class.
        /// </summary>
        /// <param name="parameterFactory">Factory used to create provider-specific parameters.</param>
        /// <param name="useAffectedRowsSemantics">
        /// <see langword="true"/> when the MySQL driver reports changed rows for UPDATE statements.
        /// Use <see langword="false"/> only when the connection string is configured for matched-row
        /// semantics, for example <c>UseAffectedRows=false</c> with MySqlConnector.
        /// </param>
        /// <remarks>
        /// When this value is <see langword="false"/>, nORM treats zero-row UPDATE results as strict
        /// optimistic-concurrency conflicts. The database connection must be configured consistently;
        /// setting this to <see langword="false"/> while the driver still uses affected-row semantics
        /// can produce false-positive conflicts for same-value updates.
        /// </remarks>
        public MySqlProvider(IDbParameterFactory parameterFactory, bool useAffectedRowsSemantics)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
            _useAffectedRowsSemantics = useAffectedRowsSemantics;
            // Dialect-only mode: when a non-native parameter factory is supplied, the caller is using
            // this provider purely for its SQL dialect against a foreign engine (e.g., SQLite). Native
            // connection-type and server-version validation must be skipped in that scenario.
            _isDialectOnly = parameterFactory is not ReflectionMySqlParameterFactory;
        }

        private readonly bool _isDialectOnly;

        private sealed class ReflectionMySqlParameterFactory : IDbParameterFactory
        {
            private readonly Type? _parameterType =
                Type.GetType("MySqlConnector.MySqlParameter, MySqlConnector") ??
                Type.GetType("MySql.Data.MySqlClient.MySqlParameter, MySql.Data");

            public DbParameter CreateParameter(string name, object? value)
            {
                if (_parameterType == null)
                {
                    throw new InvalidOperationException(
                        "MySQL package is required for MySQL support. Add PackageReference Include=\"MySqlConnector\" or Include=\"MySql.Data\" to the consuming project.");
                }

                return (DbParameter)Activator.CreateInstance(_parameterType, name, value ?? DBNull.Value)!;
            }
        }

        /// <summary>
        /// Maximum allowed length of a single SQL statement in characters.
        /// </summary>
        public override int MaxSqlLength => 4_194_304;

        /// <summary>
        /// Maximum number of parameters permitted in a single MySQL command.
        /// </summary>
        public override int MaxParameters => 65_535;

        /// <inheritdoc />
        public override ProviderCapabilities Capabilities => new(
            "MySQL",
            new Version(8, 0),
            MaxParameters,
            true,
            true,
            true,
            true,
            "Requires MySqlConnector or MySql.Data and MySQL 8.0 or newer.");

        /// <summary>
        /// Escapes an identifier using MySQL backtick delimiters.
        /// Handles multi-part identifiers (schema.table) by escaping each segment separately so
        /// that <c>`schema`.`table`</c> is produced rather than the invalid <c>`schema.table`</c>.
        /// Embedded backtick characters are doubled to prevent SQL injection via identifiers.
        /// </summary>
        /// <param name="id">The identifier to escape.</param>
        /// <returns>The escaped identifier.</returns>
        public override string Escape(string id)
        {
            if (string.IsNullOrWhiteSpace(id)) return id;
            if (id.Contains('.'))
                return string.Join(".", id.Split('.').Select(part => $"`{part.Replace("`", "``")}`"));
            return $"`{id.Replace("`", "``")}`";
        }

        /// <summary>
        /// Appends a MySQL <c>LIMIT</c> clause to the SQL builder to paginate results.
        /// </summary>
        /// <param name="sb">The SQL builder being appended to.</param>
        /// <param name="limit">The maximum number of rows to return.</param>
        /// <param name="offset">The number of rows to skip before returning results.</param>
        /// <param name="limitParameterName">Parameter name for the limit value.</param>
        /// <param name="offsetParameterName">Parameter name for the offset value.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            // MySQL uses a single LIMIT clause for both limit and offset in the form
            // "LIMIT offset, limit". The previous implementation only emitted the clause
            // when a limit was present which meant that queries using only Skip() would
            // ignore the offset. MySQL requires a limit value when an offset is specified,
            // so when only an offset is provided we use the maximum unsigned BIGINT value
            // to effectively indicate "no limit".
            bool hasLimit = limitParameterName != null || limit.HasValue;
            bool hasOffset = offsetParameterName != null || offset.HasValue;
            if (hasLimit || hasOffset)
            {
                sb.Append(" LIMIT ");
                if (hasOffset)
                {
                    if (offsetParameterName != null) sb.Append(offsetParameterName);
                    else sb.Append(offset!.Value);
                    sb.Append(", ");
                }

                if (limitParameterName != null) sb.Append(limitParameterName);
                else if (limit.HasValue) sb.Append(limit.Value);
                else sb.Append("18446744073709551615");
            }
        }
        
        /// <summary>
        /// MySQL Connector/NET defaults to reporting <em>affected</em> (changed) rows rather than
        /// <em>matched</em> rows. Returning <c>true</c> here disables the rowcount-based OCC check
        /// in the save pipeline to prevent false-positive <see cref="DbConcurrencyException"/>s
        /// on same-value updates.
        ///
        /// <para><b>Known trade-off (S1):</b> With affected-row semantics, a genuine stale-row
        /// conflict where the concurrent writer sets the token to the <em>same</em> value is not
        /// detected. For full OCC guarantee on MySQL, add <c>UseAffectedRows=false</c> to the
        /// connection string and construct this provider with
        /// <c>useAffectedRowsSemantics: false</c>.</para>
        /// </summary>
        internal override bool UseAffectedRowsSemantics => _useAffectedRowsSemantics;

        /// <summary>
        /// Returns a SQL fragment that retrieves the last auto-incremented identity value.
        /// MySQL <c>LAST_INSERT_ID()</c> only supports numeric <c>AUTO_INCREMENT</c> columns.
        /// Non-numeric generated keys (Guid, string, …) must be assigned by the application or a trigger
        /// with <c>IsDbGenerated = false</c>.
        /// </summary>
        public override string GetIdentityRetrievalString(TableMapping m)
        {
            var keyCol = m?.KeyColumns.FirstOrDefault(c => c.IsDbGenerated);
            if (keyCol != null)
            {
                var keyType = Nullable.GetUnderlyingType(keyCol.Prop.PropertyType) ?? keyCol.Prop.PropertyType;
                if (!IsNumericType(keyType))
                    throw new NormConfigurationException(
                        $"MySQL LAST_INSERT_ID() only supports numeric AUTO_INCREMENT keys. " +
                        $"Entity '{m!.Type.Name}' has a db-generated key of type '{keyType.Name}'. " +
                        "Assign the key in application code (e.g. Guid.NewGuid()) and mark IsDbGenerated=false, " +
                        "or use a trigger with a numeric surrogate key.");
            }
            return "; SELECT LAST_INSERT_ID();";
        }

        internal override object? GetCommandGeneratedKey(DbCommand command, TableMapping mapping)
        {
            var accessor = _lastInsertedIdAccessors.GetOrAdd(command.GetType(), static commandType =>
            {
                var property = commandType.GetProperty("LastInsertedId");
                if (property == null)
                    return static _ => null;

                var commandParameter = Expression.Parameter(typeof(DbCommand), "command");
                var typedCommand = Expression.Convert(commandParameter, commandType);
                var propertyAccess = Expression.Property(typedCommand, property);
                var boxed = Expression.Convert(propertyAccess, typeof(object));
                return Expression.Lambda<Func<DbCommand, object?>>(boxed, commandParameter).Compile();
            });

            return accessor(command);
        }

        private static bool IsNumericType(Type t) =>
            t == typeof(int)   || t == typeof(long)   || t == typeof(short)  ||
            t == typeof(uint)  || t == typeof(ulong)  || t == typeof(ushort) ||
            t == typeof(byte)  || t == typeof(sbyte)  || t == typeof(decimal);

        /// <summary>MySQL CAST(... AS CHAR) is the cross-version-safe text conversion target.</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS CHAR)";

        /// <summary>
        /// MySQL's <c>FORMAT(x, N)</c> inserts thousand-separators by default
        /// ('1,234.50') which doesn't match .NET's <c>ToString("F2")</c>
        /// ('1234.50'). Wrap in REPLACE to strip the commas. The result still
        /// uses a decimal point regardless of server locale.
        /// </summary>
        public override string FormatFixedDecimalSql(string sql, int digits)
            => $"REPLACE(FORMAT({sql}, {digits}), ',', '')";

        /// <summary>
        /// MySQL uses <c>DATE_FORMAT(x, '%Y-%m-%d')</c> with strftime-like
        /// tokens but different minute token (%i, not %M which means month-name).
        /// Converts the .NET pattern; returns null for unsupported tokens.
        /// </summary>
        public override string? FormatDateUsingDotNetPattern(string sql, string dotNetFormat)
        {
            var sb = new System.Text.StringBuilder(dotNetFormat.Length + 4);
            int i = 0;
            while (i < dotNetFormat.Length)
            {
                if (i + 4 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 4).SequenceEqual("yyyy")) { sb.Append("%Y"); i += 4; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("yy")) { sb.Append("%y"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("MM")) { sb.Append("%m"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("dd")) { sb.Append("%d"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("HH")) { sb.Append("%H"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("mm")) { sb.Append("%i"); i += 2; continue; }
                if (i + 2 <= dotNetFormat.Length && dotNetFormat.AsSpan(i, 2).SequenceEqual("ss")) { sb.Append("%s"); i += 2; continue; }
                char c = dotNetFormat[i];
                if (c == 'M' || c == 'd' || c == 'H' || c == 'h' || c == 'm' || c == 's' || c == 'y'
                    || c == 'f' || c == 'F' || c == 'z' || c == 'K' || c == 't')
                    return null;
                if (c == '\'') sb.Append("''");
                else if (c == '%') sb.Append("%%");
                else sb.Append(c);
                i++;
            }
            return $"DATE_FORMAT({sql}, '{sb}')";
        }

        /// <summary>MySQL uses <c>DATE_ADD(col, INTERVAL N SECOND)</c>.</summary>
        public override string? AddSecondsToDateTimeSql(string dateTimeSql, string secondsSqlFragment)
            => $"DATE_ADD({dateTimeSql}, INTERVAL ({secondsSqlFragment}) SECOND)";

        /// <summary>MySQL uses ORD() for the code point of the first char.</summary>
        public override string GetCharCodeSql(string charSql) => $"ORD({charSql})";

        /// <summary>
        /// MySQL TimeSpan maps to TIME. TIME_TO_SEC extracts the seconds
        /// count; DATE_ADD applies the interval to the DateTime.
        /// </summary>
        public override string? AddTimeSpanColumnToDateTimeSql(string dateTimeSql, string timeSpanColumnSql, bool subtract)
        {
            var sign = subtract ? "-" : "";
            return $"DATE_ADD({dateTimeSql}, INTERVAL {sign}TIME_TO_SEC({timeSpanColumnSql}) SECOND)";
        }

        /// <summary>
        /// MySQL DATE_ADD returns DATETIME; wrap with DATE() to cast back to a
        /// DATE so the materializer reads a DateOnly-compatible value.
        /// </summary>
        public override string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment)
            => $"DATE(DATE_ADD({dateOnlySql}, INTERVAL ({daysSqlFragment}) DAY))";

        /// <summary>MySQL uses GROUP_CONCAT(expr SEPARATOR sep); no separator arg defaults to ','.</summary>
        public override string GetStringAggregateSql(string expr, string sepLiteral)
            => $"GROUP_CONCAT({expr} SEPARATOR {sepLiteral})";

        /// <summary>MySQL uses the REGEXP infix operator for regex match (synonym RLIKE).</summary>
        public override string GetRegexMatchSql(string inputSql, string patternLiteral)
            => $"({inputSql} REGEXP {patternLiteral})";

        /// <summary>MySQL 8.0+ has REGEXP_REPLACE(input, pattern, replacement).</summary>
        public override string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => $"REGEXP_REPLACE({inputSql}, {patternLiteral}, {replacementLiteral})";

        /// <summary>MySQL DATE_ADD returns DATETIME; DATE() casts back to DATE for the materializer.</summary>
        public override string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment)
            => $"DATE(DATE_ADD({dateOnlySql}, INTERVAL ({monthsSqlFragment}) MONTH))";

        /// <summary>MySQL DATE_ADD returns DATETIME; DATE() casts back to DATE for the materializer.</summary>
        public override string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment)
            => $"DATE(DATE_ADD({dateOnlySql}, INTERVAL ({yearsSqlFragment}) YEAR))";

        /// <summary>MySQL ADDTIME(TIME, SEC_TO_TIME(N)) stays TIME (no DATETIME promotion).</summary>
        public override string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment)
            => $"ADDTIME({timeOnlySql}, SEC_TO_TIME({secondsSqlFragment}))";

        /// <summary>MySQL ADDTIME / SUBTIME(TIME, TIME) stay TIME.</summary>
        public override string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract)
            => subtract
                ? $"SUBTIME({timeOnlySql}, {timeSpanColumnSql})"
                : $"ADDTIME({timeOnlySql}, {timeSpanColumnSql})";

        /// <summary>MySQL uses SIGNED / UNSIGNED for integer casts — `CAST(x AS INT)` is a syntax error.</summary>
        public override string GetIntCastSql(string innerSql, bool asLong = false)
            => $"CAST({innerSql} AS SIGNED)";

        /// <summary>MySQL supports INSERT IGNORE for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT IGNORE INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2})";

        /// <summary>
        /// Creates a new <see cref="DbParameter"/> for use with MySQL commands.
        /// </summary>
        public override DbParameter CreateParameter(string name, object? value) =>
            _parameterFactory.CreateParameter(name, value);

        /// <summary>
        /// MySQL TIMESTAMPDIFF(SECOND) is integer seconds. Wrap in CAST to DOUBLE so the
        /// subsequent division for TotalHours / TotalDays etc. stays fractional.
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"CAST(TIMESTAMPDIFF(SECOND, {startSql}, {endSql}) AS DOUBLE)";

        /// <summary>
        /// MySQL has no native date-from-parts; STR_TO_DATE on a zero-padded
        /// concat round-trips through the DATETIME parser.
        /// </summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"STR_TO_DATE(CONCAT_WS('-', {yearSql}, LPAD({monthSql}, 2, '0'), LPAD({daySql}, 2, '0')), '%Y-%m-%d')";

        /// <summary>6-arg STR_TO_DATE with full 'YYYY-MM-DD HH:MM:SS' shape.</summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql)
            => $"STR_TO_DATE(CONCAT(" +
               $"CONCAT_WS('-', {yearSql}, LPAD({monthSql}, 2, '0'), LPAD({daySql}, 2, '0')), " +
               $"' ', " +
               $"CONCAT_WS(':', LPAD({hourSql}, 2, '0'), LPAD({minuteSql}, 2, '0'), LPAD({secondSql}, 2, '0'))), " +
               $"'%Y-%m-%d %H:%i:%s')";

        /// <summary>MySQL DATE() wrapper around the STR_TO_DATE text shape ensures DATE return type.</summary>
        public override string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"DATE(STR_TO_DATE(CONCAT_WS('-', {yearSql}, LPAD({monthSql}, 2, '0'), LPAD({daySql}, 2, '0')), '%Y-%m-%d'))";

        /// <summary>MySQL MAKETIME builds a TIME from int parts; seconds accept double for sub-second.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => $"MAKETIME({hourSql}, {minuteSql}, {secondSql})";

        /// <summary>
        /// MySQL stores TimeOnly as TIME. TIMEDIFF returns a signed TIME diff;
        /// TIME_TO_SEC produces the integer-seconds form which can be negative.
        /// Wrap +86400 MOD 86400 to match TimeOnly's [0, 24h) semantics.
        /// </summary>
        public override string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
            => $"CAST(((TIME_TO_SEC(TIMEDIFF({endSql}, {startSql})) + 86400) MOD 86400) AS DOUBLE)";

        /// <summary>
        /// Overload-aware Math.Round / decimal.Round handling. MySQL ROUND(x [, n])
        /// is AwayFromZero; TRUNCATE(x, n) truncates toward zero.
        /// </summary>
        public override string? TranslateMethodCall(System.Linq.Expressions.MethodCallExpression node, string[] args)
            => TryTranslateMathRoundWithMode(node, args,
                awayFromZero: (x, digits) => digits == null ? $"ROUND({x})" : $"ROUND({x}, {digits})",
                truncateTowardZero: (x, digits) => digits == null ? $"TRUNCATE({x}, 0)" : $"TRUNCATE({x}, {digits})")
            ?? TryTranslateIeee754Predicate(node, args);

        /// <summary>
        /// MySQL DOUBLE rejects NaN / Infinity at insert by default; the
        /// predicate is typically false for stored values. The (x != x)
        /// algebraic form remains correct for computed float expressions
        /// such as division-by-zero columns.
        /// </summary>
        private static string? TryTranslateIeee754Predicate(System.Linq.Expressions.MethodCallExpression node, string[] args)
        {
            var dt = node.Method.DeclaringType;
            if ((dt != typeof(double) && dt != typeof(float)) || args.Length != 1) return null;
            return node.Method.Name switch
            {
                "IsNaN" => $"({args[0]} != {args[0]})",
                // POW(10, 400) overflows to a DOUBLE that MySQL treats as the
                // out-of-range marker -- emit the algebraic form that doesn't
                // require a portable +Infinity literal.
                "IsInfinity" => $"({args[0]} = {args[0]} AND ABS({args[0]}) > 1.7976931348623157E+307)",
                "IsFinite" => $"({args[0]} = {args[0]} AND ABS({args[0]}) <= 1.7976931348623157E+307)",
                "IsPositiveInfinity" => $"({args[0]} = {args[0]} AND {args[0]} > 1.7976931348623157E+307)",
                "IsNegativeInfinity" => $"({args[0]} = {args[0]} AND {args[0]} < -1.7976931348623157E+307)",
                "IsNormal" => $"({args[0]} = {args[0]} AND ABS({args[0]}) <= 1.7976931348623157E+307 " +
                              $"AND {args[0]} != 0 AND ABS({args[0]}) >= 2.2250738585072014E-308)",
                "IsSubnormal" => $"({args[0]} != 0 AND ABS({args[0]}) < 2.2250738585072014E-308)",
                _ => null
            };
        }

        /// <summary>
        /// Translates selected .NET methods to their MySQL SQL equivalents.
        /// </summary>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"CHAR_LENGTH({args[0]})",
                    nameof(string.Trim) when args.Length == 1 => $"TRIM({args[0]})",
                    nameof(string.TrimStart) when args.Length == 1 => $"LTRIM({args[0]})",
                    nameof(string.TrimEnd) when args.Length == 1 => $"RTRIM({args[0]})",
                    // MySQL LPAD/RPAD truncates when input length exceeds the
                    // target width; gate with CASE so longer inputs pass
                    // through unchanged, matching .NET PadLeft/PadRight's
                    // never-truncate contract.
                    nameof(string.PadLeft) when args.Length == 2 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE LPAD({args[0]}, {args[1]}, ' ') END)",
                    nameof(string.PadLeft) when args.Length == 3 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE LPAD({args[0]}, {args[1]}, {args[2]}) END)",
                    nameof(string.PadRight) when args.Length == 2 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE RPAD({args[0]}, {args[1]}, ' ') END)",
                    nameof(string.PadRight) when args.Length == 3 =>
                        $"(CASE WHEN CHAR_LENGTH({args[0]}) >= {args[1]} THEN {args[0]} ELSE RPAD({args[0]}, {args[1]}, {args[2]}) END)",
                    // MySQL SUBSTRING is 1-indexed; .NET Substring is 0-indexed, add 1.
                    nameof(string.Substring) when args.Length == 2 => $"SUBSTRING({args[0]}, ({args[1]}) + 1)",
                    nameof(string.Substring) when args.Length == 3 => $"SUBSTRING({args[0]}, ({args[1]}) + 1, {args[2]})",
                    nameof(string.Replace) when args.Length == 3 => $"REPLACE({args[0]}, {args[1]}, {args[2]})",
                    // LOCATE returns 1-based position or 0 if not found; .NET IndexOf is 0-based
                    // returning -1, so subtract 1.
                    nameof(string.IndexOf) when args.Length == 2 => $"(LOCATE({args[1]}, {args[0]}) - 1)",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTimeOffset))
            {
                // MySQL has no native DATETIMEOFFSET type. MySqlConnector
                // stores DateTimeOffset as DATETIME / TIMESTAMP normalized
                // to UTC, so the column represents a UTC instant and the
                // offset portion is always TimeSpan.Zero from a query
                // perspective.
                var dtoMatch = name switch
                {
                    nameof(DateTimeOffset.UtcDateTime) => args[0],
                    nameof(DateTimeOffset.DateTime) => args[0],
                    // REAL-seconds emit for the materializer's double ->
                    // TimeSpan.FromSeconds path, yielding TimeSpan.Zero.
                    nameof(DateTimeOffset.Offset) => "CAST(0 AS DOUBLE)",
                    _ => null
                };
                if (dtoMatch != null) return dtoMatch;
            }

            if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"YEAR({args[0]})",
                    nameof(DateTime.Month) => $"MONTH({args[0]})",
                    nameof(DateTime.Day) => $"DAY({args[0]})",
                    nameof(DateTime.Hour) => $"HOUR({args[0]})",
                    nameof(DateTime.Minute) => $"MINUTE({args[0]})",
                    nameof(DateTime.Second) => $"SECOND({args[0]})",
                    nameof(DateTime.DayOfYear) => $"DAYOFYEAR({args[0]})",
                    nameof(DateTime.Date) => $"DATE({args[0]})",
                    nameof(DateTime.AddDays) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) DAY)",
                    nameof(DateTime.AddMonths) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) MONTH)",
                    nameof(DateTime.AddYears) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) YEAR)",
                    nameof(DateTime.AddHours) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) HOUR)",
                    nameof(DateTime.AddMinutes) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) MINUTE)",
                    nameof(DateTime.AddSeconds) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL ({args[1]}) SECOND)",
                    // MySQL DATETIME(6) supports microsecond precision; ms*1000 = microseconds.
                    nameof(DateTime.AddMilliseconds) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL (({args[1]}) * 1000) MICROSECOND)",
                    // MySQL DOES NOT expose MILLISECOND() but MICROSECOND returns
                    // 0..999999; integer-divide by 1000 to get the ms component.
                    nameof(DateTime.Millisecond) => $"(MICROSECOND({args[0]}) DIV 1000)",
                    nameof(DateTime.Microsecond) => $"(MICROSECOND({args[0]}) % 1000)",
                    // MySQL DATETIME(6) has microsecond precision; the sub-
                    // microsecond Nanosecond is always zero.
                    nameof(DateTime.Nanosecond) => "0",
                    // 1 tick = 100ns. MySQL's smallest interval unit is MICROSECOND so
                    // ticks/10 gives microseconds (sub-microsecond truncates).
                    nameof(DateTime.AddTicks) when args.Length == 2 => $"DATE_ADD({args[0]}, INTERVAL (({args[1]}) / 10) MICROSECOND)",
                    // MySQL DAYOFWEEK returns 1=Sun..7=Sat; .NET DayOfWeek is 0=Sun..6=Sat.
                    nameof(DateTime.DayOfWeek) => $"(DAYOFWEEK({args[0]}) - 1)",
                    // Compare / CompareTo: signed -1/0/1 sentinel via CASE.
                    nameof(DateTime.Compare) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.CompareTo) when args.Length == 2 =>
                        $"(CASE WHEN {args[0]} < {args[1]} THEN -1 WHEN {args[0]} > {args[1]} THEN 1 ELSE 0 END)",
                    nameof(DateTime.IsLeapYear) when args.Length == 1 => BuildIsLeapYearSql(args[0]),
                    nameof(DateTime.DaysInMonth) when args.Length == 2 => BuildDaysInMonthSql(args[0], args[1]),
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
                    nameof(DateOnly.DayOfYear) => $"DAYOFYEAR({args[0]})",
                    // MySQL DAYOFWEEK returns 1=Sun..7=Sat; .NET DayOfWeek is 0=Sun..6=Sat.
                    nameof(DateOnly.DayOfWeek) => $"(DAYOFWEEK({args[0]}) - 1)",
                    // MySQL TO_DAYS uses proleptic Gregorian anchored at year 0;
                    // TO_DAYS('0001-01-01') == 366 (year 0 was 366 days). Subtract
                    // 366 so the result matches .NET DateOnly.MinValue == day 0.
                    nameof(DateOnly.DayNumber) => $"(TO_DAYS({args[0]}) - 366)",
                    nameof(DateOnly.FromDayNumber) when args.Length == 1 =>
                        $"FROM_DAYS(({args[0]}) + 366)",
                    nameof(DateOnly.FromDateTime) when args.Length == 1 =>
                        $"DATE({args[0]})",
                    // MySQL TIMESTAMP(date, time) combines into a DATETIME.
                    nameof(DateOnly.ToDateTime) when args.Length == 2 =>
                        $"TIMESTAMP({args[0]}, {args[1]})",
                    _ => null
                };
            }

            if (declaringType == typeof(TimeOnly))
            {
                return name switch
                {
                    nameof(TimeOnly.Hour) => $"HOUR({args[0]})",
                    nameof(TimeOnly.Minute) => $"MINUTE({args[0]})",
                    nameof(TimeOnly.Second) => $"SECOND({args[0]})",
                    nameof(TimeOnly.Millisecond) => $"(MICROSECOND({args[0]}) DIV 1000)",
                    // MySQL TIME() extracts the TIME portion from a DATETIME
                    // or coerces a TIME-typed expression.
                    nameof(TimeOnly.FromDateTime) when args.Length == 1 => $"TIME({args[0]})",
                    nameof(TimeOnly.FromTimeSpan) when args.Length == 1 => $"TIME({args[0]})",
                    nameof(TimeOnly.Microsecond) => $"(MICROSECOND({args[0]}) % 1000)",
                    nameof(TimeOnly.Nanosecond) => "0",
                    // TIME_TO_SEC returns the whole-second count; multiply by
                    // 10_000_000 ticks/sec. MICROSECOND yields 0..999999;
                    // multiply by 10 to get ticks (MySQL has microsecond precision).
                    nameof(TimeOnly.Ticks) => $"(TIME_TO_SEC({args[0]}) * 10000000 + MICROSECOND({args[0]}) * 10)",
                    // IsBetween(start, end) wraps around midnight when start > end.
                    nameof(TimeOnly.IsBetween) when args.Length == 3 =>
                        $"(CASE WHEN {args[1]} <= {args[2]} THEN ({args[0]} >= {args[1]} AND {args[0]} < {args[2]}) " +
                        $"ELSE ({args[0]} >= {args[1]} OR {args[0]} < {args[2]}) END)",
                    _ => null
                };
            }

            if (declaringType == typeof(NormFunctions))
            {
                return name switch
                {
                    // MySQL LIKE is case-insensitive on case-insensitive collations (the
                    // default for utf8mb4_general_ci) but case-sensitive on _bin / _cs
                    // collations. Forcing LOWER on both sides guarantees consistent
                    // ILIKE semantics regardless of column collation.
                    nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
                    // MySQL UTC_TIMESTAMP returns DATETIME in UTC; UUID() yields
                    // a 36-char hex-with-dashes string compatible with .NET Guid
                    // via Guid.Parse on the reader; RAND() is double in [0, 1).
                    nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "UTC_TIMESTAMP()",
                    nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => "UUID()",
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
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    nameof(Math.Sqrt) when args.Length == 1 => $"SQRT({args[0]})",
                    nameof(Math.Pow) when args.Length == 2 => $"POW({args[0]}, {args[1]})",
                    nameof(Math.Exp) when args.Length == 1 => $"EXP({args[0]})",
                    nameof(Math.Log) when args.Length == 1 => $"LN({args[0]})",
                    nameof(Math.Log) when args.Length == 2 => $"LOG({args[1]}, {args[0]})",
                    nameof(Math.Log10) when args.Length == 1 => $"LOG10({args[0]})",
                    nameof(Math.Sign) when args.Length == 1 => $"SIGN({args[0]})",
                    nameof(Math.Truncate) when args.Length == 1 => $"TRUNCATE({args[0]}, 0)",
                    nameof(Math.Min) when args.Length == 2 => $"LEAST({args[0]}, {args[1]})",
                    nameof(Math.Max) when args.Length == 2 => $"GREATEST({args[0]}, {args[1]})",
                    // Basic trig + inverse trig + 2-arg arctangent. All native
                    // in MySQL with .NET-matching names.
                    nameof(Math.Sin) when args.Length == 1 => $"SIN({args[0]})",
                    nameof(Math.Cos) when args.Length == 1 => $"COS({args[0]})",
                    nameof(Math.Tan) when args.Length == 1 => $"TAN({args[0]})",
                    nameof(Math.Asin) when args.Length == 1 => $"ASIN({args[0]})",
                    nameof(Math.Acos) when args.Length == 1 => $"ACOS({args[0]})",
                    nameof(Math.Atan) when args.Length == 1 => $"ATAN({args[0]})",
                    nameof(Math.Atan2) when args.Length == 2 => $"ATAN2({args[0]}, {args[1]})",
                    // Hyperbolic + inverse hyperbolic: MySQL has no native
                    // hyperbolic functions. Emit via algebraic identities
                    // using EXP / LOG (single-arg LOG is natural log in MySQL,
                    // matching SQL Server's LOG) / SQRT / POW. Same identities
                    // used in the SQL Server branch -- see that comment for
                    // the formula derivation and domain notes.
                    nameof(Math.Sinh) when args.Length == 1 =>
                        $"((EXP({args[0]}) - EXP(-({args[0]}))) / 2)",
                    nameof(Math.Cosh) when args.Length == 1 =>
                        $"((EXP({args[0]}) + EXP(-({args[0]}))) / 2)",
                    nameof(Math.Tanh) when args.Length == 1 =>
                        $"((EXP(2 * ({args[0]})) - 1) / (EXP(2 * ({args[0]})) + 1))",
                    nameof(Math.Asinh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POW({args[0]}, 2) + 1))",
                    nameof(Math.Acosh) when args.Length == 1 =>
                        $"LOG(({args[0]}) + SQRT(POW({args[0]}, 2) - 1))",
                    nameof(Math.Atanh) when args.Length == 1 =>
                        $"(0.5 * LOG((1 + ({args[0]})) / (1 - ({args[0]}))))",
                    // Extended Math methods. MySQL has POW and LOG(B, X).
                    nameof(Math.Cbrt) when args.Length == 1 => $"POW({args[0]}, 1.0/3.0)",
                    // MySQL LOG(B, X): base first.
                    nameof(Math.Log2) when args.Length == 1 => $"LOG(2, {args[0]})",
                    nameof(Math.MaxMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) >= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.MinMagnitude) when args.Length == 2 =>
                        $"CASE WHEN ABS({args[0]}) <= ABS({args[1]}) THEN {args[0]} ELSE {args[1]} END",
                    nameof(Math.ScaleB) when args.Length == 2 =>
                        $"({args[0]} * POW(2.0, {args[1]}))",
                    // MySQL SIGNED is 64-bit (BIGINT range); cast widens
                    // the int*int product so |a*b| > 2^31 doesn't overflow.
                    nameof(Math.BigMul) when args.Length == 2 =>
                        $"(CAST({args[0]} AS SIGNED) * {args[1]})",
                    nameof(Math.CopySign) when args.Length == 2 =>
                        $"(ABS({args[0]}) * SIGN({args[1]}))",
                    _ => null
                };
            }

            if (declaringType == typeof(decimal))
            {
                return name switch
                {
                    nameof(decimal.Truncate) when args.Length == 1 => $"TRUNCATE({args[0]}, 0)",
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

            return null;
        }

        /// <summary>
        /// Translates access to a JSON value using MySQL's <c>JSON_EXTRACT</c> function.
        /// </summary>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);
            return $"JSON_UNQUOTE(JSON_EXTRACT({columnName}, '{jsonPath}'))";
        }

        /// <summary>
        /// Introspects live column definitions via INFORMATION_SCHEMA.COLUMNS.
        /// Uses COLUMN_TYPE which already includes precision/length (e.g. decimal(10,4)).
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                var (schemaOverride, bareTable) = SplitSchemaTable(tableName);
                cmd.CommandText = schemaOverride != null
                    ? @"
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @t AND TABLE_SCHEMA = @s
ORDER BY ORDINAL_POSITION"
                    : @"
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @t AND TABLE_SCHEMA = DATABASE()
ORDER BY ORDINAL_POSITION";
                var p = cmd.CreateParameter(); p.ParameterName = "@t"; p.Value = bareTable; cmd.Parameters.Add(p);
                if (schemaOverride != null)
                {
                    var ps = cmd.CreateParameter(); ps.ParameterName = "@s"; ps.Value = schemaOverride; cmd.Parameters.Add(ps);
                }
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(0);
                    var sqlType = rdr.GetString(1);
                    var isNullable = rdr.GetString(2).Equals("YES", StringComparison.OrdinalIgnoreCase);
                    result.Add(new LiveColumnInfo(name, sqlType, isNullable));
                }
            }
            catch (DbException dbEx) when (IsObjectNotFoundError(dbEx))
            {
                // Table does not exist yet — return empty list.
            }
            return result;
        }

        /// <summary>
        /// Splits a possibly schema-qualified table name into its schema and table components.
        /// </summary>
        private static (string? Schema, string Table) SplitSchemaTable(string tableName)
        {
            var dot = tableName.IndexOf('.');
            if (dot < 0)
                return (null, tableName.Trim('`'));
            return (tableName[..dot].Trim('`'), tableName[(dot + 1)..].Trim('`'));
        }

        /// <summary>
        /// Generates the SQL statement to create the temporal history table for an entity.
        /// When liveColumns are supplied, column types are taken from the live DB schema.
        /// </summary>
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
                return $"{Escape(c.Name)} {GetSqlType(c.Prop.PropertyType)}";
            }));

            return $@"
CREATE TABLE {historyTable} (
    `__VersionId` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `__ValidFrom` DATETIME NOT NULL,
    `__ValidTo` DATETIME NOT NULL,
    `__Operation` CHAR(1) NOT NULL,
    {columns}
) ENGINE=InnoDB;";
        }

        /// <summary>
        /// Generates the SQL needed to create triggers that populate the temporal history table.
        /// </summary>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.Name)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.Name)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.Name)} = OLD.{Escape(c.Name)}"));

            return $@"
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_ai")};
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
FOR EACH ROW
BEGIN
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(), '9999-12-31', 'I', {newColumns});
END;
-- DELIMITER
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_au")};
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP() WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(), '9999-12-31', 'U', {newColumns});
END;
-- DELIMITER
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_ad")};
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP() WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'D', {oldColumns});
END;";
        }

        /// <summary>
        /// Validates that the supplied connection is compatible with the MySQL provider.
        /// </summary>
        /// <param name="connection">The connection to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not a MySQL connection.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (_isDialectOnly) return; // foreign parameter factory → dialect-only mode, foreign connection is intentional
            var name = connection.GetType().FullName;
            if (name != "MySqlConnector.MySqlConnection" && name != "MySql.Data.MySqlClient.MySqlConnection")
                throw new NormConfigurationException("A MySqlConnection is required for MySqlProvider. Please install MySqlConnector or MySql.Data.");
        }

        /// <summary>
        /// Checks whether the necessary MySQL client libraries are available and that a
        /// modern MySQL server (version 8.0 or higher) can be reached.
        /// </summary>
        public override async Task<bool> IsAvailableAsync()
        {
            var type =
                Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector") ??
                Type.GetType("MySql.Data.MySqlClient.MySqlConnection, MySql.Data");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString =
                "Server=localhost;Database=test;User=root;Password=;Allow User Variables=true";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SELECT VERSION()";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr.Split('-')[0]);
                return version >= new Version(8, 0);
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

        /// <summary>
        /// Creates a transaction savepoint to allow partial rollbacks when supported by the underlying MySQL driver.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/> rather than silently proceeding.
        /// </summary>
        /// <param name="transaction">Active transaction to create the savepoint on.</param>
        /// <param name="name">Name of the savepoint.</param>
        /// <param name="ct">Cancellation token for the asynchronous operation.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken — a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("CreateSavepoint", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("Savepoint", new[] { typeof(string) });
            if (saveMethod != null)
            {
                try
                {
                    saveMethod.Invoke(transaction, new object[] { name });

                    // Check after the sync call in case cancellation arrived mid-operation.
                    ct.ThrowIfCancellationRequested();

                    return Task.CompletedTask;
                }
                catch (System.Reflection.TargetInvocationException ex)
                {
                    // Unwrap and rethrow the inner exception from reflection invoke.
                    // TargetInvocationException wraps the actual database exception, making it harder to handle.
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
        /// Rolls back the transaction to a previously created savepoint.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/> rather than silently proceeding.
        /// </summary>
        /// <param name="transaction">The transaction containing the savepoint.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Cancellation token for the asynchronous operation.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken — a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            var rollbackMethod = transaction.GetType().GetMethod("Rollback", new[] { typeof(string) }) ??
                                 transaction.GetType().GetMethod("RollbackToSavepoint", new[] { typeof(string) });
            if (rollbackMethod != null)
            {
                try
                {
                    rollbackMethod.Invoke(transaction, new object[] { name });

                    // Check after the sync call in case cancellation arrived mid-operation.
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

        /// <summary>
        /// Performs a high-performance bulk insert using <c>INSERT INTO ... VALUES</c> statements grouped into batches.
        /// </summary>
        /// <typeparam name="T">Entity type being inserted.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> orchestrating the operation.</param>
        /// <param name="m">Mapping metadata for the entity's table.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows inserted.</returns>
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var operationKey = $"MySql_BulkInsert_{m.Type.Name}";
            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);

            var bulkCopyType = Type.GetType("MySqlConnector.MySqlBulkCopy, MySqlConnector");
            var bulkCopyKey = GetBulkCopyCapabilityKey(ctx.Connection);
            if (bulkCopyType != null &&
                ctx.Connection.GetType().FullName == "MySqlConnector.MySqlConnection" &&
                !_bulkCopyUnavailable.ContainsKey(bulkCopyKey))
            {
                // Respect ambient CurrentTransaction; only create a new one if none is active.
                bool ownedTx = ctx.CurrentTransaction == null;
                DbTransaction transaction = ctx.CurrentTransaction
                    ?? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
                try
                {
                    dynamic bulkCopy = Activator.CreateInstance(bulkCopyType, ctx.Connection, transaction)!;
                    bulkCopy.DestinationTableName = m.EscTable.Trim('`');
                    bulkCopy.BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                    var totalInserted = 0;
                    for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
                    {
                        var batch = entityList.GetRange(i, Math.Min(sizing.OptimalBatchSize, entityList.Count - i));
                        using var table = GetDataTable(m, insertableCols);
                        LoadBulkInsertRows(table, insertableCols, batch);

                        var batchSw = Stopwatch.StartNew();
                        await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
                        batchSw.Stop();
                        totalInserted += table.Rows.Count;
                        BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                    }

                    // Use CancellationToken.None so a cancelled caller token after a successful commit
                    // does not cause a spurious OperationCanceledException for already-committed data.
                    if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                    ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
                    ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
                    return totalInserted;
                }
                catch (Exception ex) when (IsMySqlBulkCopyUnavailable(ex))
                {
                    _bulkCopyUnavailable.TryAdd(bulkCopyKey, true);
                    if (ownedTx)
                    {
                        try { await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                        catch (Exception rollbackEx) { throw new AggregateException(ex, rollbackEx); }
                    }
                }
                catch (Exception ex)
                {
                    if (ownedTx)
                    {
                        try { await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                        catch (Exception rollbackEx) { throw new AggregateException(ex, rollbackEx); }
                    }
                    throw;
                }
                finally
                {
                    if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
                }
            }

            var affected = await base.BulkInsertAsync(ctx, m, entityList, ct).ConfigureAwait(false);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, affected, sw.Elapsed);
            return affected;
        }

        /// <inheritdoc />
        protected override async Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT VERSION()";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            if (_isDialectOnly) return Capabilities.MinimumServerVersion?.ToString();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT VERSION()";
            return cmd.ExecuteScalar() as string;
        }

        private static string GetBulkCopyCapabilityKey(DbConnection connection)
            => $"{connection.GetType().FullName}|{connection.DataSource}|{connection.Database}";

        private static bool IsMySqlBulkCopyUnavailable(Exception ex)
        {
            for (var current = ex; current != null; current = current.InnerException)
            {
                if (current is NotSupportedException)
                    return true;

                if (current.Message.Contains("Loading local data is disabled", StringComparison.OrdinalIgnoreCase) ||
                    current.Message.Contains("AllowLoadLocalInfile", StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Updates multiple entities using a MySQL-optimized temp table approach for efficient bulk updates.
        /// Always uses MySQL-specific implementation with temp tables for optimal performance.
        /// </summary>
        /// <typeparam name="T">Entity type being updated.</typeparam>
        /// <param name="ctx">The active <see cref="DbContext"/>.</param>
        /// <param name="m">Table mapping for the entity.</param>
        /// <param name="entities">Entities containing updated values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"`BulkUpdate_{Guid.NewGuid():N}`";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();
            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));

            var tempCreated = false;
            var updatedCount = 0;
            try
            {
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    cmd.CommandText = $"CREATE TEMPORARY TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                tempCreated = true;

                var tempMapping = new TableMapping(m.Type, this, ctx, null) { EscTable = tempTableName };
                await BulkInsertAsync(ctx, tempMapping, entities, ct).ConfigureAwait(false);

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
                        cmd.AddParam($"{ParamPrefix}__tenant_bulk", ctx.Options.TenantProvider.GetCurrentTenantId());
                    cmd.CommandText = BuildBulkUpdateSql(
                        m.EscTable,
                        tempTableName,
                        setClause,
                        joinClause,
                        ctx.Options.TenantProvider != null ? m.TenantColumn?.EscCol : null);
                    updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                // X4: Always drop temp table to prevent session resource leak
                if (tempCreated)
                {
                    try
                    {
                        await using var dropCmd = ctx.CreateCommand();
                        dropCmd.CommandText = BuildBulkUpdateDropTempTableSql(tempTableName);
                        await dropCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (DbException ex)
                    {
                        Trace.TraceWarning($"MySqlProvider: Failed to drop temp table {tempTableName} during BulkUpdate cleanup: {ex.Message}");
                    }
                }
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
            return updatedCount;
        }

        internal string BuildBulkUpdateSql(string tableName, string tempTableName, string setClause, string joinClause, string? tenantColumn)
        {
            var sql = $"UPDATE {tableName} T1 JOIN {tempTableName} T2 ON {joinClause} SET {setClause}";
            return tenantColumn == null
                ? sql
                : $"{sql} WHERE T1.{tenantColumn} = {ParamPrefix}__tenant_bulk";
        }

        internal static string BuildBulkUpdateDropTempTableSql(string tempTableName)
            => $"DROP TEMPORARY TABLE IF EXISTS {tempTableName}";
        
        /// <summary>
        /// Deletes multiple records using MySQL-optimized WHERE IN clauses for efficient bulk deletes.
        /// Always uses MySQL-specific implementation with batched WHERE IN for optimal performance.
        /// </summary>
        /// <typeparam name="T">Entity type to delete.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> managing the connection.</param>
        /// <param name="m">Mapping describing the table and key columns.</param>
        /// <param name="entities">Entities whose keys determine the rows to remove.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var keyCols = m.KeyColumns.ToList();
            if (keyCols.Count == 0)
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var operationKey = $"MySql_BulkDelete_{m.Type.Name}";
            var hasTenant = ctx.Options.TenantProvider != null && m.TenantColumn != null;
            var paramsPerEntity = keyCols.Count + (hasTenant ? 1 : 0);
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);
            var maxBatchForProvider = MaxParameters / Math.Max(1, paramsPerEntity);
            var batchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));

            var totalDeleted = 0;
            for (int i = 0; i < entityList.Count; i += batchSize)
            {
                var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                await using var cmd = ctx.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                var valueClauses = new List<string>();
                var paramIndex = 0;
                foreach (var entity in batch)
                {
                    var paramNames = new List<string>();
                    foreach (var col in keyCols)
                    {
                        var pName = $"{ParamPrefix}p{paramIndex++}";
                        cmd.AddParam(pName, col.Getter(entity));
                        paramNames.Add(pName);
                    }
                    if (keyCols.Count == 1)
                        valueClauses.Add(paramNames[0]);
                    else
                        valueClauses.Add($"({string.Join(", ", paramNames)})");
                }

                string whereClause;
                if (keyCols.Count == 1)
                {
                    whereClause = $"{keyCols[0].EscCol} IN ({string.Join(", ", valueClauses)})";
                }
                else
                {
                    var cols = string.Join(", ", keyCols.Select(c => c.EscCol));
                    whereClause = $"({cols}) IN ({string.Join(", ", valueClauses)})";
                }

                // X1: Add tenant predicate to prevent cross-tenant bulk deletes
                if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                {
                    var tenantParam = $"{ParamPrefix}__tenant_bulk";
                    cmd.AddParam(tenantParam, ctx.Options.TenantProvider.GetCurrentTenantId());
                    whereClause += $" AND {m.TenantColumn.EscCol} = {tenantParam}";
                }
                cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {whereClause}";
                var batchSw = Stopwatch.StartNew();
                totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                batchSw.Stop();
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }

        /// <summary>
        /// Maps a CLR type to its corresponding MySQL column type.
        /// </summary>
        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(string)) return "TEXT";
            if (t == typeof(DateTime)) return "DATETIME";
            if (t == typeof(bool)) return "BIT";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(Guid)) return "CHAR(36)";
            if (t == typeof(byte[])) return "BLOB";
            return "TEXT";
        }

        /// <summary>
        /// Returns a cloned DataTable with the schema matching the specified columns.
        /// Caches the schema per entity type to avoid repeated reflection.
        /// </summary>
        private static DataTable GetDataTable(TableMapping m, List<Column> cols)
        {
            var schema = _tableSchemas.GetOrAdd(m.Type, _ =>
            {
                var dt = new DataTable();
                foreach (var c in cols)
                {
                    var propType = c.Prop.PropertyType;
                    dt.Columns.Add(c.PropName, Nullable.GetUnderlyingType(propType) ?? propType);
                }
                return dt;
            });
            return schema.Clone();
        }

        private static void LoadBulkInsertRows<T>(DataTable table, List<Column> cols, List<T> batch) where T : class
        {
            var values = new object[cols.Count];
            table.BeginLoadData();
            try
            {
                for (int i = 0; i < batch.Count; i++)
                {
                    var entity = batch[i];
                    for (int j = 0; j < cols.Count; j++)
                        values[j] = cols[j].Getter(entity) ?? DBNull.Value;

                    table.Rows.Add(values);
                }
            }
            finally
            {
                table.EndLoadData();
            }
        }
    }
}
