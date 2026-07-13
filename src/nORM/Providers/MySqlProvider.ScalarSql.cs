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
    public sealed partial class MySqlProvider
    {
        /// <summary>
        /// Returns a SQL fragment that retrieves the last auto-incremented identity value.
        /// MySQL <c>LAST_INSERT_ID()</c> only supports numeric <c>AUTO_INCREMENT</c> columns.
        /// Non-numeric generated keys (Guid, string, etc.) must be assigned by the application or a trigger
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
            var accessor = _lastInsertedIdAccessors.GetOrAdd(command.GetType(), static commandType => BuildLastInsertedIdAccessor(commandType));

            return accessor(command);
        }

        // The MySQL command type comes from an optional driver assembly loaded by name at
        // runtime, so the trimmer cannot see the LastInsertedId property. The probe is
        // best-effort: when the property is unavailable the accessor returns null and key
        // retrieval falls back to the appended `SELECT LAST_INSERT_ID()` statement.
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2070",
            Justification = "Best-effort probe of the optional MySQL driver's LastInsertedId property; a missing property falls back to SQL-based generated-key retrieval.")]
        private static Func<DbCommand, object?> BuildLastInsertedIdAccessor(Type commandType)
        {
            var property = commandType.GetProperty("LastInsertedId");
            if (property == null)
                return static _ => null;

            var commandParameter = Expression.Parameter(typeof(DbCommand), "command");
            var typedCommand = Expression.Convert(commandParameter, commandType);
            var propertyAccess = Expression.Property(typedCommand, property);
            var boxed = Expression.Convert(propertyAccess, typeof(object));
            return Expression.Lambda<Func<DbCommand, object?>>(boxed, commandParameter).Compile();
        }

        private static bool IsNumericType(Type t) =>
            t == typeof(int)   || t == typeof(long)   || t == typeof(short)  ||
            t == typeof(uint)  || t == typeof(ulong)  || t == typeof(ushort) ||
            t == typeof(byte)  || t == typeof(sbyte)  || t == typeof(decimal);

        /// <summary>MySQL CAST(... AS CHAR) is the cross-version-safe text conversion target.</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS CHAR)";

        /// <summary>
        /// MySQL treats <c>\</c> as a string-literal escape so <c>ESCAPE '\'</c> generates a syntax error.
        /// Use <c>!</c> - it has no special meaning in MySQL string literals and is safe as a LIKE escape character.
        /// </summary>
        public override char LikeEscapeChar => '!';

        /// <summary>
        /// MySQL's <c>/</c> always yields a DECIMAL (5/2 = 2.5) unlike C#'s truncating
        /// <c>int / int</c>; <c>DIV</c> discards the fraction (truncating toward zero, matching
        /// C# for negative operands) so integer-typed division translates faithfully.
        /// </summary>
        internal override string IntegerDivisionOperator => "DIV";

        /// <summary>
        /// MySQL's default collations (utf8mb4_0900_ai_ci and friends) make <c>=</c>
        /// case-insensitive, unlike C# ordinal string equality.
        /// </summary>
        internal override bool DefaultStringEqualityIsCaseInsensitive => true;

        /// <summary>
        /// Value-preserving ordinal wrap for set-operation projections. A bare <c>BINARY x</c>
        /// materializes as VARBINARY (raw bytes); casting to utf8mb4 text with the binary
        /// collation keeps the value a string while UNION / INTERSECT / EXCEPT compare
        /// byte-wise (live-verified: dedup keeps 'abc' and 'ABC' distinct, values unchanged).
        /// </summary>
        internal override string OrdinalComparableStringProjection(string sql)
            => $"CAST({sql} AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_bin";

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
        /// MySQL stores TimeSpan as TIME. TIME_TO_SEC returns integer seconds;
        /// the MICROSECOND() function adds sub-second precision back in.
        /// </summary>
        public override string GetTimeSpanColumnSecondsSql(string timeSpanColumnSql)
            => $"(TIME_TO_SEC({timeSpanColumnSql}) + MICROSECOND({timeSpanColumnSql}) / 1000000.0)";

        /// <summary>
        /// MySQL has no native DATETIMEOFFSET - MySqlConnector stores DateTimeOffset
        /// as DATETIME normalised to UTC. Adding the new offset's seconds shifts the
        /// rendered wall clock; the trailing offset suffix completes the canonical
        /// text the materialiser parses.
        /// </summary>
        public override string GetDateTimeOffsetWithOffsetSql(string dtoSql, TimeSpan offset)
        {
            var suffix = FormatOffsetSuffix(offset);
            var totalSec = (long)offset.TotalSeconds;
            var fmtArg = totalSec == 0
                ? dtoSql
                : $"DATE_ADD({dtoSql}, INTERVAL {totalSec} SECOND)";
            return $"CONCAT(DATE_FORMAT({fmtArg}, '%Y-%m-%dT%H:%i:%s'), '{suffix}')";
        }

        /// <inheritdoc/>
        public override string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
        {
            var totalSec = (long)localOffset.TotalSeconds;
            return totalSec == 0
                ? $"CAST({dtoSql} AS DATETIME)"
                : $"DATE_ADD({dtoSql}, INTERVAL {totalSec} SECOND)";
        }

        /// <inheritdoc/>
        public override string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
            // MySQL stores nORM DateTimeOffset columns as DATETIME(6) pre-normalised
            // to UTC. UNIX_TIMESTAMP() interprets its argument in the session timezone,
            // so it gives wrong results when the session TZ is not UTC.
            // TIMESTAMPDIFF(SECOND, epoch, col) does pure arithmetic on the stored
            // value without any TZ conversion - correct because the value IS UTC.
            => $"TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', {dtoSql})";

        internal override string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"FLOOR(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', {dtoSql}) / 1000)";

        internal override string GetDateTimeOffsetUtcEpochMicrosecondsSql(string dtoSql)
            => $"TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', {dtoSql})";

        internal override string GetDateTimeOffsetDifferenceSecondsSql(string leftSql, string rightSql)
            => $"(CAST(({GetDateTimeOffsetUtcEpochMicrosecondsSql(leftSql)} - {GetDateTimeOffsetUtcEpochMicrosecondsSql(rightSql)}) AS DOUBLE) / 1000000.0)";

        /// <summary>
        /// MySQL DATE_ADD returns DATETIME; wrap with DATE() to cast back to a
        /// DATE so the materializer reads a DateOnly-compatible value.
        /// </summary>
        public override string? AddDaysToDateOnlySql(string dateOnlySql, string daysSqlFragment)
            => $"DATE(DATE_ADD({dateOnlySql}, INTERVAL ({daysSqlFragment}) DAY))";

        /// <summary>MySQL uses GROUP_CONCAT(expr SEPARATOR sep); no separator arg defaults to ','.</summary>
        public override string GetStringAggregateSql(string expr, string sepLiteral)
            => $"GROUP_CONCAT({expr} SEPARATOR {sepLiteral})";

        /// <summary>MySQL ordered GROUP_CONCAT: ORDER BY comes before SEPARATOR.</summary>
        public override string GetStringAggregateSql(string expr, string sepLiteral, string orderBySql)
            => $"GROUP_CONCAT({expr} ORDER BY {orderBySql} SEPARATOR {sepLiteral})";

        /// <summary>
        /// MySQL has no BOOLEAN as a CAST target; use a comparison expression that
        /// returns 1 (true) for any nonzero input and 0 (false) for zero.
        /// </summary>
        public override string GetBoolCastSql(string innerSql)
            => $"(({innerSql}) <> 0)";

        /// <summary>
        /// MySQL REGEXP/RLIKE obeys column collation and can become case-insensitive.
        /// REGEXP_LIKE with explicit match_type preserves .NET Regex.IsMatch default
        /// case-sensitive semantics across collations.
        /// </summary>
        public override string GetRegexMatchSql(string inputSql, string patternLiteral)
            => $"REGEXP_LIKE({inputSql}, {patternLiteral}, 'c')";

        /// <summary>MySQL REGEXP_LIKE with match_type i preserves RegexOptions.IgnoreCase.</summary>
        public override string GetRegexMatchIgnoreCaseSql(string inputSql, string patternLiteral)
            => $"REGEXP_LIKE({inputSql}, {patternLiteral}, 'i')";

        /// <summary>MySQL 8.0+ has REGEXP_REPLACE; match_type c preserves case-sensitive .NET defaults.</summary>
        public override string GetRegexReplaceSql(string inputSql, string patternLiteral, string replacementLiteral)
            => $"REGEXP_REPLACE({inputSql}, {patternLiteral}, {replacementLiteral}, 1, 0, 'c')";

        /// <summary>MySQL REGEXP_REPLACE accepts match_type as the sixth argument.</summary>
        public override string GetRegexReplaceIgnoreCaseSql(string inputSql, string patternLiteral, string replacementLiteral)
            => $"REGEXP_REPLACE({inputSql}, {patternLiteral}, {replacementLiteral}, 1, 0, 'i')";

        /// <summary>MySQL DATE_ADD returns DATETIME; DATE() casts back to DATE for the materializer.</summary>
        public override string? AddMonthsToDateOnlySql(string dateOnlySql, string monthsSqlFragment)
            => $"DATE(DATE_ADD({dateOnlySql}, INTERVAL ({monthsSqlFragment}) MONTH))";

        /// <summary>MySQL DATE_ADD returns DATETIME; DATE() casts back to DATE for the materializer.</summary>
        public override string? AddYearsToDateOnlySql(string dateOnlySql, string yearsSqlFragment)
            => $"DATE(DATE_ADD({dateOnlySql}, INTERVAL ({yearsSqlFragment}) YEAR))";

        /// <summary>
        /// .NET TimeOnly arithmetic wraps around midnight, but MySQL's ADDTIME does
        /// not — TIME values happily exceed 24 hours ('23:30' + 2.5h = '26:00:00').
        /// Convert to seconds, apply a positive modulo (MOD keeps the dividend's
        /// sign, so the double-MOD folds negative results back into [0, 86400)),
        /// and convert back to TIME.
        /// </summary>
        public override string? AddSecondsToTimeOnlySql(string timeOnlySql, string secondsSqlFragment)
            => $"SEC_TO_TIME(MOD(MOD(TIME_TO_SEC({timeOnlySql}) + ({secondsSqlFragment}), 86400) + 86400, 86400))";

        /// <summary>Same midnight wrap as <see cref="AddSecondsToTimeOnlySql"/> for a TimeSpan column operand.</summary>
        public override string? AddTimeSpanColumnToTimeOnlySql(string timeOnlySql, string timeSpanColumnSql, bool subtract)
        {
            var op = subtract ? "-" : "+";
            return $"SEC_TO_TIME(MOD(MOD(TIME_TO_SEC({timeOnlySql}) {op} TIME_TO_SEC({timeSpanColumnSql}), 86400) + 86400, 86400))";
        }

        /// <summary>MySQL uses SIGNED / UNSIGNED for integer casts - `CAST(x AS INT)` is a syntax error.</summary>
        public override string GetIntCastSql(string innerSql, bool asLong = false)
            => $"CAST({innerSql} AS SIGNED)";

        /// <summary>MySQL's integral CAST target keyword (CAST(x AS INT) is a syntax error).</summary>
        internal override string IntegerCastTypeName => "SIGNED";

        /// <summary>MySQL TIME to fractional seconds: whole seconds plus the microsecond tail.</summary>
        internal override string TimeSpanOperandToSecondsSql(string sql)
            => $"(TIME_TO_SEC({sql}) + MICROSECOND({sql}) / 1000000.0)";

        /// <summary>MySQL requires SIGNED as the integer cast target and TRUNCATE for toward-zero semantics.</summary>
        public override string GetTruncateToIntSql(string numericSql)
            => $"CAST(TRUNCATE({numericSql}, 0) AS SIGNED)";

        /// <summary>MySQL supports INSERT IGNORE for idempotent join-table inserts.</summary>
        public override string GetInsertOrIgnoreSql(string escTable, string escC1, string escC2, string p1, string p2)
            => $"INSERT IGNORE INTO {escTable} ({escC1}, {escC2}) VALUES ({p1}, {p2})";

        /// <summary>
        /// Creates a new <see cref="DbParameter"/> for use with MySQL commands.
        /// </summary>
        public override DbParameter CreateParameter(string name, object? value) =>
            _parameterFactory.CreateParameter(name, value);

        /// <summary>
        /// MySQL TIMESTAMPDIFF(MICROSECOND) preserves sub-second deltas while
        /// avoiding TIMESTAMPDIFF(SECOND)'s integer truncation. Divide by
        /// 1,000,000.0 so Total* projections and TimeSpan materialisation receive
        /// fractional seconds.
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"(CAST(TIMESTAMPDIFF(MICROSECOND, {startSql}, {endSql}) AS DOUBLE) / 1000000.0)";

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

        /// <summary>
        /// 7-arg STR_TO_DATE with 'yyyy-MM-dd HH:mm:ss.fff' shape; %f in MySQL
        /// is microseconds (0..999999); LPAD ms to 3 digits then append '000' so
        /// MySQL parses the fractional tail as microseconds and rounds correctly.
        /// </summary>
        public override string GetDateTimeFromPartsSql(string yearSql, string monthSql, string daySql, string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"STR_TO_DATE(CONCAT(" +
               $"CONCAT_WS('-', {yearSql}, LPAD({monthSql}, 2, '0'), LPAD({daySql}, 2, '0')), " +
               $"' ', " +
               $"CONCAT_WS(':', LPAD({hourSql}, 2, '0'), LPAD({minuteSql}, 2, '0'), LPAD({secondSql}, 2, '0')), " +
               $"'.', LPAD({millisecondSql}, 3, '0'), '000'), " +
               $"'%Y-%m-%d %H:%i:%s.%f')";

        /// <summary>MySQL DATE() wrapper around the STR_TO_DATE text shape ensures DATE return type.</summary>
        public override string GetDateOnlyFromPartsSql(string yearSql, string monthSql, string daySql)
            => $"DATE(STR_TO_DATE(CONCAT_WS('-', {yearSql}, LPAD({monthSql}, 2, '0'), LPAD({daySql}, 2, '0')), '%Y-%m-%d'))";

        /// <summary>MySQL MAKETIME builds a TIME from int parts; seconds accept double for sub-second.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql)
            => $"MAKETIME({hourSql}, {minuteSql}, {secondSql})";

        /// <summary>4-arg variant: MAKETIME accepts double seconds.</summary>
        public override string GetTimeOnlyFromPartsSql(string hourSql, string minuteSql, string secondSql, string millisecondSql)
            => $"MAKETIME({hourSql}, {minuteSql}, ({secondSql}) + ({millisecondSql}) / 1000.0)";

        /// <summary>
        /// MySQL stores TimeOnly as TIME. TIMEDIFF returns a signed TIME diff;
        /// TIME_TO_SEC produces the integer-seconds form which can be negative.
        /// Wrap +86400 MOD 86400 to match TimeOnly's [0, 24h) semantics.
        /// </summary>
        public override string GetTimeOnlyDifferenceSecondsSql(string endSql, string startSql)
            => $"CAST(((TIME_TO_SEC(TIMEDIFF({endSql}, {startSql})) + 86400) MOD 86400) AS DOUBLE)";
    }
}
