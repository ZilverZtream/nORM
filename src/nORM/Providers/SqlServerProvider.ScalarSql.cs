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
    public sealed partial class SqlServerProvider
    {
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
        /// When the mapping also declares a timestamp (ROWVERSION) column, that column is included as
        /// a second OUTPUT column so the server-assigned value is read back after INSERT and kept in sync
        /// with the in-memory entity — preventing a false concurrency conflict on the first UPDATE.
        /// </summary>
        /// <param name="m">The table mapping for the insert.</param>
        /// <returns>SQL clause e.g. <c> OUTPUT INSERTED.[Id]</c> or <c> OUTPUT INSERTED.[Id], INSERTED.[RowVersion]</c>.</returns>
        public override string GetIdentityRetrievalPrefix(TableMapping m)
        {
            // Target a DB-generated key, or — for the store-generated convention key's default-value run —
            // the convention key column, so OUTPUT INSERTED reads the generated value back.
            var keyCol = m.KeyColumns.FirstOrDefault(c => c.IsDbGenerated) ?? m.ConventionGeneratedKeyColumn;
            if (keyCol == null) return string.Empty;
            var tsCol = m.TimestampColumn;
            return tsCol != null
                ? $" OUTPUT INSERTED.{keyCol.EscCol}, INSERTED.{tsCol.EscCol}"
                : $" OUTPUT INSERTED.{keyCol.EscCol}";
        }

        /// <summary>SQL Server uses NVARCHAR(MAX) for unbounded textual conversion.</summary>
        public override string GetToStringSql(string innerSql) => $"CAST({innerSql} AS NVARCHAR(MAX))";

        /// <summary>SQL Server uses BIT (0/1) as its boolean type; BOOLEAN is not valid SQL Server syntax.</summary>
        public override string GetBoolCastSql(string innerSql) => $"CAST({innerSql} AS BIT)";

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

        /// <summary>
        /// SQL Server has a native <c>SWITCHOFFSET(datetimeoffset, '+HH:MM')</c>
        /// function that returns DATETIMEOFFSET at the requested offset for the
        /// same UTC instant. SqlClient reads it directly as DateTimeOffset.
        /// </summary>
        public override string GetDateTimeOffsetWithOffsetSql(string dtoSql, TimeSpan offset)
            => $"SWITCHOFFSET({dtoSql}, '{FormatOffsetSuffix(offset)}')";

        /// <inheritdoc/>
        public override string GetDateTimeOffsetLocalDateTimeSql(string dtoSql, TimeSpan localOffset)
            => $"CAST(SWITCHOFFSET({dtoSql}, '{FormatOffsetSuffix(localOffset)}') AS DATETIME2)";

        /// <inheritdoc/>
        public override string GetDateTimeOffsetUtcEpochSecondsSql(string dtoSql)
            => $"DATEDIFF_BIG(SECOND, CAST('1970-01-01 00:00:00 +00:00' AS DATETIMEOFFSET), {dtoSql})";

        internal override string GetDateTimeOffsetUtcEpochMillisecondsSql(string dtoSql)
            => $"DATEDIFF_BIG(MILLISECOND, CAST('1970-01-01 00:00:00 +00:00' AS DATETIMEOFFSET), {dtoSql})";

        internal override string GetDateTimeOffsetUtcEpochMicrosecondsSql(string dtoSql)
            => $"DATEDIFF_BIG(MICROSECOND, CAST('1970-01-01 00:00:00 +00:00' AS DATETIMEOFFSET), {dtoSql})";

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

        /// <summary>SQL Server ROUND(x, 0, 1) truncates toward zero.</summary>
        public override string GetTruncateToIntSql(string numericSql)
            => $"CAST(ROUND({numericSql}, 0, 1) AS INT)";

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
        /// SQL Server's usual default database collation (SQL_Latin1_General_CP1_CI_AS) makes
        /// <c>=</c> case-insensitive, unlike C# ordinal string equality.
        /// </summary>
        internal override bool DefaultStringEqualityIsCaseInsensitive => true;

        /// <summary>
        /// Value-preserving ordinal wrap for set-operation projections: COLLATE changes only the
        /// comparison semantics, not the value, so UNION / INTERSECT / EXCEPT dedup byte-wise
        /// while the column still materializes as a string.
        /// </summary>
        internal override string OrdinalComparableStringProjection(string sql)
            => $"{sql} COLLATE Latin1_General_100_BIN2";

        /// <summary>
        /// T-SQL AVG over an integer operand performs integer division (AVG of 1,2 = 1), but C#
        /// Average over ints returns a double (1.5). Cast integral operands to FLOAT so the
        /// aggregate matches C# semantics; non-integral operands (decimal, float) already do.
        /// </summary>
        internal override string AverageAggregateOperand(string sql, Type operandClrType)
            => IsIntegralArithmeticType(operandClrType) ? $"CAST({sql} AS FLOAT)" : sql;

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
        /// DATEDIFF_BIG(MICROSECOND) preserves sub-second deltas while avoiding
        /// DATEDIFF(SECOND)'s integer truncation. Divide by 1,000,000.0 so Total*
        /// projections and TimeSpan materialisation receive fractional seconds.
        /// </summary>
        /// <param name="endSql">SQL fragment evaluating the later timestamp.</param>
        /// <param name="startSql">SQL fragment evaluating the earlier timestamp.</param>
        public override string GetDateTimeDifferenceSecondsSql(string endSql, string startSql)
            => $"(CAST(DATEDIFF_BIG(MICROSECOND, {startSql}, {endSql}) AS FLOAT) / 1000000.0)";

        /// <summary>SQL Server TIME to fractional seconds, microsecond precision from midnight.</summary>
        internal override string TimeSpanOperandToSecondsSql(string sql)
            => $"(CAST(DATEDIFF_BIG(MICROSECOND, CAST('00:00:00' AS TIME), {sql}) AS FLOAT) / 1000000.0)";

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
    }
}
