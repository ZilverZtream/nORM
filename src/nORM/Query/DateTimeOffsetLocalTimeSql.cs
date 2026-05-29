using System;
using System.Collections.Generic;
using nORM.Providers;

namespace nORM.Query;

internal static class DateTimeOffsetLocalTimeSql
{
    private static readonly Lazy<IReadOnlyList<OffsetRange>> s_localOffsetRanges =
        new(BuildLocalOffsetRanges, isThreadSafe: true);

    public static string Build(DatabaseProvider provider, string dtoSql)
    {
        var ranges = s_localOffsetRanges.Value;
        if (ranges.Count <= 1)
        {
            var offset = ranges.Count == 1
                ? ranges[0].Offset
                : TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
            return provider.GetDateTimeOffsetLocalDateTimeSql(dtoSql, offset);
        }

        var epochMsSql = provider.GetDateTimeOffsetUtcEpochMillisecondsSql(dtoSql);
        var sql = new System.Text.StringBuilder();
        sql.Append("(CASE");
        foreach (var range in ranges)
        {
            sql.Append(" WHEN ").Append(epochMsSql)
                .Append(" >= ").Append(range.StartUnixMs)
                .Append(" AND ").Append(epochMsSql)
                .Append(" < ").Append(range.EndUnixMs)
                .Append(" THEN ").Append(provider.GetDateTimeOffsetLocalDateTimeSql(dtoSql, range.Offset));
        }

        var fallbackOffset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
        sql.Append(" ELSE ")
            .Append(provider.GetDateTimeOffsetLocalDateTimeSql(dtoSql, fallbackOffset))
            .Append(" END)");
        return sql.ToString();
    }

    private static IReadOnlyList<OffsetRange> BuildLocalOffsetRanges()
    {
        var zone = TimeZoneInfo.Local;
        var start = new DateTimeOffset(1900, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var end = new DateTimeOffset(2101, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ranges = new List<OffsetRange>();
        var rangeStart = start;
        var offset = zone.GetUtcOffset(start.UtcDateTime);

        for (var cursor = start.AddDays(1); cursor <= end; cursor = cursor.AddDays(1))
        {
            var nextOffset = zone.GetUtcOffset(cursor.UtcDateTime);
            if (nextOffset == offset)
                continue;

            var transition = FindTransition(zone, cursor.AddDays(-1), cursor, offset);
            ranges.Add(new OffsetRange(ToUnixMs(rangeStart), ToUnixMs(transition), offset));
            rangeStart = transition;
            offset = nextOffset;
        }

        ranges.Add(new OffsetRange(ToUnixMs(rangeStart), ToUnixMs(end), offset));
        return ranges;
    }

    private static DateTimeOffset FindTransition(
        TimeZoneInfo zone,
        DateTimeOffset low,
        DateTimeOffset high,
        TimeSpan oldOffset)
    {
        while ((high - low) > TimeSpan.FromSeconds(1))
        {
            var midTicks = low.Ticks + ((high.Ticks - low.Ticks) / 2);
            var mid = new DateTimeOffset(midTicks, TimeSpan.Zero);
            if (zone.GetUtcOffset(mid.UtcDateTime) == oldOffset)
                low = mid;
            else
                high = mid;
        }

        return new DateTimeOffset(
            high.Year,
            high.Month,
            high.Day,
            high.Hour,
            high.Minute,
            high.Second,
            TimeSpan.Zero);
    }

    private static long ToUnixMs(DateTimeOffset value) => value.ToUnixTimeMilliseconds();

    private readonly record struct OffsetRange(long StartUnixMs, long EndUnixMs, TimeSpan Offset);
}
