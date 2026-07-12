using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// .NET DateTimeOffset equality is instant-based: values with different offsets but the same UTC
/// instant are EQUAL, so LINQ GroupBy puts their rows in ONE group. SQLite stores DateTimeOffset
/// as offset-suffixed TEXT, so a naive GROUP BY on the raw text splits same-instant rows into
/// separate groups — silently wrong aggregates. The key is canonicalized to UTC text so grouping
/// matches .NET while the selected key still materializes as a DateTimeOffset.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DateTimeOffsetGroupKeyTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DtoG")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public DateTimeOffset WhenAt { get; set; }
        public int Amount { get; set; }
    }

    [Fact]
    public void GroupBy_mixed_offset_same_instant_groups_together()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Rows 1+2 are the SAME instant (03:00 UTC) written at different offsets;
            // row 3 is a different instant.
            cmd.CommandText = """
                CREATE TABLE DtoG (Id INTEGER PRIMARY KEY, WhenAt TEXT NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO DtoG VALUES
                    (1, '2020-01-01 05:00:00+02:00', 1),
                    (2, '2020-01-01 03:00:00+00:00', 2),
                    (3, '2020-01-01 04:00:00+00:00', 5);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var groups = ctx.Query<Row>()
            .GroupBy(r => r.WhenAt)
            .Select(g => new { g.Key, Total = g.Sum(x => x.Amount) })
            .ToList().OrderBy(x => x.Key).ToList();

        // LINQ semantics: instants 03:00Z (rows 1+2 → total 3) and 04:00Z (row 3 → total 5).
        Assert.Equal(2, groups.Count);
        Assert.Equal(3, groups[0].Total);
        Assert.Equal(new DateTimeOffset(2020, 1, 1, 3, 0, 0, TimeSpan.Zero).UtcDateTime, groups[0].Key.UtcDateTime);
        Assert.Equal(5, groups[1].Total);
        Assert.Equal(new DateTimeOffset(2020, 1, 1, 4, 0, 0, TimeSpan.Zero).UtcDateTime, groups[1].Key.UtcDateTime);
    }
}
