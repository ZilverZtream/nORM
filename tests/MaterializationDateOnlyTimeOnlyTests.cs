using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// MM1 — DateOnly / TimeOnly materialization gap
// ══════════════════════════════════════════════════════════════════════════════

[Table("MM1_DateTimeEntity")]
public class MM1DateTimeEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public DateOnly BirthDate { get; set; }

    public TimeOnly WakeUpTime { get; set; }

    public string Name { get; set; } = string.Empty;
}

[Table("MM1_NullableDateTimeEntity")]
public class MM1NullableDateTimeEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public DateOnly? BirthDate { get; set; }

    public TimeOnly? WakeUpTime { get; set; }

    public string Name { get; set; } = string.Empty;
}

public class MaterializationDateOnlyTimeOnlyTests
{
    private static (SqliteConnection cn, DbContext ctx) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE MM1_DateTimeEntity (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                BirthDate TEXT NOT NULL,
                WakeUpTime TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE TABLE MM1_NullableDateTimeEntity (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                BirthDate TEXT,
                WakeUpTime TEXT,
                Name TEXT NOT NULL
            );";
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions();
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    // ── Test 1: DateOnly round-trip ─────────────────────────────────────────

    [Fact]
    public async Task DateOnly_InsertAndQuery_RoundTrip()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        var expected = new DateOnly(1990, 6, 15);
        await ctx.InsertAsync(new MM1DateTimeEntity
        {
            BirthDate = expected,
            WakeUpTime = new TimeOnly(7, 30),
            Name = "Alice"
        });

        var rows = await ctx.Query<MM1DateTimeEntity>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expected, rows[0].BirthDate);
        Assert.Equal("Alice", rows[0].Name);
    }

    // ── Test 2: TimeOnly round-trip ─────────────────────────────────────────

    [Fact]
    public async Task TimeOnly_InsertAndQuery_RoundTrip()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        var expectedTime = new TimeOnly(14, 30, 45);
        await ctx.InsertAsync(new MM1DateTimeEntity
        {
            BirthDate = new DateOnly(2000, 1, 1),
            WakeUpTime = expectedTime,
            Name = "Bob"
        });

        var rows = await ctx.Query<MM1DateTimeEntity>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expectedTime, rows[0].WakeUpTime);
    }

    // ── Test 3: DateOnly Where filter ───────────────────────────────────────

    [Fact]
    public async Task DateOnly_WhereFilter_ReturnsMatchingRow()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        var target = new DateOnly(1985, 3, 22);
        await ctx.InsertAsync(new MM1DateTimeEntity
        {
            BirthDate = target,
            WakeUpTime = new TimeOnly(6, 0),
            Name = "Match"
        });
        await ctx.InsertAsync(new MM1DateTimeEntity
        {
            BirthDate = new DateOnly(2000, 12, 31),
            WakeUpTime = new TimeOnly(8, 0),
            Name = "NoMatch"
        });

        // Verify both rows inserted
        var all = await ctx.Query<MM1DateTimeEntity>().ToListAsync();
        Assert.Equal(2, all.Count);

        // Verify the target date round-tripped correctly
        var match = all.Where(x => x.BirthDate == target).ToList();
        Assert.Single(match);
        Assert.Equal("Match", match[0].Name);
    }

    // ── Test 4: Nullable DateOnly round-trip ────────────────────────────────

    [Fact]
    public async Task NullableDateOnly_InsertAndQuery_RoundTrip()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        var expected = new DateOnly(1975, 11, 8);

        // Insert one with a value and one with null
        await ctx.InsertAsync(new MM1NullableDateTimeEntity
        {
            BirthDate = expected,
            WakeUpTime = new TimeOnly(9, 0),
            Name = "HasDate"
        });
        await ctx.InsertAsync(new MM1NullableDateTimeEntity
        {
            BirthDate = null,
            WakeUpTime = null,
            Name = "NoDate"
        });

        var rows = (await ctx.Query<MM1NullableDateTimeEntity>().ToListAsync())
            .OrderBy(x => x.Name).ToList();
        Assert.Equal(2, rows.Count);

        // Row with value
        var hasDate = rows.First(r => r.Name == "HasDate");
        Assert.NotNull(hasDate.BirthDate);
        Assert.Equal(expected, hasDate.BirthDate!.Value);

        // Row with null
        var noDate = rows.First(r => r.Name == "NoDate");
        Assert.Null(noDate.BirthDate);
        Assert.Null(noDate.WakeUpTime);
    }

    // ── Test 5: Nullable TimeOnly round-trip ────────────────────────────────

    [Fact]
    public async Task NullableTimeOnly_InsertAndQuery_RoundTrip()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        var expectedTime = new TimeOnly(22, 15, 30);

        await ctx.InsertAsync(new MM1NullableDateTimeEntity
        {
            BirthDate = new DateOnly(2000, 1, 1),
            WakeUpTime = expectedTime,
            Name = "HasTime"
        });
        await ctx.InsertAsync(new MM1NullableDateTimeEntity
        {
            BirthDate = null,
            WakeUpTime = null,
            Name = "NoTime"
        });

        var rows = (await ctx.Query<MM1NullableDateTimeEntity>().ToListAsync())
            .OrderBy(x => x.Name).ToList();
        Assert.Equal(2, rows.Count);

        var hasTime = rows.First(r => r.Name == "HasTime");
        Assert.NotNull(hasTime.WakeUpTime);
        Assert.Equal(expectedTime, hasTime.WakeUpTime!.Value);

        var noTime = rows.First(r => r.Name == "NoTime");
        Assert.Null(noTime.WakeUpTime);
    }

    // ── Test 6: Multiple DateOnly/TimeOnly entities ─────────────────────────

    [Fact]
    public async Task MultipleEntities_DateOnlyTimeOnly_AllRoundTrip()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        for (int i = 0; i < 5; i++)
        {
            await ctx.InsertAsync(new MM1DateTimeEntity
            {
                BirthDate = new DateOnly(2000 + i, i + 1, i + 10),
                WakeUpTime = new TimeOnly(i + 5, i * 10, i),
                Name = $"Entity{i}"
            });
        }

        var rows = (await ctx.Query<MM1DateTimeEntity>().ToListAsync())
            .OrderBy(x => x.Name).ToList();
        Assert.Equal(5, rows.Count);

        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(new DateOnly(2000 + i, i + 1, i + 10), rows[i].BirthDate);
            Assert.Equal(new TimeOnly(i + 5, i * 10, i), rows[i].WakeUpTime);
        }
    }
}
