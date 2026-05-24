using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Exercises (end - start).TimeSpanMember on real rows. Each test pins a different unit
/// (Total* fractional or Days/Hours/Minutes/Seconds integer-component) so a regression in
/// the provider's GetDateTimeDifferenceSecondsSql or the visitor's unit-conversion table
/// shows up at the first failing assertion.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeArithmeticTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DaRow (Id INTEGER PRIMARY KEY, Start TEXT NOT NULL, End TEXT NOT NULL);
            INSERT INTO DaRow VALUES
                (1, '2024-01-01 00:00:00', '2024-01-01 00:00:30'),   -- 30s
                (2, '2024-01-01 00:00:00', '2024-01-01 00:05:00'),   --  5m
                (3, '2024-01-01 00:00:00', '2024-01-01 06:00:00'),   --  6h
                (4, '2024-01-01 00:00:00', '2024-01-03 00:00:00');   --  2d
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task DateTime_diff_TotalHours_in_where_filters_long_intervals()
    {
        // > 1 hour: the 6h and 2d rows.
        var ids = (await _ctx.Query<DaRow>()
            .Where(r => (r.End - r.Start).TotalHours > 1)
            .OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 4 }, ids);
    }

    [Fact]
    public async Task DateTime_diff_TotalDays_in_where_filters_multi_day_intervals()
    {
        var ids = (await _ctx.Query<DaRow>()
            .Where(r => (r.End - r.Start).TotalDays >= 1)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4 }, ids);
    }

    [Fact]
    public async Task DateTime_diff_TotalSeconds_in_where_filters_short_intervals()
    {
        var ids = (await _ctx.Query<DaRow>()
            .Where(r => (r.End - r.Start).TotalSeconds < 60)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task DateTime_diff_Days_integer_component_matches_TimeSpan_semantics()
    {
        // Days truncates toward zero: 30s → 0, 5m → 0, 6h → 0, 2d → 2.
        var ids = (await _ctx.Query<DaRow>()
            .Where(r => (r.End - r.Start).Days == 2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4 }, ids);
    }

    [Table("DaRow")]
    public sealed class DaRow
    {
        [Key] public int Id { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }
}
