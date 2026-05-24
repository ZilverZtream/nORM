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
/// Pins date arithmetic inside a WHERE predicate:
/// <c>Where(p =&gt; p.CreatedAt.AddDays(n) &lt; anchor)</c>. The left side
/// lowers to <c>datetime(col, '+N days')</c> (SQLite text), the right side
/// binds as a DateTime parameter. Silent-wrongness risk if the parameter
/// formatter and the strftime output disagree (e.g. "2024-01-15T00:00:00"
/// vs "2024-01-15 00:00:00") — string comparison would fail asymmetrically,
/// returning the wrong subset of rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeArithmeticTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdaRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT NOT NULL);
            INSERT INTO WdaRow VALUES
              (1, '2024-01-01 00:00:00'),
              (2, '2024-01-05 12:00:00'),
              (3, '2024-01-10 00:00:00'),
              (4, '2024-01-15 00:00:00'),
              (5, '2024-01-20 18:30:00');
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
    public async Task Where_adddays_less_than_anchor_returns_rows_whose_shifted_date_is_earlier()
    {
        // Row.CreatedAt + 7 days < 2024-01-15:
        //   Id 1: 2024-01-01 + 7 = 2024-01-08 < 2024-01-15 ✓
        //   Id 2: 2024-01-05 12:00 + 7 = 2024-01-12 12:00 < 2024-01-15 ✓
        //   Id 3: 2024-01-10 + 7 = 2024-01-17 < 2024-01-15 ✗
        //   Id 4: 2024-01-15 + 7 = 2024-01-22 ✗
        //   Id 5: 2024-01-20 + 7 = 2024-01-27 ✗
        var anchor = new DateTime(2024, 1, 15);
        var ids = (await _ctx.Query<WdaRow>()
            .Where(p => p.CreatedAt.AddDays(7) < anchor)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public async Task Where_addmonths_greater_than_anchor_returns_rows_whose_shifted_date_is_later()
    {
        // Row.CreatedAt + 2 months > 2024-03-01:
        //   Id 1: 2024-01-01 + 2mo = 2024-03-01     == not >
        //   Id 2: 2024-01-05 + 2mo = 2024-03-05     >  2024-03-01 ✓
        //   Id 3: 2024-01-10 + 2mo = 2024-03-10     ✓
        //   Id 4: 2024-01-15 + 2mo = 2024-03-15     ✓
        //   Id 5: 2024-01-20 + 2mo = 2024-03-20     ✓
        var anchor = new DateTime(2024, 3, 1);
        var ids = (await _ctx.Query<WdaRow>()
            .Where(p => p.CreatedAt.AddMonths(2) > anchor)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 3, 4, 5 }, ids);
    }

    [Fact]
    public async Task Where_adddays_with_closure_captured_delta_uses_runtime_value()
    {
        // Verifies closure-captured int reaches the SQL: delta is bound as a
        // compiled parameter; if it were baked at translate-time the second call
        // (delta = -3) would return the same rows as the first (delta = +7).
        int delta = 7;
        var anchor = new DateTime(2024, 1, 15);
        var ids1 = (await _ctx.Query<WdaRow>()
            .Where(p => p.CreatedAt.AddDays(delta) < anchor)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids1);

        delta = -3;
        // Now CreatedAt - 3 days < 2024-01-15 — every row qualifies because the
        // latest row (2024-01-20) minus 3 = 2024-01-17 > 2024-01-15 ✗ — actually
        // Id 5: 2024-01-20 - 3 = 2024-01-17 > 2024-01-15 ✗
        // Id 4: 2024-01-15 - 3 = 2024-01-12 < 2024-01-15 ✓
        // Id 3: 2024-01-10 - 3 = 2024-01-07 ✓
        // Id 2: 2024-01-05 12:00 - 3 = 2024-01-02 12:00 ✓
        // Id 1: 2024-01-01 - 3 = 2023-12-29 ✓
        var ids2 = (await _ctx.Query<WdaRow>()
            .Where(p => p.CreatedAt.AddDays(delta) < anchor)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 3, 4 }, ids2);
    }

    [Table("WdaRow")]
    public sealed class WdaRow
    {
        [Key] public int Id { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
