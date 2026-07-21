using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probes <c>Take(int.MaxValue)</c> and the boundary values nearby
/// (<c>int.MaxValue - 1</c>, <c>Skip(int.MaxValue)</c>). This is a
/// real bug-risk shape because:
///   * SQLite accepts a 64-bit INTEGER for LIMIT/OFFSET, so 2_147_483_647
///     fits comfortably -- but if nORM ever emits LIMIT as a 32-bit
///     parameter cast (e.g. wraps DbType.Int32 explicitly) and the
///     provider tries to bind a negative cast, the result is silent
///     truncation or "near LIMIT: syntax error".
///   * <c>Skip(int.MaxValue) + Take(1)</c> stacks two max-int paging
///     values; some providers reject if combined > 2^31, even if each
///     fits individually.
///   * Some code paths cap N to 1000 or similar -- a silent cap would
///     produce wrong-row-count without crashing.
///
/// Pins the SQL emit + result contract for max-int paging.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTakeMaxValueTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TmvItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO TmvItem VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<TmvItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Take_int_maxvalue_returns_all_rows()
    {
        // 5-row source; Take(int.MaxValue) should return all 5.
        // Silent-wrongness: capped or overflowed N would return < 5 or 0.
        // Hard-fail: provider rejects LIMIT 2147483647 with a syntax error.
        var rows = await _ctx.Query<TmvItem>().OrderBy(i => i.Id).Take(int.MaxValue).ToListAsync();
        Assert.Equal(5, rows.Count);
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Take_int_maxvalue_minus_one_returns_all_rows()
    {
        // Boundary just below int.MaxValue -- also exercises the limit.
        var rows = await _ctx.Query<TmvItem>().OrderBy(i => i.Id).Take(int.MaxValue - 1).ToListAsync();
        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task Skip_int_maxvalue_returns_empty_set()
    {
        // OFFSET 2147483647 on a 5-row source yields zero rows. Silent-
        // wrongness probe: a wrapped/truncated value could yield a wrong
        // page; an unsupported value could throw a cryptic error.
        var rows = await _ctx.Query<TmvItem>().OrderBy(i => i.Id).Skip(int.MaxValue).ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Skip_int_maxvalue_then_take_one_returns_empty()
    {
        // Stacked max-int paging. SQLite tolerates LIMIT 1 OFFSET 2147483647
        // and just returns nothing; the test pins that no provider intermediate
        // overflows the offset+limit sum.
        var rows = await _ctx.Query<TmvItem>().OrderBy(i => i.Id).Skip(int.MaxValue).Take(1).ToListAsync();
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Take_int_maxvalue_chained_after_where_returns_filtered_rows()
    {
        // Where narrows to 3 rows; Take(int.MaxValue) is effectively unbounded
        // on the windowed set. Pin: filtered result unaffected by the huge Take.
        var rows = await _ctx.Query<TmvItem>()
            .Where(i => i.V >= 200)
            .OrderBy(i => i.Id)
            .Take(int.MaxValue)
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(new[] { 2, 3, 4, 5 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Take_int_maxvalue_after_simple_equality_where_returns_matching_row()
    {
        // .Where(col == val).Take(int.MaxValue) hits the simple-where fast path. Pre-sizing the
        // result list to int.MaxValue would OOM ("Array dimensions exceeded supported range");
        // pin that the capacity hint is clamped and the single matching row comes back.
        var rows = await _ctx.Query<TmvItem>().Where(i => i.V == 300).Take(int.MaxValue).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Id);
    }

    [Table("TmvItem")]
    public sealed class TmvItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
