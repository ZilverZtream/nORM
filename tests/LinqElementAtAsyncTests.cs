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
/// EF-parity additions: <c>ElementAtAsync(index)</c> and
/// <c>ElementAtOrDefaultAsync(index)</c>. The translator's
/// <c>ElementAtTranslator</c> rewrites <c>ElementAt(N)</c> as
/// <c>Skip(N).Take(1)</c>, so the SQL pipeline only fetches the
/// targeted row. Both were missing from the async surface even though
/// <c>NormQueryProvider</c> already maps both terminals.
///
/// Pins behavior + silent-wrongness probes:
///   * dropped index -> returns first row instead of the requested one
///   * out-of-range -> Throws (ElementAt) vs null (ElementAtOrDefault)
///   * negative index -> ArgumentOutOfRangeException on ElementAtAsync,
///     null on ElementAtOrDefaultAsync (LINQ Enumerable semantics)
///   * composes after outer Where / OrderBy
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqElementAtAsyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EaiItem (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO EaiItem VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<EaiItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ElementAtAsync_returns_row_at_index_after_orderby()
    {
        // OrderBy(Id) -> ids {1,2,3,4}; ElementAt(2) -> Id=3 (Code=C).
        // Silent-wrongness: dropped index -> returns Id=1 (Code=A).
        var row = await _ctx.Query<EaiItem>().OrderBy(i => i.Id).ElementAtAsync(2);
        Assert.Equal(3, row.Id);
        Assert.Equal("C", row.Code);
    }

    [Fact]
    public async Task ElementAtAsync_throws_when_index_out_of_range()
    {
        // Index 10 is beyond the 4-row source; ElementAt must throw to
        // match LINQ Enumerable semantics. NormQueryProvider throws
        // ArgumentOutOfRangeException("index") (matching Enumerable.ElementAt
        // which throws ArgumentOutOfRangeException, NOT InvalidOperationException
        // -- the latter is Single/First's empty-source exception).
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await _ctx.Query<EaiItem>().OrderBy(i => i.Id).ElementAtAsync(10));
    }

    [Fact]
    public void ElementAtAsync_negative_index_throws_argument_out_of_range()
    {
        // Negative-index guard is synchronous (throws before the Task is constructed),
        // so the lambda's return type stays void and we can use Assert.Throws.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = _ctx.Query<EaiItem>().ElementAtAsync(-1);
        });
    }

    [Fact]
    public async Task ElementAtOrDefaultAsync_returns_row_at_index_after_orderby()
    {
        var row = await _ctx.Query<EaiItem>().OrderBy(i => i.Id).ElementAtOrDefaultAsync(1);
        Assert.NotNull(row);
        Assert.Equal(2, row!.Id);
    }

    [Fact]
    public async Task ElementAtOrDefaultAsync_returns_null_when_index_out_of_range()
    {
        var row = await _ctx.Query<EaiItem>().OrderBy(i => i.Id).ElementAtOrDefaultAsync(99);
        Assert.Null(row);
    }

    [Fact]
    public async Task ElementAtOrDefaultAsync_negative_index_returns_null_matching_linq()
    {
        // Enumerable.ElementAtOrDefault returns default for negative indexes; the wrapper
        // short-circuits to null without round-tripping a negative OFFSET to the DB.
        var row = await _ctx.Query<EaiItem>().ElementAtOrDefaultAsync(-1);
        Assert.Null(row);
    }

    [Fact]
    public async Task ElementAtAsync_composes_after_outer_where()
    {
        // Outer Where keeps {2,3,4}; OrderBy(Id) -> {2,3,4}; ElementAt(1) -> Id=3.
        // Silent-wrongness probes both predicate-drop (would return Id=2) and index-drop.
        var row = await _ctx.Query<EaiItem>()
            .Where(i => i.Id >= 2)
            .OrderBy(i => i.Id)
            .ElementAtAsync(1);
        Assert.Equal(3, row.Id);
    }

    [Table("EaiItem")]
    public sealed class EaiItem
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
