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
/// Single/SingleOrDefault async family parity with EF Core. The
/// underlying QueryTranslator already routes Single/SingleOrDefault
/// via FirstSingleTranslator and emits LIMIT 2 to detect duplicates;
/// these were the missing extension methods on the nORM async surface.
///
/// Pins each spelling (with and without predicate) on happy path,
/// duplicate-throws, empty-throws (Single) / null (SingleOrDefault),
/// and predicate composition. Silent-wrongness risks: predicate
/// dropped on the overload (would silently match all rows), or the
/// duplicate-detection LIMIT 2 not actually fetched (would return
/// the first match instead of throwing).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSingleAsyncFamilyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SinglePredItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
            INSERT INTO SinglePredItem VALUES (1, 10), (2, 20), (3, 20), (4, 30);
            -- Amount=10 is unique (Id=1).
            -- Amount=20 has 2 rows (Id=2 + Id=3) -> Single/SingleOrDefault throw.
            -- Amount=30 is unique (Id=4).
            -- Amount=999 has 0 rows -> Single throws, SingleOrDefault returns null.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SinglePredItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SingleAsync_returns_the_row_when_source_has_one()
    {
        var row = await _ctx.Query<SinglePredItem>().Where(i => i.Amount == 10).SingleAsync();
        Assert.Equal(1, row.Id);
    }

    [Fact]
    public async Task SingleAsync_throws_when_source_is_empty()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _ctx.Query<SinglePredItem>().Where(i => i.Amount == 999).SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_throws_when_source_has_two_or_more()
    {
        // Silent-wrongness: missing LIMIT 2 would return the first match
        // without throwing. The translator's LIMIT 2 + post-fetch count
        // check is what makes this contract enforceable.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _ctx.Query<SinglePredItem>().Where(i => i.Amount == 20).SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_predicate_returns_the_row_when_predicate_matches_one()
    {
        var row = await _ctx.Query<SinglePredItem>().SingleAsync(i => i.Amount == 30);
        Assert.Equal(4, row.Id);
    }

    [Fact]
    public async Task SingleAsync_predicate_throws_when_predicate_matches_two()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _ctx.Query<SinglePredItem>().SingleAsync(i => i.Amount == 20));
    }

    [Fact]
    public async Task SingleOrDefaultAsync_returns_null_when_source_is_empty()
    {
        var row = await _ctx.Query<SinglePredItem>().Where(i => i.Amount == 999).SingleOrDefaultAsync();
        Assert.Null(row);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_returns_the_row_when_source_has_one()
    {
        var row = await _ctx.Query<SinglePredItem>().Where(i => i.Amount == 10).SingleOrDefaultAsync();
        Assert.NotNull(row);
        Assert.Equal(1, row!.Id);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_throws_when_source_has_two_or_more()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _ctx.Query<SinglePredItem>().Where(i => i.Amount == 20).SingleOrDefaultAsync());
    }

    [Fact]
    public async Task SingleOrDefaultAsync_predicate_returns_null_when_no_match()
    {
        // Silent-wrongness: dropped predicate -> would throw the multi-
        // row exception because the table has 4 rows. Correct: null.
        var row = await _ctx.Query<SinglePredItem>().SingleOrDefaultAsync(i => i.Amount == 999);
        Assert.Null(row);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_predicate_returns_the_row_when_predicate_matches_one()
    {
        var row = await _ctx.Query<SinglePredItem>().SingleOrDefaultAsync(i => i.Amount == 30);
        Assert.NotNull(row);
        Assert.Equal(4, row!.Id);
    }

    [Table("SinglePredItem")]
    public sealed class SinglePredItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
    }
}
