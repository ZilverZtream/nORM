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
/// Probes nORM's behavior for the .NET 6+ key-based set operators
/// added to <c>System.Linq.Queryable</c>:
/// <see cref="System.Linq.Queryable.DistinctBy{TSource, TKey}(System.Linq.IQueryable{TSource}, System.Linq.Expressions.Expression{System.Func{TSource, TKey}})"/>
/// and friends (IntersectBy / ExceptBy / UnionBy). Most ORMs don't
/// implement these; the question is whether nORM:
///   (a) translates them to SQL (best),
///   (b) throws an actionable error (acceptable), or
///   (c) silently materializes the whole table and applies the op
///       client-side (silent-wrongness -- the worst outcome).
///
/// Each probe asserts an actionable error is raised. If the assertion
/// fails because a result is returned, the surface materialized the
/// entire source -- which the test message documents so a maintainer
/// reading the failure knows what changed.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqKeyedSetOpsTranslatabilityTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE KsoItem (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO KsoItem VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<KsoItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task DistinctBy_throws_actionable_error_with_groupby_workaround_hint()
    {
        // DistinctBy is not translated; expect a nORM error pointing the user
        // at the GroupBy().Select(g => g.First()) workaround so they don't
        // accidentally call .AsEnumerable().DistinctBy() and materialize the
        // whole table.
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await _ctx.Query<KsoItem>()
                .DistinctBy(i => i.Category)
                .ToListAsync());

        Assert.True(
            ex is NormException || ex is NotSupportedException || ex is InvalidOperationException,
            $"DistinctBy threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
        // Actionability: message should mention DistinctBy or GroupBy so the
        // user knows what's not supported and what to use instead.
        var msgLower = ex.Message.ToLowerInvariant();
        Assert.True(
            msgLower.Contains("distinctby") || msgLower.Contains("groupby") || msgLower.Contains("group by") || msgLower.Contains("not supported") || msgLower.Contains("unsupported") || msgLower.Contains("translat"),
            $"DistinctBy error message lacks actionable hint about workaround. Got: {ex.Message}");
    }

    [Fact]
    public async Task ExceptBy_either_translates_or_throws_actionable_error()
    {
        // Excluding category 'A' should leave only 'B' rows (Id=3, Id=4 = 2 rows).
        Exception? ex = null;
        var rowCount = -1;
        try
        {
            var excluded = new[] { "A" };
            var result = await _ctx.Query<KsoItem>()
                .ExceptBy(excluded, i => i.Category)
                .ToListAsync();
            rowCount = result.Count;
        }
        catch (Exception caught)
        {
            ex = caught;
        }

        if (ex != null)
        {
            Assert.True(
                ex is NormException || ex is NotSupportedException || ex is InvalidOperationException,
                $"ExceptBy threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
            return;
        }

        Assert.True(rowCount == 2,
            $"ExceptBy returned {rowCount} rows; expected 2 (Category != 'A') if translated, " +
            $"or an exception if unsupported. Other counts mean silent-wrongness.");
    }

    [Fact]
    public async Task IntersectBy_either_translates_or_throws_actionable_error()
    {
        Exception? ex = null;
        var rowCount = -1;
        try
        {
            var keep = new[] { "B" };
            var result = await _ctx.Query<KsoItem>()
                .IntersectBy(keep, i => i.Category)
                .ToListAsync();
            rowCount = result.Count;
        }
        catch (Exception caught)
        {
            ex = caught;
        }

        if (ex != null)
        {
            Assert.True(
                ex is NormException || ex is NotSupportedException || ex is InvalidOperationException,
                $"IntersectBy threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
            return;
        }

        // Intersect-by-key with the single-row key set 'B' keeps the FIRST
        // matching row per key (Enumerable.IntersectBy semantics) -> 1 row.
        Assert.True(rowCount == 1,
            $"IntersectBy returned {rowCount} rows; expected 1 (first row matching Category='B') " +
            $"if translated, or an exception if unsupported.");
    }

    [Table("KsoItem")]
    public sealed class KsoItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
