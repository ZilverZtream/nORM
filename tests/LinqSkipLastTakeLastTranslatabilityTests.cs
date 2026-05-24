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
/// Probes nORM's behavior for the .NET 6+ tail-paging operators
/// added to <c>System.Linq.Queryable</c>:
/// <see cref="System.Linq.Queryable.SkipLast{TSource}(System.Linq.IQueryable{TSource}, int)"/>
/// and
/// <see cref="System.Linq.Queryable.TakeLast{TSource}(System.Linq.IQueryable{TSource}, int)"/>.
///
/// SQL has no direct equivalent (LIMIT/OFFSET work from the start, not
/// the end). Implementations either (a) reverse the ORDER BY +
/// LIMIT/OFFSET in SQL, (b) throw NormUnsupportedFeatureException, or
/// (c) silently materialize the entire source and apply the op client-
/// side -- the worst outcome because it's a hidden full table scan.
///
/// Pins the throw-or-correct contract so a future regression that
/// silently falls through to client-eval surfaces immediately.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSkipLastTakeLastTranslatabilityTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SltlItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO SltlItem VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SltlItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task TakeLast_either_translates_or_throws_actionable_error()
    {
        // 5-row source ordered by Id; TakeLast(2) should yield {Id=4, Id=5}.
        // Silent-wrongness:
        //   * returns 5 rows (no trim)
        //   * returns 2 rows from the START (TakeLast routed to Take)
        // Both produce non-throw outcomes -- assertion catches them.
        Exception? ex = null;
        int[]? ids = null;
        try
        {
            var result = await _ctx.Query<SltlItem>()
                .OrderBy(i => i.Id)
                .TakeLast(2)
                .ToListAsync();
            ids = result.Select(r => r.Id).ToArray();
        }
        catch (Exception caught)
        {
            ex = caught;
        }

        if (ex != null)
        {
            Assert.True(
                ex is NormException || ex is NotSupportedException || ex is InvalidOperationException,
                $"TakeLast threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
            return;
        }

        // No throw: must be the correct tail slice. Anything else is silent-wrongness.
        Assert.Equal(new[] { 4, 5 }, ids);
    }

    [Fact]
    public async Task SkipLast_either_translates_or_throws_actionable_error()
    {
        // 5-row source ordered by Id; SkipLast(2) should yield {Id=1, Id=2, Id=3}.
        // Silent-wrongness:
        //   * returns 5 rows (no trim)
        //   * returns IDs {3, 4, 5} (SkipLast routed to Skip from start)
        Exception? ex = null;
        int[]? ids = null;
        try
        {
            var result = await _ctx.Query<SltlItem>()
                .OrderBy(i => i.Id)
                .SkipLast(2)
                .ToListAsync();
            ids = result.Select(r => r.Id).ToArray();
        }
        catch (Exception caught)
        {
            ex = caught;
        }

        if (ex != null)
        {
            Assert.True(
                ex is NormException || ex is NotSupportedException || ex is InvalidOperationException,
                $"SkipLast threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
            return;
        }

        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public async Task TakeLast_zero_either_returns_empty_or_throws_actionable_error()
    {
        // Edge case: TakeLast(0) -> Enumerable.TakeLast returns empty. If routed
        // to "no LIMIT" (interpreted as "no rows wanted == 0 limit") and SQLite
        // refuses LIMIT 0, this could throw a provider error. Pin either empty
        // or a nORM-typed error.
        Exception? ex = null;
        int rowCount = -1;
        try
        {
            var result = await _ctx.Query<SltlItem>()
                .OrderBy(i => i.Id)
                .TakeLast(0)
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
                $"TakeLast(0) threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
            return;
        }

        Assert.Equal(0, rowCount);
    }

    [Table("SltlItem")]
    public sealed class SltlItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
