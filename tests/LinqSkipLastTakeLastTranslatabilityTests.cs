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
    public async Task TakeLast_after_OrderBy_returns_tail_slice_in_original_order()
    {
        // 5-row source ordered by Id; TakeLast(2) yields {Id=4, Id=5}.
        // Implementation: translator flips the ORDER BY direction, applies
        // LIMIT 2 (DB returns the last 2 ids in descending order), and the
        // materializer reverses the resulting 2-row list to restore the
        // original ascending order.
        var result = await _ctx.Query<SltlItem>()
            .OrderBy(i => i.Id)
            .TakeLast(2)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task SkipLast_after_OrderBy_drops_tail_returns_remaining_in_original_order()
    {
        // 5-row source ordered by Id; SkipLast(2) yields {Id=1, Id=2, Id=3}.
        // Implementation: flip ORDER BY direction, apply OFFSET 2 (DB returns
        // the first n-2 rows in descending order), materializer reverses.
        var result = await _ctx.Query<SltlItem>()
            .OrderBy(i => i.Id)
            .SkipLast(2)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task TakeLast_zero_returns_empty_collection()
    {
        // Edge case: TakeLast(0) -> empty. The flipped-order LIMIT 0 returns
        // no rows; the reverse is a no-op on an empty list.
        var result = await _ctx.Query<SltlItem>()
            .OrderBy(i => i.Id)
            .TakeLast(0)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("SltlItem")]
    public sealed class SltlItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
