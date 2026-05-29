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
/// Pins the DistinctBy implementation: one row per distinct key, full row
/// preserved. nORM lowers this to ROW_NUMBER() OVER (PARTITION BY key)
/// and filters rn = 1 so the dedupe runs in the provider, not after a
/// full client-side materialization.
///
/// LINQ semantics: keeps the FIRST row encountered per key (source order).
/// The tests rely on the natural Id-ordered iteration for determinism.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctByImplementationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DbiItem (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO DbiItem VALUES
                (1, 'A', 10),
                (2, 'A', 20),
                (3, 'B', 30),
                (4, 'B', 40),
                (5, 'C', 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DbiItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task DistinctBy_keeps_first_row_per_category()
    {
        // Iteration is Id-ordered (PK ascending). First row per Category:
        //   A -> Id 1, B -> Id 3, C -> Id 5.
        var result = await _ctx.Query<DbiItem>()
            .OrderBy(i => i.Id)
            .DistinctBy(i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task DistinctBy_after_Where_keeps_first_matching_row_per_key()
    {
        // Filter to Amount > 15 -> Ids 2, 3, 4, 5 (Categories A, B, B, C).
        // First per Category: A -> 2, B -> 3, C -> 5.
        var result = await _ctx.Query<DbiItem>()
            .Where(i => i.Amount > 15)
            .OrderBy(i => i.Id)
            .DistinctBy(i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task DistinctBy_on_unique_key_returns_all_rows()
    {
        // Id is unique per row -> DistinctBy(Id) returns all 5 rows.
        var result = await _ctx.Query<DbiItem>()
            .OrderBy(i => i.Id)
            .DistinctBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("DbiItem")]
    public sealed class DbiItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
