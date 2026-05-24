using System.Collections.Generic;
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
/// Pins the implementation of <c>ExceptBy</c>, <c>IntersectBy</c>, and
/// <c>UnionBy</c> — the same shape as DistinctBy (7ff7bc2) with an
/// extra IEnumerable argument: a key set (ExceptBy/IntersectBy) or a
/// source set (UnionBy). Each is implemented via the
/// PostMaterializeTransform plumbing: compile a closure that operates
/// on the materialized list and applies the LINQ set-op semantics.
///
/// LINQ semantics (Enumerable.* parity):
///   * ExceptBy: keep source elements whose key is NOT in `second`'s
///     key set. Set semantics -> first occurrence per key.
///   * IntersectBy: keep source elements whose key IS in `second`'s
///     key set. Set semantics -> first occurrence per key.
///   * UnionBy: source ∪ second deduped by key, source order first.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExceptByIntersectByUnionByImplementationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EibItem (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO EibItem VALUES
                (1, 'A', 10),
                (2, 'A', 20),
                (3, 'B', 30),
                (4, 'B', 40),
                (5, 'C', 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<EibItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ExceptBy_excludes_rows_whose_key_is_in_the_second_key_set()
    {
        // Exclude category 'A'. Set semantics -> first occurrence per remaining key.
        // Source per-key first: A->1, B->3, C->5. Drop A -> {3, 5}.
        var excluded = new[] { "A" };
        var result = await _ctx.Query<EibItem>()
            .OrderBy(i => i.Id)
            .ExceptBy(excluded, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task IntersectBy_keeps_rows_whose_key_is_in_the_second_key_set()
    {
        // Keep categories {B, C}. Set semantics -> first occurrence per matched key.
        // Source per-key first: B->3, C->5. -> {3, 5}.
        var keep = new[] { "B", "C" };
        var result = await _ctx.Query<EibItem>()
            .OrderBy(i => i.Id)
            .IntersectBy(keep, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task UnionBy_appends_second_rows_whose_key_is_not_already_in_source()
    {
        // Source categories: {A, B, C}. Second includes one new key 'X' and a
        // duplicate 'A'. Result keys: {A, B, C, X}. Source order takes priority
        // for duplicates -> A from source (Id=1), B from source (Id=3),
        // C from source (Id=5), X from second (Id=99).
        var other = new[]
        {
            new EibItem { Id = 98, Category = "A", Amount = 1 },
            new EibItem { Id = 99, Category = "X", Amount = 2 },
        };
        var result = await _ctx.Query<EibItem>()
            .OrderBy(i => i.Id)
            .UnionBy(other, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 5, 99 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task ExceptBy_with_empty_key_set_returns_all_distinct_source_rows()
    {
        // Empty exclude set -> equivalent to DistinctBy. First per key: 1, 3, 5.
        var excluded = System.Array.Empty<string>();
        var result = await _ctx.Query<EibItem>()
            .OrderBy(i => i.Id)
            .ExceptBy(excluded, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("EibItem")]
    public sealed class EibItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
