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
    public async Task DistinctBy_returns_one_row_per_key_first_in_source_order()
    {
        // Source ordered by Id; first per Category: A -> 1, B -> 3.
        // The provider-shape tests pin that this is now server-side ROW_NUMBER,
        // not a post-materialize dedupe.
        var result = await _ctx.Query<KsoItem>()
            .OrderBy(i => i.Id)
            .DistinctBy(i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task ExceptBy_excludes_rows_whose_key_is_in_the_second_key_set()
    {
        // LINQ set semantics: first occurrence per surviving key. Source per-key
        // first: A->1, B->3. Drop A -> {3}.
        var excluded = new[] { "A" };
        var result = await _ctx.Query<KsoItem>()
            .ExceptBy(excluded, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task IntersectBy_keeps_rows_whose_key_is_in_the_second_key_set()
    {
        // Set semantics -> first row per matched key. Keep 'B' -> Id 3 only.
        var keep = new[] { "B" };
        var result = await _ctx.Query<KsoItem>()
            .IntersectBy(keep, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task UnionBy_appends_second_rows_whose_key_is_not_already_in_source()
    {
        // Source per-key first: A->1, B->3. Second adds 'X' (new) and reuses 'A'.
        // Result keys {A, B, X} in source-then-second order -> {1, 3, 99}.
        var other = new[]
        {
            new KsoItem { Id = 98, Category = "A", Amount = 1 },
            new KsoItem { Id = 99, Category = "X", Amount = 2 },
        };
        var result = await _ctx.Query<KsoItem>()
            .UnionBy(other, i => i.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 99 }, result.Select(r => r.Id).ToArray());
    }

    [Table("KsoItem")]
    public sealed class KsoItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
