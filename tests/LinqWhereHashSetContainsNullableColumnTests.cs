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
/// Sister of b03d4ae (List&lt;T?&gt;.Contains null-safe pin), but for HashSet
/// receivers. HashSet&lt;T&gt; goes through the same admit-gate (d97c5f0:
/// IsTranslatableContainsReceiver accepts any IEnumerable&lt;T&gt; /
/// ICollection&lt;T&gt;) and the same handler that walks items via
/// TryGetConstantValue. This pin verifies the null-safe
/// <c>(col IN (...) OR col IS NULL)</c> emit from memory item #35 also
/// fires on the HashSet receiver path.
///
/// Silent-wrongness probes mirror b03d4ae but stress the HashSet path
/// specifically: if the handler's <c>colVal is IEnumerable</c> branch
/// silently iterates without honoring the null-separation logic, the
/// null-in-set case would fail to match null column rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereHashSetContainsNullableColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WhscItem (Id INTEGER PRIMARY KEY, OptId INTEGER NULL);
            INSERT INTO WhscItem VALUES (1, NULL), (2, 100), (3, 200), (4, NULL), (5, 300);
            -- Rows: Id 1 and 4 have NULL OptId; Id 2/3/5 have 100/200/300.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WhscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task HashSet_Contains_with_only_non_null_values_returns_matching_rows()
    {
        // Baseline: HashSet<int?> with only non-null values -> standard IN.
        // Matches rows with OptId 100/200.
        var values = new HashSet<int?> { 100, 200 };
        var result = await _ctx.Query<WhscItem>()
            .Where(i => values.Contains(i.OptId))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task HashSet_Contains_with_null_in_set_matches_null_column_rows()
    {
        // {100, null} -> must emit (OptId IN (100) OR OptId IS NULL). Silent-
        // wrongness: emitting `OptId IN (NULL, 100)` would skip Id 1/4 because
        // SQL `col IN (NULL, ...)` never matches null.
        var values = new HashSet<int?> { 100, null };
        var result = await _ctx.Query<WhscItem>()
            .Where(i => values.Contains(i.OptId))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task HashSet_Contains_with_only_null_matches_null_column_rows_only()
    {
        // All-null case: must emit `col IS NULL` with no IN clause.
        var values = new HashSet<int?> { null };
        var result = await _ctx.Query<WhscItem>()
            .Where(i => values.Contains(i.OptId))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task HashSet_Contains_with_empty_set_returns_no_rows()
    {
        // Empty IN clause -> SqlFalseLiteral path; no rows.
        var values = new HashSet<int?>();
        var result = await _ctx.Query<WhscItem>()
            .Where(i => values.Contains(i.OptId))
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("WhscItem")]
    public sealed class WhscItem
    {
        [Key] public int Id { get; set; }
        public int? OptId { get; set; }
    }
}
