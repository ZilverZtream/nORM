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
/// Pins null-safe semantics for <c>List&lt;int?&gt;.Contains(p.NullableInt)</c>
/// (and the same-shape <c>list.Contains(p.col)</c> over a nullable column).
/// Prior nORM work (item #35 in the project memory) established that the
/// translator must emit <c>(col IN (...) OR col IS NULL)</c> when the
/// captured list contains null, because the SQL standard says
/// <c>col IN (NULL, @p1)</c> never matches a null row -- only
/// <c>col IS NULL</c> does. The d97c5f0 fix admitted instance Contains on
/// generic collections; this test verifies the null-safe handling kicks in
/// on the SAME instance path (not just the previous Enumerable-extension
/// path that #35 originally targeted).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereListContainsNullableColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WlcnItem (Id INTEGER PRIMARY KEY, OptId INTEGER NULL);
            INSERT INTO WlcnItem VALUES (1, NULL), (2, 100), (3, 200), (4, NULL), (5, 300);
            -- Rows: Id 1 and 4 have NULL OptId; Id 2/3/5 have 100/200/300.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WlcnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task List_Contains_with_only_non_null_values_returns_matching_rows()
    {
        // Baseline: non-null list -> standard IN(...). Should match rows with OptId 100/200.
        var values = new List<int?> { 100, 200 };
        var result = await _ctx.Query<WlcnItem>()
            .Where(i => values.Contains(i.OptId))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task List_Contains_with_null_in_list_matches_null_column_rows()
    {
        // {100, null} -> SQL must emit (OptId IN (100) OR OptId IS NULL).
        // Silent-wrongness: emitting `OptId IN (NULL, 100)` would skip Id 1/4
        // because SQL `col IN (NULL, ...)` never matches null.
        var values = new List<int?> { 100, null };
        var result = await _ctx.Query<WlcnItem>()
            .Where(i => values.Contains(i.OptId))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task List_Contains_with_only_null_in_list_matches_null_column_rows_only()
    {
        // All-nulls case: nORM #35 expects col IS NULL with no IN clause.
        var values = new List<int?> { null };
        var result = await _ctx.Query<WlcnItem>()
            .Where(i => values.Contains(i.OptId))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task List_Contains_with_empty_list_returns_no_rows()
    {
        // Empty IN clause -> SqlFalseLiteral path; no rows.
        var values = new List<int?>();
        var result = await _ctx.Query<WlcnItem>()
            .Where(i => values.Contains(i.OptId))
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("WlcnItem")]
    public sealed class WlcnItem
    {
        [Key] public int Id { get; set; }
        public int? OptId { get; set; }
    }
}
