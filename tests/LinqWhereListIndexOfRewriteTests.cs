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
/// Pins the implementation of the <c>list.IndexOf(col) [op] 0/-1</c>
/// idiom in Where predicates. Previously rejected (see 89fb655). LINQ
/// semantics:
///   * <c>IndexOf(x) &gt;= 0</c> is equivalent to <c>list.Contains(x)</c>
///   * <c>IndexOf(x) &lt; 0</c> / <c>== -1</c> is equivalent to
///     <c>!list.Contains(x)</c>
///   * <c>IndexOf(x) != -1</c> is equivalent to <c>list.Contains(x)</c>
///
/// The translator detects this BinaryExpression shape and rewrites it
/// to the equivalent Contains MethodCall, then re-visits -- so it
/// inherits all of the d97c5f0 Contains plumbing (List/HashSet/ICollection
/// receivers, b03d4ae null-safe contract, etc.).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereListIndexOfRewriteTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WlioRwItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO WlioRwItem VALUES (1, 10), (2, 20), (3, 30), (4, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WlioRwItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_List_IndexOf_greater_or_equal_zero_returns_matching_rows()
    {
        var ids = new List<int> { 2, 4 };
        var result = await _ctx.Query<WlioRwItem>()
            .Where(i => ids.IndexOf(i.Id) >= 0)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_List_IndexOf_equals_minus_one_returns_non_matching_rows()
    {
        var ids = new List<int> { 2, 4 };
        var result = await _ctx.Query<WlioRwItem>()
            .Where(i => ids.IndexOf(i.Id) == -1)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_List_IndexOf_less_than_zero_returns_non_matching_rows()
    {
        var ids = new List<int> { 2, 4 };
        var result = await _ctx.Query<WlioRwItem>()
            .Where(i => ids.IndexOf(i.Id) < 0)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_List_IndexOf_not_equal_minus_one_returns_matching_rows()
    {
        var ids = new List<int> { 2, 4 };
        var result = await _ctx.Query<WlioRwItem>()
            .Where(i => ids.IndexOf(i.Id) != -1)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WlioRwItem")]
    public sealed class WlioRwItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
