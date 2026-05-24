using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the canonical paging shape <c>OrderBy(x).Skip(n).Take(m)</c>. SQL
/// emits <c>ORDER BY x LIMIT m OFFSET n</c>. Tests both literal and captured
/// (parameter) variants of Skip/Take and confirms the page contents are
/// deterministic and contiguous. Silent-wrongness risk if Skip and Take get
/// swapped, if the OFFSET / LIMIT values get baked from the wrong source, or
/// if Take applies before Skip (returning the wrong window).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSkipTakePagingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE StpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO StpRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task OrderBy_skip_then_take_with_literal_values_returns_correct_window()
    {
        // OrderBy Id ASC → 1..10. Skip(3).Take(4) → 4,5,6,7.
        var ids = (await _ctx.Query<StpRow>()
            .OrderBy(r => r.Id)
            .Skip(3)
            .Take(4)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4, 5, 6, 7 }, ids);
    }

    [Fact]
    public async Task OrderBy_skip_then_take_with_captured_values_returns_correct_window()
    {
        int pageSize = 4;
        int pageIndex = 1; // 0-based; second page → Skip(4).Take(4) → 5,6,7,8.
        var ids = (await _ctx.Query<StpRow>()
            .OrderBy(r => r.Id)
            .Skip(pageSize * pageIndex)
            .Take(pageSize)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 6, 7, 8 }, ids);
    }

    [Fact]
    public async Task Skip_past_end_of_results_returns_empty()
    {
        var ids = (await _ctx.Query<StpRow>()
            .OrderBy(r => r.Id)
            .Skip(100)
            .Take(5)
            .ToListAsync())
            .ToArray();
        Assert.Empty(ids);
    }

    [Fact]
    public async Task Take_then_skip_uses_skip_after_take_window()
    {
        // LINQ semantics for Take(n).Skip(m): Take a window of n, then drop m from it.
        // OrderBy(Id ASC).Take(5).Skip(2) → from {1,2,3,4,5} drop first 2 → {3,4,5}.
        var ids = (await _ctx.Query<StpRow>()
            .OrderBy(r => r.Id)
            .Take(5)
            .Skip(2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 4, 5 }, ids);
    }

    [Table("StpRow")]
    public sealed class StpRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
