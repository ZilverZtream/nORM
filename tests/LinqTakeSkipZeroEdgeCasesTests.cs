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
/// Pins zero-valued paging boundaries: <c>Take(0)</c> must return no rows
/// and <c>Skip(0)</c> must return every row. Silent-wrongness if either
/// is folded out as a no-op: <c>Take(0)</c> dropped returns every row
/// (catastrophic in early-return paging that uses Take(0) to materialise
/// nothing), <c>Skip(0)</c> dropped is harmless but pins the contract.
/// Combinations with OrderBy also covered so paging-with-order interactions
/// don't drift.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTakeSkipZeroEdgeCasesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TszRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO TszRow VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
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
    public async Task Take_zero_returns_empty_sequence()
    {
        var ids = (await _ctx.Query<TszRow>()
            .OrderBy(p => p.Id)
            .Take(0)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        // Silent-wrongness check: if Take(0) is folded out, ids would be
        // [1..5] instead of empty.
        Assert.Empty(ids);
    }

    [Fact]
    public async Task Skip_zero_returns_full_sequence()
    {
        var ids = (await _ctx.Query<TszRow>()
            .OrderBy(p => p.Id)
            .Skip(0)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, ids);
    }

    [Fact]
    public async Task Skip_zero_then_take_two_pages_first_two_rows()
    {
        // Skip(0).Take(2) -> first page. Distinct from Take(0).Skip(0)
        // because here Take applies after a no-op Skip.
        var ids = (await _ctx.Query<TszRow>()
            .OrderBy(p => p.Id)
            .Skip(0)
            .Take(2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public async Task Skip_two_then_take_zero_returns_empty_after_offset()
    {
        // Skip(2).Take(0) -> still empty regardless of how many rows would
        // have followed the offset. Pins that the Take(0) limit fires
        // AFTER the Skip is applied.
        var ids = (await _ctx.Query<TszRow>()
            .OrderBy(p => p.Id)
            .Skip(2)
            .Take(0)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Empty(ids);
    }

    [Table("TszRow")]
    public sealed class TszRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
