using System;
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
/// Pins <c>q.OrderBy(Id).Take(N).ElementAt(k)</c>: must return the k-th
/// element of the windowed set, throwing IndexOutOfRange if k >= N. The
/// translator must not silently index into the full table.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqElementAtAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EaaRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO EaaRow VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 999);
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
    public async Task ElementAtOrDefault_after_take_returns_default_when_index_outside_window()
    {
        // Take(3) → 3 rows (Ids 1,2,3). ElementAt(5) on this window is out-of-range → null.
        // Full table ElementAt(4) would return row 5 (Id=5, V=999) — silent-wrongness.
        var hit = await _ctx.Query<EaaRow>().OrderBy(r => r.Id).Take(3).ElementAtOrDefaultAsync(4);
        Assert.Null(hit);
    }

    [Fact]
    public async Task ElementAt_after_take_returns_indexed_row_within_window()
    {
        // Take(3) → Ids 1,2,3. ElementAt(2) → Id=3.
        var hit = await _ctx.Query<EaaRow>().OrderBy(r => r.Id).Take(3).ElementAtAsync(2);
        Assert.Equal(3, hit.Id);
    }

    [Table("EaaRow")]
    public sealed class EaaRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
