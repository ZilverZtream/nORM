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
/// Pins the canonical <c>OrderBy(k).First()</c> and friends. The ORDER BY must
/// survive to the LIMIT 1 SQL — otherwise SQLite picks an arbitrary row and the
/// "first" you get back is non-deterministic across runs. Silent-wrongness if
/// the ORDER BY is dropped before the LIMIT.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqFirstSingleAfterOrderByTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FsoRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
            -- Inserted in non-sorted order on purpose so a naive "first physical row"
            -- query returns a different result than the OrderBy-required one.
            INSERT INTO FsoRow VALUES (3,30),(1,10),(5,50),(2,20),(4,40);
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
    public async Task First_after_orderby_returns_first_row_by_order_not_insertion()
    {
        var r = await _ctx.Query<FsoRow>().OrderBy(x => x.Id).FirstAsync();
        Assert.Equal(1, r.Id);
    }

    [Fact]
    public async Task FirstOrDefault_after_orderby_descending_returns_highest()
    {
        var r = await _ctx.Query<FsoRow>().OrderByDescending(x => x.Score).FirstOrDefaultAsync();
        Assert.NotNull(r);
        Assert.Equal(5, r!.Id);
        Assert.Equal(50, r.Score);
    }

    [Fact]
    public async Task First_after_where_orderby_returns_the_unique_match()
    {
        // nORM's async surface offers First/FirstOrDefault/Last/LastOrDefault — no
        // Single*Async. For a known-unique row, First is equivalent.
        var r = await _ctx.Query<FsoRow>()
            .Where(x => x.Score == 30)
            .OrderBy(x => x.Id)
            .FirstAsync();
        Assert.Equal(3, r.Id);
    }

    [Fact]
    public async Task FirstOrDefault_with_no_match_returns_null()
    {
        var r = await _ctx.Query<FsoRow>().Where(x => x.Score == 99).FirstOrDefaultAsync();
        Assert.Null(r);
    }

    [Fact]
    public async Task Last_after_orderby_returns_last_row_by_order()
    {
        var r = await _ctx.Query<FsoRow>().OrderBy(x => x.Id).LastAsync();
        Assert.Equal(5, r.Id);
    }

    [Table("FsoRow")]
    public sealed class FsoRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
