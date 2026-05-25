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
/// Sister of <see cref="LinqAggregateAfterTakeTests"/> for the predicate-
/// aggregate Any/All. <c>q.OrderBy(Id).Take(2).Any(r =&gt; r.Active)</c>
/// must check whether ANY of the first 2 rows is Active. If the translator
/// runs Any on the full table, it'll find Active rows further down.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAnyAllAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AatRow (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL);
            -- First 2 rows are Active=0. Row 3 is Active=1.
            INSERT INTO AatRow VALUES (1,0),(2,0),(3,1),(4,1),(5,0);
            -- Take(2).Any(r => r.Active == 1) must be FALSE (first 2 rows have no Active).
            -- Full-table Any(r => r.Active == 1) is TRUE — that's the silent-wrong answer.
            -- Take(2).All(r => r.Active == 0) must be TRUE (first 2 rows are both 0).
            -- Full-table All(r => r.Active == 0) is FALSE — wrong if applied to whole table.
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
    public async Task Composed_take_where_any_chain_evaluates_predicate_only_inside_window()
    {
        // First 2 rows (1,0), (2,0) — both have Active=0, none match Active==1. The
        // windowed Any returns false. Full-table Any(Active==1) would be true.
        Assert.False(await _ctx.Query<AatRow>()
            .OrderBy(r => r.Id)
            .Take(2)
            .Where(r => r.Active == 1)
            .AnyAsync());
    }

    [Fact]
    public async Task Bare_any_after_take_returns_true_when_window_non_empty()
    {
        // Take(2) yields 2 rows; bare Any (no predicate) returns true.
        Assert.True(await _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(2).AnyAsync());
    }

    [Fact]
    public async Task All_after_take_evaluates_only_windowed_rows_matching_predicate()
    {
        // First 2 rows are Active=0 → All(Active==0) is TRUE.
        Assert.True(await _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(2).AllAsync(r => r.Active == 0));
        // Full-table All would be false (rows 3,4 are Active=1).
        Assert.False(await _ctx.Query<AatRow>().OrderBy(r => r.Id).AllAsync(r => r.Active == 0));
    }


    [Table("AatRow")]
    public sealed class AatRow
    {
        [Key] public int Id { get; set; }
        public int Active { get; set; }
    }
}
