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
    public async Task Composed_take_where_any_chain_throws_actionable_pin_via_one_of_the_two_pins()
    {
        // Chain `Take.Where.Any` has Take inside Any's source, so either the
        // HandleSetOperation pin (this commit) catches it on the way in or
        // 47acc83's WhereTranslator pin catches the Where-after-Take leg —
        // both are correct actionable throws. Accept either pin's message,
        // require Take + an actionable hint somewhere in the text.
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await _ctx.Query<AatRow>()
                .OrderBy(r => r.Id)
                .Take(2)
                .Where(r => r.Active == 1)
                .AnyAsync();
        });
        Assert.IsType<NormUnsupportedFeatureException>(ex);
        Assert.Contains("Take", ex.Message, System.StringComparison.Ordinal);
        var mentionsAnyOrWhere = ex.Message.Contains("Any", System.StringComparison.Ordinal)
                              || ex.Message.Contains("Where", System.StringComparison.Ordinal);
        Assert.True(mentionsAnyOrWhere, $"Message should call out Any or Where as the operator that hit the pin: {ex.Message}");
    }

    [Fact]
    public async Task Bare_any_after_take_throws_with_workaround_hint_avoiding_invalid_sql()
    {
        // Without the new pin, this emitted `… LIMIT 2 LIMIT 1` and SQLite errored
        // with `near "LIMIT": syntax error`. The pin now throws
        // NormUnsupportedFeatureException explaining that for N>=1 `Take(N).Any()`
        // is equivalent to `Any()` and pointing at the materialize workaround.
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await _ctx.Query<AatRow>().OrderBy(r => r.Id).Take(2).AnyAsync();
        });
        Assert.IsType<NormUnsupportedFeatureException>(ex);
        Assert.Contains("Any", ex.Message, System.StringComparison.Ordinal);
        Assert.Contains("Take", ex.Message, System.StringComparison.Ordinal);
        Assert.Contains("Drop the Take", ex.Message, System.StringComparison.Ordinal);
    }


    [Table("AatRow")]
    public sealed class AatRow
    {
        [Key] public int Id { get; set; }
        public int Active { get; set; }
    }
}
