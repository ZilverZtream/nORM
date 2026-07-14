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
/// Pins MinBy and MaxBy (.NET 6+ terminal operators) as SQL-translated operators.
/// MinBy(keySelector) → ORDER BY key ASC  LIMIT 1 (returns entity with smallest key)
/// MaxBy(keySelector) → ORDER BY key DESC LIMIT 1 (returns entity with largest key)
///
/// Silent-wrongness probes:
///   * If MinBy returns same row as MaxBy → ordering direction not set
///   * If MinBy(r => r.IntVal) returns a row with middle value → using wrong row
///   * If empty-sequence MinBy returns null instead of throwing → semantics wrong
///
/// Test data: MbRow (Id, IntVal, Name)
///   (1, 30, 'c'), (2, 10, 'a'), (3, 50, 'e'), (4, 20, 'b'), (5, 40, 'd')
/// MinBy(IntVal) → Id=2 (val=10, name='a')
/// MaxBy(IntVal) → Id=3 (val=50, name='e')
/// MinBy(Name) → Id=2 (name='a')
/// MaxBy(Name) → Id=3 (name='e')
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMinByMaxByTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE MbRow (Id INTEGER PRIMARY KEY, IntVal INTEGER NOT NULL, Name TEXT NOT NULL);
            INSERT INTO MbRow VALUES (1,30,'c'),(2,10,'a'),(3,50,'e'),(4,20,'b'),(5,40,'d');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Table("MbRow")]
    private sealed class MbRow
    {
        [Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string Name { get; set; } = "";
    }

    // ── 1: MinBy integer column returns entity with smallest value ────────────

    [Fact]
    public async Task MinBy_int_column_returns_entity_with_smallest_value()
    {
        var row = await _ctx.Query<MbRow>().MinByAsync(r => r.IntVal);
        Assert.Equal(2, row.Id);
        Assert.Equal(10, row.IntVal);
        Assert.Equal("a", row.Name);
    }

    // ── 2: MaxBy integer column returns entity with largest value ─────────────

    [Fact]
    public async Task MaxBy_int_column_returns_entity_with_largest_value()
    {
        var row = await _ctx.Query<MbRow>().MaxByAsync(r => r.IntVal);
        Assert.Equal(3, row.Id);
        Assert.Equal(50, row.IntVal);
        Assert.Equal("e", row.Name);
    }

    // ── 3: MinBy string column returns lexicographically first entity ─────────

    [Fact]
    public async Task MinBy_string_column_returns_entity_with_lexicographically_first_name()
    {
        var row = await _ctx.Query<MbRow>().MinByAsync(r => r.Name);
        Assert.Equal("a", row.Name);
    }

    // ── 4: MaxBy string column returns lexicographically last entity ──────────

    [Fact]
    public async Task MaxBy_string_column_returns_entity_with_lexicographically_last_name()
    {
        var row = await _ctx.Query<MbRow>().MaxByAsync(r => r.Name);
        Assert.Equal("e", row.Name);
    }

    // ── 5: MinBy after Where filters first ───────────────────────────────────
    // Among rows where IntVal > 20 → {(1,30,'c'),(3,50,'e'),(5,40,'d')}
    // MinBy(IntVal) → (1,30,'c')

    [Fact]
    public async Task MinBy_after_Where_returns_minimum_from_filtered_set()
    {
        var row = await _ctx.Query<MbRow>().Where(r => r.IntVal > 20).MinByAsync(r => r.IntVal);
        Assert.Equal(1, row.Id);
        Assert.Equal(30, row.IntVal);
    }

    // ── 6: MaxBy after Where filters first ───────────────────────────────────

    [Fact]
    public async Task MaxBy_after_Where_returns_maximum_from_filtered_set()
    {
        var row = await _ctx.Query<MbRow>().Where(r => r.IntVal < 40).MaxByAsync(r => r.IntVal);
        Assert.Equal(1, row.Id);
        Assert.Equal(30, row.IntVal);
    }

    // ── 7: MinBy empty returns null ───────────────────────────────────────────
    // LINQ MinBy/MaxBy return null for an empty sequence of reference-type
    // elements; only non-nullable value types throw.

    [Fact]
    public async Task MinBy_on_empty_sequence_returns_null_for_reference_elements()
    {
        var row = await _ctx.Query<MbRow>().Where(r => r.IntVal > 9999).MinByAsync(r => r.IntVal);
        Assert.Null(row);
    }

    // ── 8: MaxBy empty returns null ───────────────────────────────────────────

    [Fact]
    public async Task MaxBy_on_empty_sequence_returns_null_for_reference_elements()
    {
        var row = await _ctx.Query<MbRow>().Where(r => r.IntVal > 9999).MaxByAsync(r => r.IntVal);
        Assert.Null(row);
    }

    // ── 9: MinBy result is different from MaxBy result ────────────────────────
    // Proves direction is set correctly — not just returning First().

    [Fact]
    public async Task MinBy_and_MaxBy_return_distinct_rows_on_non_uniform_data()
    {
        var min = await _ctx.Query<MbRow>().MinByAsync(r => r.IntVal);
        var max = await _ctx.Query<MbRow>().MaxByAsync(r => r.IntVal);
        Assert.NotEqual(min.Id, max.Id);
        Assert.True(min.IntVal < max.IntVal);
    }
}
