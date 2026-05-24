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
/// Pins <c>Where(p =&gt; !p.IsActive)</c> — bare-column boolean negation in a
/// predicate. Should emit <c>NOT IsActive</c> or <c>IsActive = 0</c>; if the
/// negation is silently dropped, the inverted set is returned with no error.
/// One of the highest-priority silent-wrongness shapes because the wrong rows
/// look correct to the test data but contain the opposite truth value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereBooleanNegationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE BnRow (Id INTEGER PRIMARY KEY, IsActive INTEGER NOT NULL, Tier INTEGER NOT NULL);
            -- Asymmetric counts so a dropped `!` produces a different answer
            -- (3 active vs 2 inactive rows; mixed Tier so AND-chained filters
            -- exercise the AndAlso negation branch).
            INSERT INTO BnRow VALUES
              (1, 1, 1),
              (2, 0, 2),
              (3, 1, 1),
              (4, 0, 1),
              (5, 1, 2);
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
    public async Task Where_bang_bool_column_returns_inactive_rows()
    {
        // If `!` silently drops, this returns the active rows (Ids 1, 3, 5) —
        // three rows instead of two, immediately failing the assertion.
        var ids = (await _ctx.Query<BnRow>()
            .Where(p => !p.IsActive)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4 }, ids);
    }

    [Fact]
    public async Task Where_bang_bool_column_with_and_filter_intersects_correctly()
    {
        // !IsActive AND Tier == 1 → row 4 only. Exercises the AndAlso branch
        // which calls TryEmitMappedBooleanPredicate on each side — the `!`
        // arm must still flip expectedValue to false.
        var ids = (await _ctx.Query<BnRow>()
            .Where(p => !p.IsActive && p.Tier == 1)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4 }, ids);
    }

    [Fact]
    public async Task Where_bang_bool_column_or_filter_unions_correctly()
    {
        // !IsActive OR Tier == 2 → inactive {2,4} ∪ tier2 {2,5} = {2, 4, 5}.
        var ids = (await _ctx.Query<BnRow>()
            .Where(p => !p.IsActive || p.Tier == 2)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4, 5 }, ids);
    }

    [Fact]
    public async Task Where_double_negation_collapses_to_identity()
    {
        // !!IsActive should behave as IsActive (returns active rows {1, 3, 5}).
        // TryEmitMappedBooleanPredicate flips expectedValue twice → back to true.
        var ids = (await _ctx.Query<BnRow>()
            .Where(p => !!p.IsActive)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3, 5 }, ids);
    }

    [Table("BnRow")]
    public sealed class BnRow
    {
        [Key] public int Id { get; set; }
        public bool IsActive { get; set; }
        public int Tier { get; set; }
    }
}
