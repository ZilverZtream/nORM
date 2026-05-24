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
/// Pins translation of computed key expressions inside OrderBy — specifically
/// the "nulls last" idiom <c>OrderBy(x =&gt; x.NullableValue ?? int.MaxValue)</c>
/// and arithmetic key like <c>OrderBy(x =&gt; x.A + x.B)</c>. The 98d2e76
/// commit added <c>??</c> translation in WHERE/SELECT; this verifies the same
/// rewrite composes inside the OrderBy clause.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByCoalesceTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OcRow (Id INTEGER PRIMARY KEY, Priority INTEGER NULL, A INTEGER NOT NULL, B INTEGER NOT NULL);
            INSERT INTO OcRow VALUES
                (1, 5,    1, 10),
                (2, NULL, 3, 4),
                (3, 1,    7, 2),
                (4, NULL, 2, 8),
                (5, 3,    0, 5);
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
    public async Task OrderBy_with_coalesce_fallback_pushes_nulls_to_end()
    {
        // (Priority ?? int.MaxValue) → row 3 (1), 5 (3), 1 (5), then 2/4 (NULL → MaxValue, tie-broken by Id).
        var ids = (await _ctx.Query<OcRow>()
            .OrderBy(r => r.Priority ?? int.MaxValue)
            .ThenBy(r => r.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 5, 1, 2, 4 }, ids);
    }

    [Fact]
    public async Task OrderBy_with_arithmetic_key_sorts_by_computed_sum()
    {
        // A + B per row: 11, 7, 9, 10, 5 → ascending order by sum: row 5 (5), 2 (7), 3 (9), 4 (10), 1 (11)
        var ids = (await _ctx.Query<OcRow>()
            .OrderBy(r => r.A + r.B)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 2, 3, 4, 1 }, ids);
    }

    [Table("OcRow")]
    public sealed class OcRow
    {
        [Key] public int Id { get; set; }
        public int? Priority { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }
}
