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
/// Pins <c>ThenBy(r =&gt; new { r.B, r.C })</c> — composite anonymous key on
/// the secondary sort. OrderByTranslator handles OrderBy / OrderByDescending
/// / ThenBy / ThenByDescending uniformly, so the 560dd16 NewExpression
/// expansion fix must apply to ThenBy too. Verifies regression coverage
/// for the secondary-sort path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqThenByCompositeKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TbckRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL);
            -- Designed so the primary sort by A leaves ties that get broken differently
            -- depending on whether ThenBy sees one or both members of the composite key.
            INSERT INTO TbckRow VALUES
                (1, 'x', 1, 2),
                (2, 'x', 1, 1),
                (3, 'x', 2, 3),
                (4, 'y', 1, 1);
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
    public async Task ThenBy_with_anonymous_composite_key_breaks_primary_ties_by_each_member()
    {
        // Primary OrderBy(A asc) groups: [x rows: 1,2,3], [y: 4].
        // ThenBy(new { B, C }) within x: B=1 ties → C asc: row 2 (C=1) then row 1 (C=2),
        // then B=2 row 3.
        // Final order: 2, 1, 3, 4.
        var rows = (await _ctx.Query<TbckRow>()
            .OrderBy(r => r.A)
            .ThenBy(r => new { r.B, r.C })
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 1, 3, 4 }, rows);
    }

    [Table("TbckRow")]
    public sealed class TbckRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
        public int C { get; set; }
    }
}
