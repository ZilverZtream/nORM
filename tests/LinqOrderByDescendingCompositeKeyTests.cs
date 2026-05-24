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
/// Pins <c>OrderByDescending(r =&gt; new { r.A, r.B })</c> — composite anonymous
/// key with the descending direction. The 560dd16 OrderByTranslator fix
/// expanded the NewExpression body into one ORDER BY entry per member and
/// propagated the ascending/descending choice across all of them; this
/// regression locks in that descending applies to BOTH columns rather than
/// only the first.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByDescendingCompositeKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OdckRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL);
            -- Designed so applying descending to ONLY the first column would
            -- produce a different sequence than applying it to both.
            INSERT INTO OdckRow VALUES
                (1, 'x', 2),
                (2, 'x', 1),
                (3, 'y', 3),
                (4, 'y', 1);
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
    public async Task OrderByDescending_anonymous_composite_key_sorts_both_members_descending()
    {
        // Expected (A desc, B desc):
        //   y/3 (Id 3), y/1 (Id 4), x/2 (Id 1), x/1 (Id 2).
        // If descending only applied to A, the within-A order would default to
        // SQL natural / insertion (effectively ascending for B): 3, 4, 2, 1 ascending B.
        // Locking the full descending semantics in either case.
        var rows = (await _ctx.Query<OdckRow>()
            .OrderByDescending(r => new { r.A, r.B })
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 4, 1, 2 }, rows);
    }

    [Table("OdckRow")]
    public sealed class OdckRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
    }
}
