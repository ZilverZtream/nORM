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
/// Pins <c>OrderBy(x =&gt; new { x.A, x.B })</c> — composite anonymous-type
/// key. C# users often write this as a shortcut for
/// `.OrderBy(x =&gt; x.A).ThenBy(x =&gt; x.B)`. The translator either has to
/// expand it into multiple ORDER BY clauses or throw with guidance pointing
/// at the explicit ThenBy chain. Either is acceptable; a silent crash or
/// wrong ordering is not.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByAnonymousCompositeKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OackRow (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL);
            -- Designed so a wrong order (single-key OrderBy on A only) returns a different
            -- sequence than the correct (A asc, B asc) composite sort.
            INSERT INTO OackRow VALUES
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
    public async Task OrderBy_anonymous_composite_key_sorts_by_each_member_in_declaration_order()
    {
        // Expected ordering by (A asc, B asc) → Ids 2, 1, 4, 3.
        var rows = (await _ctx.Query<OackRow>()
            .OrderBy(r => new { r.A, r.B })
            .ToListAsync())
            .Select(r => r.Id).ToArray();

        Assert.Equal(new[] { 2, 1, 4, 3 }, rows);
    }

    [Table("OackRow")]
    public sealed class OackRow
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public int B { get; set; }
    }
}
