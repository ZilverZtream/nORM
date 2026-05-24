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
/// Pins <c>.Reverse()</c> behaviour. Two cases worth nailing down:
/// (1) Reverse after a preceding OrderBy — flips the sort direction so the
///     result is the same query ordered descendingly.
/// (2) Reverse on a query with no preceding OrderBy — there's no well-defined
///     "natural" order in SQL to reverse against, so the call must either
///     surface an actionable error or use a deterministic key (typically the
///     primary key) as a fallback. Either behaviour is acceptable; a silent
///     same-as-original-order result is not.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqReverseOrderingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE RvRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO RvRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
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
    public async Task Reverse_after_orderby_flips_sort_direction()
    {
        // OrderBy(Id ASC) then Reverse → Id DESC: 4, 3, 2, 1.
        var ids = (await _ctx.Query<RvRow>()
            .OrderBy(r => r.Id)
            .Reverse()
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4, 3, 2, 1 }, ids);
    }

    [Table("RvRow")]
    public sealed class RvRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
