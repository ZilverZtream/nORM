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
/// Pins <c>OrderBy(x).Reverse().Take(n)</c> — Reverse + Take composition.
/// Semantics: Reverse first flips the sort, then Take pulls the top-N from
/// the flipped order (equivalent to OrderByDescending(x).Take(n)).
/// A naive implementation that LIMITs before flipping the direction would
/// return the wrong rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqReverseTakeInteractionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE RtRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO RtRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
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
    public async Task OrderBy_then_reverse_then_take_returns_top_n_in_descending_order()
    {
        // OrderBy(Id ASC) → 1,2,3,4,5. Reverse → 5,4,3,2,1. Take(3) → 5,4,3.
        var ids = (await _ctx.Query<RtRow>()
            .OrderBy(r => r.Id)
            .Reverse()
            .Take(3)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 4, 3 }, ids);
    }

    [Table("RtRow")]
    public sealed class RtRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
