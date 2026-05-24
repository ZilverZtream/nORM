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
/// Pins <c>OrderBy(x).Reverse().Skip(n)</c>. LINQ semantics: OrderBy(x) gives
/// ASC order, Reverse flips to DESC, Skip(n) drops the first n of the
/// reversed sequence. SQL idiom: <c>ORDER BY x DESC OFFSET n</c>. Silent
/// wrongness if Reverse is dropped or Skip is applied before the reverse.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByReverseSkipCompositionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OrsRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO OrsRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
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
    public async Task OrderBy_then_reverse_then_skip_drops_first_n_from_reversed_sequence()
    {
        // OrderBy(Id ASC) → 1,2,3,4,5. Reverse → 5,4,3,2,1. Skip(2) → 3,2,1.
        var ids = (await _ctx.Query<OrsRow>()
            .OrderBy(r => r.Id)
            .Reverse()
            .Skip(2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 2, 1 }, ids);
    }

    [Fact]
    public async Task OrderBy_then_reverse_then_skip_then_take_returns_window_of_reversed_sequence()
    {
        // OrderBy(Id ASC) → 1,2,3,4,5. Reverse → 5,4,3,2,1. Skip(1).Take(2) → 4,3.
        var ids = (await _ctx.Query<OrsRow>()
            .OrderBy(r => r.Id)
            .Reverse()
            .Skip(1)
            .Take(2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4, 3 }, ids);
    }

    [Table("OrsRow")]
    public sealed class OrsRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
