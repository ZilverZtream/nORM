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
/// Pins projection-side COALESCE — <c>Select(x =&gt; new { Val = x.A ?? 0 })</c>.
/// SelectClauseVisitor builds the projection SELECT via a different code path
/// than predicate / aggregate visitors; if it doesn't merge sub-visitor
/// parameters as literals the same parameter-binding bug shows up.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectCoalesceProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SpRow (Id INTEGER PRIMARY KEY, Amount INTEGER NULL, Name TEXT NULL);
            INSERT INTO SpRow VALUES
                (1, 10,   'a'),
                (2, NULL, 'b'),
                (3, 30,   NULL),
                (4, NULL, NULL);
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
    public async Task Anonymous_projection_with_int_coalesce_substitutes_zero_for_null()
    {
        var rows = (await _ctx.Query<SpRow>()
            .Select(r => new { r.Id, Val = r.Amount ?? 0 })
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Equal(10, rows[0].Val);
        Assert.Equal(0,  rows[1].Val);
        Assert.Equal(30, rows[2].Val);
        Assert.Equal(0,  rows[3].Val);
    }

    [Fact]
    public async Task Anonymous_projection_with_string_coalesce_substitutes_label_for_null()
    {
        var rows = (await _ctx.Query<SpRow>()
            .Select(r => new { r.Id, Label = r.Name ?? "anon" })
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Equal("a",    rows[0].Label);
        Assert.Equal("b",    rows[1].Label);
        Assert.Equal("anon", rows[2].Label);
        Assert.Equal("anon", rows[3].Label);
    }

    [Table("SpRow")]
    public sealed class SpRow
    {
        [Key] public int Id { get; set; }
        public int? Amount { get; set; }
        public string? Name { get; set; }
    }
}
