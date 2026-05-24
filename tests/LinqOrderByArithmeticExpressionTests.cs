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
/// Pins <c>OrderBy(p =&gt; p.A * p.B + C)</c> — arithmetic expression as the
/// sort key. SQL should emit `ORDER BY (A * B + C)` against the entity
/// columns. Silent-wrongness risk if the expression is partially translated
/// (e.g. only the leftmost column survives) or evaluated client-side after
/// LIMIT.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByArithmeticExpressionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OaeRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);
            -- Product A*B values: row 1 = 6, row 2 = 12, row 3 = 4, row 4 = 9, row 5 = 1.
            -- Sorted ASC by A*B: rows 5(1), 3(4), 1(6), 4(9), 2(12) → Ids [5,3,1,4,2].
            INSERT INTO OaeRow VALUES (1,2,3),(2,4,3),(3,2,2),(4,3,3),(5,1,1);
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
    public async Task OrderBy_arithmetic_product_sorts_by_computed_value()
    {
        var ids = (await _ctx.Query<OaeRow>()
            .OrderBy(p => p.A * p.B)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 3, 1, 4, 2 }, ids);
    }

    [Fact]
    public async Task OrderByDescending_arithmetic_with_offset_sorts_by_full_expression()
    {
        // (A * B) + 1 values: row 1=7, 2=13, 3=5, 4=10, 5=2.
        // DESC: rows 2(13), 4(10), 1(7), 3(5), 5(2) → Ids [2,4,1,3,5].
        var ids = (await _ctx.Query<OaeRow>()
            .OrderByDescending(p => p.A * p.B + 1)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4, 1, 3, 5 }, ids);
    }

    [Table("OaeRow")]
    public sealed class OaeRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }
}
