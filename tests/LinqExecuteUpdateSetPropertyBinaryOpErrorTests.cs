using System;
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
/// Strict pin for bitshift operators inside a <c>SetProperty</c> value
/// expression of <c>ExecuteUpdateAsync</c>. Originally pinned as throw-or-
/// correct (must throw with actionable multiply-rewrite message) -- a
/// cop-out: all four supported providers accept native &lt;&lt; / &gt;&gt;.
/// Pin flipped to strict per implement-first feedback; sister to
/// LinqUnsupportedBinaryOpErrorTests and LinqProjectionBinaryOpErrorTests.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteUpdateSetPropertyBinaryOpErrorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EusRow (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);
            INSERT INTO EusRow VALUES (1, 4),(2, 8);
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
    public async Task LeftShift_in_set_property_value_doubles_each_row()
    {
        var affected = await ((INormQueryable<EusRow>)_ctx.Query<EusRow>())
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Value, r => r.Value << 1));
        Assert.Equal(2, affected);

        // Verify the UPDATE landed: Id 1: 4 -> 8, Id 2: 8 -> 16.
        var values = await _ctx.Query<EusRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Value })
            .ToListAsync();
        Assert.Equal(new[] { 8, 16 }, values.Select(v => v.Value).ToArray());
    }

    [Table("EusRow")]
    public sealed class EusRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
