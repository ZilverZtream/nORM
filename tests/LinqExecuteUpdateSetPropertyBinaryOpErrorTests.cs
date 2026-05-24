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
/// Pins the actionable error message for unsupported binary operators
/// inside a <c>SetProperty</c> value expression of <c>ExecuteUpdateAsync</c>.
/// Third place this pattern surfaces — completes the trio after fcb4199
/// (WHERE side) and b846b0c (projection side).
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
    public async Task LeftShift_in_set_property_value_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await ((INormQueryable<EusRow>)_ctx.Query<EusRow>())
                .ExecuteUpdateAsync(s => s.SetProperty(r => r.Value, r => r.Value << 1));
        });
        Assert.Contains("LeftShift", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("multiply",  ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("EusRow")]
    public sealed class EusRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
