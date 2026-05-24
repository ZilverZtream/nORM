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
/// (LeftShift / RightShift / Power). Pre-existing message was
/// "Operation 'Op 'LeftShift'' is not supported in this context" — vague,
/// doesn't tell the user what to do. Continues the actionable-message
/// series from 887ce1a / 4adca6e / 524f24f / e79d353.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqUnsupportedBinaryOpErrorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE UbRow (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);
            INSERT INTO UbRow VALUES (1, 4),(2, 8);
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
    public async Task LeftShift_in_where_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<UbRow>().Where(r => (r.Value << 1) > 5).ToListAsync();
        });
        // Must identify the operator and point at the supported multiplication workaround.
        Assert.Contains("LeftShift", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("multiply", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("UbRow")]
    public sealed class UbRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
