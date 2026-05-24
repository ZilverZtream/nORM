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
/// (LeftShift / RightShift / Power) inside a SELECT projection. Companion
/// to the WHERE-side test from fcb4199 — the projection path goes through
/// SelectClauseVisitor whose unsupported-operator throw was also vague.
/// Continues the actionable-message series.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionBinaryOpErrorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PbRow (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);
            INSERT INTO PbRow VALUES (1, 4),(2, 8);
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
    public async Task LeftShift_in_projection_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<PbRow>().Select(r => new { r.Id, Doubled = r.Value << 1 }).ToListAsync();
        });
        // Must identify the operator and point at the supported multiply workaround.
        Assert.Contains("LeftShift", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("multiply",  ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("PbRow")]
    public sealed class PbRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
