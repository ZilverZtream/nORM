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
/// Strict pin for the bit-shift binary operators in Where. Originally
/// pinned as throw-or-correct ('throws with actionable multiply
/// workaround') -- a cop-out: SQLite, SQL Server, MySQL, and PostgreSQL
/// all support &lt;&lt; / &gt;&gt; natively. Pin flipped to strict per
/// the implement-first feedback. Sister to the projection-side flip in
/// LinqProjectionBinaryOpErrorTests.
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
    public async Task LeftShift_in_where_filters_rows_strictly()
    {
        // (Value << 1) > 5 -> rows where Value > 2 -> {1 (Value 4), 2 (Value 8)}.
        var result = await _ctx.Query<UbRow>()
            .Where(r => (r.Value << 1) > 5)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task RightShift_in_where_filters_rows_strictly()
    {
        // (Value >> 1) >= 3 -> rows where Value >= 6 -> {2 (Value 8)}.
        var result = await _ctx.Query<UbRow>()
            .Where(r => (r.Value >> 1) >= 3)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("UbRow")]
    public sealed class UbRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
