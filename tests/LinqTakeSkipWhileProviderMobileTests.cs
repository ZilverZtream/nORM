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
/// Ordered TakeWhile/SkipWhile lower to a windowed cumulative break flag. The
/// unordered forms remain unsupported because relational sources have no stable
/// enumeration order without OrderBy.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class LinqTakeSkipWhileProviderMobileTests : IAsyncLifetime
{
    private SqliteConnection _connection = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _connection = new SqliteConnection("Data Source=:memory:");
        await _connection.OpenAsync();
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WhileRow (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO WhileRow VALUES
                (1, 'a', 10),
                (2, 'a', 20),
                (3, 'a', 30),
                (4, 'b', 40),
                (5, 'a', 10);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_connection, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _connection.DisposeAsync();
    }

    [Fact]
    public async Task TakeWhile_after_OrderBy_returns_prefix_before_first_false_row()
    {
        var ids = (await _ctx.Query<WhileRow>()
            .OrderBy(r => r.Id)
            .TakeWhile(r => r.Score < 40)
            .ToListAsync())
            .Select(r => r.Id)
            .ToArray();

        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public async Task SkipWhile_after_OrderBy_returns_suffix_starting_at_first_false_row()
    {
        var ids = (await _ctx.Query<WhileRow>()
            .OrderBy(r => r.Id)
            .SkipWhile(r => r.Score < 40)
            .ToListAsync())
            .Select(r => r.Id)
            .ToArray();

        Assert.Equal(new[] { 4, 5 }, ids);
    }

    [Fact]
    public async Task Index_aware_TakeWhile_after_OrderBy_uses_zero_based_sequence_index()
    {
        var ids = (await _ctx.Query<WhileRow>()
            .OrderBy(r => r.Id)
            .TakeWhile((r, index) => index < 3 && r.Score < 40)
            .ToListAsync())
            .Select(r => r.Id)
            .ToArray();

        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public async Task Index_aware_SkipWhile_after_OrderBy_uses_zero_based_sequence_index()
    {
        var ids = (await _ctx.Query<WhileRow>()
            .OrderBy(r => r.Id)
            .SkipWhile((r, index) => index < 2)
            .ToListAsync())
            .Select(r => r.Id)
            .ToArray();

        Assert.Equal(new[] { 3, 4, 5 }, ids);
    }

    [Fact]
    public async Task TakeWhile_requires_explicit_order()
    {
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
            await _ctx.Query<WhileRow>().TakeWhile(r => r.Score < 40).ToListAsync());

        Assert.Contains("requires an explicit OrderBy", ex.Message, StringComparison.Ordinal);
    }

    [Table("WhileRow")]
    private sealed class WhileRow
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
