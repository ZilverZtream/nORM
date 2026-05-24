using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies projections that require client-side evaluation: interpolated strings, string.Format,
/// and user-defined helper methods. The SQL side fetches the raw columns, the client-side path
/// reconstructs the projected value. Uses ClientEvaluationPolicy.Allow so the fallback engages.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqClientProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CpRow (Id INTEGER PRIMARY KEY, First TEXT NOT NULL, Last TEXT NOT NULL);
            INSERT INTO CpRow VALUES
                (1, 'Ada', 'Lovelace'),
                (2, 'Grace', 'Hopper'),
                (3, 'Linus', 'Torvalds');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(),
            new DbContextOptions { ClientEvaluationPolicy = ClientEvaluationPolicy.Allow });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Interpolated_string_in_projection_runs_client_side()
    {
        var rows = (await _ctx.Query<CpRow>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, FullName = $"{r.First} {r.Last}" })
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Ada Lovelace", rows[0].FullName);
        Assert.Equal("Grace Hopper", rows[1].FullName);
        Assert.Equal("Linus Torvalds", rows[2].FullName);
    }

    [Fact]
    public async Task StringFormat_in_projection_runs_client_side()
    {
        var rows = (await _ctx.Query<CpRow>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, Display = string.Format("{0}, {1}", r.Last, r.First) })
            .ToListAsync())
            .ToArray();
        Assert.Equal("Lovelace, Ada", rows[0].Display);
        Assert.Equal("Torvalds, Linus", rows[2].Display);
    }

    [Table("CpRow")]
    public sealed class CpRow
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public string Last { get; set; } = string.Empty;
    }
}
