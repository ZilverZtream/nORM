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
/// DateTime.UtcNow / DateTime.Now / DateTime.Today must evaluate to client-side constants the
/// visitor inlines as a parameter — using them in a Where predicate is one of the most common
/// LINQ shapes for "everything created before today / after now". Asserts the comparison
/// resolves on the server, not by walking every row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeNowTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtnRow (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO DtnRow VALUES
                (1, '2000-01-01 00:00:00'),
                (2, '2100-01-01 00:00:00');
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
    public async Task UtcNow_in_where_compares_against_server_current_time()
    {
        var ids = (await _ctx.Query<DtnRow>().Where(r => r.Stamp < DateTime.UtcNow).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Now_in_where_compares_against_local_time()
    {
        var ids = (await _ctx.Query<DtnRow>().Where(r => r.Stamp > DateTime.Now).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public async Task Today_in_where_uses_midnight_of_current_date()
    {
        var ids = (await _ctx.Query<DtnRow>().Where(r => r.Stamp < DateTime.Today).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Table("DtnRow")]
    public sealed class DtnRow
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
