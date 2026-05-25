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
/// Strict pin for well-known DateTime / TimeSpan static constants in
/// WHERE: DateTime.MinValue/MaxValue/Today/UtcNow, TimeSpan.Zero. These
/// are static member accesses that the closure-fold should evaluate
/// to a constant DateTime/TimeSpan and bind as a parameter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeWellKnownConstantsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtwkItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL, Dur TEXT NOT NULL);
            INSERT INTO WdtwkItem VALUES
                (1, '2026-05-25 12:00:00', '00:00:00'),
                (2, '2030-01-01 00:00:00', '01:00:00'),
                (3, '1900-01-01 00:00:00', '00:30:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtwkItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_column_greater_than_DateTime_MinValue_returns_all_rows()
    {
        var ids = await _ctx.Query<WdtwkItem>()
            .Where(p => p.Stamp > DateTime.MinValue)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_column_less_than_DateTime_MaxValue_returns_all_rows()
    {
        var ids = await _ctx.Query<WdtwkItem>()
            .Where(p => p.Stamp < DateTime.MaxValue)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_column_greater_than_TimeSpan_Zero_returns_nonzero_rows()
    {
        var ids = await _ctx.Query<WdtwkItem>()
            .Where(p => p.Dur > TimeSpan.Zero)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdtwkItem")]
    public sealed class WdtwkItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Dur { get; set; }
    }
}
