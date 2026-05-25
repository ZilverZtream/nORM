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
/// Strict pin for <c>TimeSpan.FromHours / FromMinutes / FromSeconds /
/// FromDays(constant)</c> appearing inline in a WHERE predicate. The
/// arg is a literal, so the entire MethodCall is a foldable constant
/// expression that the closure-fold should evaluate to a TimeSpan
/// value and bind as a parameter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereTimeSpanFromHoursConstantTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WtsfhItem (Id INTEGER PRIMARY KEY, Dur TEXT NOT NULL);
            INSERT INTO WtsfhItem VALUES
                (1, '00:30:00'),
                (2, '01:30:00'),
                (3, '02:30:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WtsfhItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_column_greater_than_TimeSpan_FromHours_constant_returns_longer_durations()
    {
        var ids = await _ctx.Query<WtsfhItem>()
            .Where(p => p.Dur > TimeSpan.FromHours(1))
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_column_equals_TimeSpan_FromMinutes_constant_returns_exact_match()
    {
        var ids = await _ctx.Query<WtsfhItem>()
            .Where(p => p.Dur == TimeSpan.FromMinutes(90))
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WtsfhItem")]
    public sealed class WtsfhItem
    {
        [Key] public int Id { get; set; }
        public TimeSpan Dur { get; set; }
    }
}
