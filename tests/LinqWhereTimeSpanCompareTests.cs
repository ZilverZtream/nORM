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
/// Verify pin for <c>TimeSpan.Compare(a, b)</c> in WHERE -- sister to
/// the projection fix in 9ae9dab. ETSV routes through TranslateFunction
/// so the typeof(TimeSpan) Compare entry should cover the WHERE path
/// for free; pin guards against future regressions.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereTimeSpanCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WtscItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO WtscItem VALUES
                (1, '02:30:00', '01:00:00'),
                (2, '05:15:00', '05:15:00'),
                (3, '00:45:00', '03:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WtscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_TimeSpan_Compare_greater_than_zero_returns_only_a_longer_rows()
    {
        var ids = await _ctx.Query<WtscItem>()
            .Where(p => TimeSpan.Compare(p.A, p.B) > 0)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WtscItem")]
    public sealed class WtscItem
    {
        [Key] public int Id { get; set; }
        public TimeSpan A { get; set; }
        public TimeSpan B { get; set; }
    }
}
