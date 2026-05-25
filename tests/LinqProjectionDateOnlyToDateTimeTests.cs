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
/// Strict pin for <c>DateOnly.ToDateTime(TimeOnly)</c> in projection --
/// combine a DateOnly column with a TimeOnly column into a DateTime.
/// SQLite stores both as canonical text so the emit is string concat
/// with a space separator; the materializer parses the result.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateOnlyToDateTimeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdotdtItem (Id INTEGER PRIMARY KEY, D TEXT NOT NULL, T TEXT NOT NULL);
            INSERT INTO PdotdtItem VALUES
                (1, '2026-05-25', '14:30:00'),
                (2, '2026-12-31', '23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdotdtItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateOnly_ToDateTime_TimeOnly_returns_combined_DateTime_per_row()
    {
        var r = await _ctx.Query<PdotdtItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Dt = p.D.ToDateTime(p.T) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 5, 25, 14, 30, 0), r[0].Dt);
        Assert.Equal(new DateTime(2026, 12, 31, 23, 59, 59), r[1].Dt);
    }

    [Table("PdotdtItem")]
    public sealed class PdotdtItem
    {
        [Key] public int Id { get; set; }
        public DateOnly D { get; set; }
        public TimeOnly T { get; set; }
    }
}
