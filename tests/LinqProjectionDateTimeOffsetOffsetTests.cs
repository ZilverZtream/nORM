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
/// Strict pin + implement-first for <c>DateTimeOffset.Offset</c> property
/// in projection. Returns the offset as a TimeSpan. Completes the DTO
/// property trio (UtcDateTime de1f5f3, DateTime 21cb2ec, Offset here).
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Returns the full DTO text -> materializer parse error.
///   * Sign-flip lost -> negative offsets read as positive.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeOffsetOffsetTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtooItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var rows = new (int id, DateTimeOffset dto)[]
        {
            (1, new DateTimeOffset(2026, 5, 24, 12, 0, 0, TimeSpan.Zero)),
            (2, new DateTimeOffset(2026, 5, 24, 14, 0, 0, TimeSpan.FromHours(2))),
            (3, new DateTimeOffset(2026, 5, 24,  7, 0, 0, TimeSpan.FromHours(-5))),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PdtooItem (Id, Stamp) VALUES ($id, $s)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        foreach (var (id, dto) in rows)
        {
            pid.Value = id;
            ps.Value = dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", System.Globalization.CultureInfo.InvariantCulture);
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtooItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTimeOffset_Offset_projects_TimeSpan_per_row()
    {
        var result = await _ctx.Query<PdtooItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, O = p.Stamp.Offset })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(TimeSpan.Zero, result[0].O);
        Assert.Equal(TimeSpan.FromHours(2), result[1].O);
        Assert.Equal(TimeSpan.FromHours(-5), result[2].O);
    }

    [Table("PdtooItem")]
    public sealed class PdtooItem
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
