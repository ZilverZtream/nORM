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
/// Probe + strict pin for <c>DateTimeOffset.UtcDateTime</c> in projection.
/// .NET semantics: returns a DateTime in UTC, with the offset applied
/// (subtracted to get the UTC instant). Storage in Microsoft.Data.Sqlite
/// is canonical 'yyyy-MM-dd HH:mm:ss.FFFFFFFzzz' text. The materializer
/// needs to parse the column's stored text and apply the offset to
/// produce a UTC DateTime.
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Returns local-clock DateTime (ignoring the offset) -- WRONG by
///     the offset's magnitude every row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeOffsetUtcDateTimeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtouItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Use Microsoft.Data.Sqlite's format so the column round-trips
        // through the materializer correctly (848eb93 documented this).
        var rows = new (int id, DateTimeOffset dto)[]
        {
            (1, new DateTimeOffset(2026, 5, 24, 12, 0, 0, TimeSpan.Zero)),       // UTC
            (2, new DateTimeOffset(2026, 5, 24, 14, 0, 0, TimeSpan.FromHours(2))), // == 12:00 UTC
            (3, new DateTimeOffset(2026, 5, 24, 7, 0, 0, TimeSpan.FromHours(-5))), // == 12:00 UTC
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PdtouItem (Id, Stamp) VALUES ($id, $s)";
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
            OnModelCreating = mb => mb.Entity<PdtouItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTimeOffset_UtcDateTime_projects_normalized_utc_instant()
    {
        var result = await _ctx.Query<PdtouItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, U = p.Stamp.UtcDateTime })
            .ToListAsync();
        var expected = new DateTime(2026, 5, 24, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal(3, result.Count);
        Assert.Equal(expected, result[0].U);
        Assert.Equal(expected, result[1].U);
        Assert.Equal(expected, result[2].U);
    }

    [Table("PdtouItem")]
    public sealed class PdtouItem
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
