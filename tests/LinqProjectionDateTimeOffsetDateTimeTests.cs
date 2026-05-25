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
/// Strict pin + implement-first for <c>DateTimeOffset.DateTime</c> in
/// projection. .NET semantics: returns the wall-clock DateTime portion
/// IGNORING the offset (Kind=Unspecified). Sister to de1f5f3's
/// UtcDateTime. Storage format is canonical 'yyyy-MM-dd HH:mm:ss
/// [.FFFFFFF]zzz' (6-char trailing offset).
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval / throw.
///   * Returns the UTC-normalized DateTime (the UtcDateTime semantic)
///     -- WRONG for rows with non-zero offset.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeOffsetDateTimeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtotItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var rows = new (int id, DateTimeOffset dto)[]
        {
            (1, new DateTimeOffset(2026, 5, 24, 12, 0, 0, TimeSpan.Zero)),
            (2, new DateTimeOffset(2026, 5, 24, 14, 0, 0, TimeSpan.FromHours(2))),
            (3, new DateTimeOffset(2026, 5, 24,  7, 0, 0, TimeSpan.FromHours(-5))),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PdtotItem (Id, Stamp) VALUES ($id, $s)";
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
            OnModelCreating = mb => mb.Entity<PdtotItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTimeOffset_DateTime_projects_local_clock_per_row()
    {
        var result = await _ctx.Query<PdtotItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.DateTime })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        // Wall-clock DateTime ignores the offset.
        Assert.Equal(new DateTime(2026, 5, 24, 12, 0, 0), result[0].D);
        Assert.Equal(new DateTime(2026, 5, 24, 14, 0, 0), result[1].D);
        Assert.Equal(new DateTime(2026, 5, 24,  7, 0, 0), result[2].D);
    }

    [Table("PdtotItem")]
    public sealed class PdtotItem
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
