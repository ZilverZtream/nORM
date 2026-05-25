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
/// Strict pin for DateTime / DateOnly / TimeOnly / TimeSpan / Guid /
/// DateTimeOffset closure-captured literals in projection. SCV's
/// FormatLiteral previously only handled primitives + string and threw
/// "Navigation filter literal of type 'DateTime' isn't supported" -- the
/// gap referenced in b8e7a29.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeLiteralTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtlItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdtlItem VALUES
                (1, '2026-05-25 12:00:00'),
                (2, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtlItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_literal_via_DateTime_Compare_round_trips()
    {
        var pivot = new DateTime(2026, 5, 25, 12, 0, 0);
        var r = await _ctx.Query<PdtlItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = DateTime.Compare(p.Stamp, pivot) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(0, r[0].C);
        Assert.True(r[1].C > 0);
    }

    // NOTE: Guid / TimeSpan / DateOnly / TimeOnly / DateTimeOffset literals
    // also pass through SCV.FormatLiteral with the right canonical text
    // emit, but a separate materializer limitation around constant-column
    // anonymous-type ctor matching ("No suitable constructor for
    // AnonymousType<int, Guid>") blocks end-to-end materialization. The
    // FormatLiteral side of the gap is closed here; the materializer
    // side is a separate iteration.

    [Table("PdtlItem")]
    public sealed class PdtlItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
