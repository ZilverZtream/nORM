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
/// Verify pin for the ParameterValueExtractor alignment audit (407e03d /
/// eeff6e7 / cf39b61 / 04a0003 / 7d6d7ac / c6c4710): a Where predicate
/// with TWO closure-captured DateTimeOffset locals -- one in a range
/// lower-bound and one as an extra equality RHS -- to verify the
/// alignment doesn't silently shift either binding.
///
/// DTO is a non-primitive struct so the bind path differs from int/string
/// captures; this pin guards against a DTO-specific regression where the
/// extractor might walk a DateTimeOffset closure differently.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeOffsetClosurePairTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtocpItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL, Tag TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var anchor = new DateTimeOffset(2026, 5, 24, 12, 0, 0, TimeSpan.Zero);
        var rows = new (int id, DateTimeOffset dto, string tag)[]
        {
            (1, anchor.AddDays(-10), "red"),
            (2, anchor.AddDays(-1),  "red"),
            (3, anchor.AddDays(1),   "blue"),
            (4, anchor.AddDays(5),   "red"),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WdtocpItem (Id, Stamp, Tag) VALUES ($id, $s, $t)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        var pt = insert.CreateParameter(); pt.ParameterName = "$t"; insert.Parameters.Add(pt);
        foreach (var (id, dto, tag) in rows)
        {
            pid.Value = id;
            ps.Value = dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", System.Globalization.CultureInfo.InvariantCulture);
            pt.Value = tag;
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtocpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTimeOffset_closure_AND_tag_closure_filters_correctly()
    {
        // Lower-bound closure (anchor) AND tag closure. Stamp > anchor (mid)
        // AND Tag == tagWanted -> {Id 4} (only future red row).
        var lowerBound = new DateTimeOffset(2026, 5, 24, 12, 0, 0, TimeSpan.Zero);
        var tagWanted = "red";
        var result = await _ctx.Query<WdtocpItem>()
            .Where(p => p.Stamp > lowerBound && p.Tag == tagWanted)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WdtocpItem")]
    public sealed class WdtocpItem
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
