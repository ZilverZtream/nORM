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
/// Strict pin for <c>Nullable&lt;DateTime&gt;.GetValueOrDefault(default)</c>
/// with a closure-captured DateTime default in projection. Should lower
/// to COALESCE(col, default-literal).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionNullableGetValueOrDefaultTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PngvdItem (Id INTEGER PRIMARY KEY, Stamp TEXT);
            INSERT INTO PngvdItem VALUES
                (1, '2026-05-25 12:00:00'),
                (2, NULL),
                (3, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PngvdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Nullable_DateTime_GetValueOrDefault_returns_fallback_for_null_per_row()
    {
        var fallback = new DateTime(2000, 1, 1);
        var r = await _ctx.Query<PngvdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.GetValueOrDefault(fallback) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(new DateTime(2026, 5, 25, 12, 0, 0), r[0].D);
        Assert.Equal(fallback, r[1].D);
        Assert.Equal(new DateTime(2026, 12, 31, 23, 59, 59), r[2].D);
    }

    [Table("PngvdItem")]
    public sealed class PngvdItem
    {
        [Key] public int Id { get; set; }
        public DateTime? Stamp { get; set; }
    }
}
