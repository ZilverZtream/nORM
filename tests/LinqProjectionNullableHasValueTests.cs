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
/// Strict pin for <c>Nullable&lt;T&gt;.HasValue</c> in projection -- emit
/// IS NOT NULL on the underlying column. ETSV already handles this in
/// WHERE; the projection mirror is needed for callers projecting a
/// boolean flag column.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionNullableHasValueTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PnhvItem (Id INTEGER PRIMARY KEY, Stamp TEXT);
            INSERT INTO PnhvItem VALUES
                (1, '2026-05-25 12:00:00'),
                (2, NULL),
                (3, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PnhvItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Nullable_DateTime_HasValue_returns_correct_flag_per_row()
    {
        var r = await _ctx.Query<PnhvItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, F = p.Stamp.HasValue }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].F);
        Assert.False(r[1].F);
        Assert.True(r[2].F);
    }

    [Table("PnhvItem")]
    public sealed class PnhvItem
    {
        [Key] public int Id { get; set; }
        public DateTime? Stamp { get; set; }
    }
}
