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
/// Probe pin for `AverageAsync(decimal?)` over an all-null source.
/// LINQ-to-Objects semantics: `Enumerable.Average(IEnumerable&lt;decimal?&gt;)`
/// returns null when every element is null (or source is empty). Non-
/// nullable `Average(IEnumerable&lt;decimal&gt;)` throws InvalidOperationException
/// on empty source.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAverageNullableDecimalAllNullTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AndaItem (Id INTEGER PRIMARY KEY, V TEXT NULL);
            INSERT INTO AndaItem VALUES (1, NULL), (2, NULL), (3, NULL);
            CREATE TABLE AndaEmpty (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<AndaItem>().HasKey(i => i.Id);
                mb.Entity<AndaEmpty>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task AverageAsync_nullable_decimal_all_null_rows_returns_null()
    {
        var avg = await _ctx.Query<AndaItem>().AverageAsync(p => p.V);
        // LINQ: Average over IEnumerable<decimal?> with all nulls -> null.
        Assert.Null(avg);
    }

    [Fact]
    public async Task AverageAsync_non_nullable_decimal_empty_source_throws()
    {
        // LINQ: Average over empty IEnumerable<decimal> -> InvalidOperationException.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _ctx.Query<AndaEmpty>().AverageAsync(p => p.V));
    }

    [Table("AndaItem")]
    public sealed class AndaItem
    {
        [Key] public int Id { get; set; }
        public decimal? V { get; set; }
    }

    [Table("AndaEmpty")]
    public sealed class AndaEmpty
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
