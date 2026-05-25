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
/// Probe pin for `SumAsync(decimal?)` over a nullable decimal column
/// when every row's value is NULL. LINQ-to-Objects semantics:
/// `Enumerable.Sum(IEnumerable&lt;decimal?&gt;)` returns 0 when the source
/// is empty or all-null. SQL `SUM(col)` returns NULL on the same input,
/// so without scalar adaptation nORM either returns null (different from
/// LINQ) or throws (worse). Verify the materialized result matches LINQ.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSumNullableDecimalAllNullTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SndaItem (Id INTEGER PRIMARY KEY, V TEXT NULL);
            INSERT INTO SndaItem VALUES (1, NULL), (2, NULL), (3, NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SndaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SumAsync_nullable_decimal_all_null_rows_matches_linq_zero()
    {
        var sum = await _ctx.Query<SndaItem>().SumAsync(p => p.V);
        // LINQ: Enumerable.Sum(IEnumerable<decimal?>) returns 0 for all-null source.
        Assert.Equal(0m, sum);
    }

    [Table("SndaItem")]
    public sealed class SndaItem
    {
        [Key] public int Id { get; set; }
        public decimal? V { get; set; }
    }
}
