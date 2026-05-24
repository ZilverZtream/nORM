using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies LongCountAsync executes server-side and returns a 64-bit total. The docs claim
/// support for it as the 64-bit equivalent of CountAsync, so this test pins the API surface
/// and the round-trip behaviour against real rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqLongCountAsyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LcRow (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL);
            INSERT INTO LcRow VALUES (1,'a'),(2,'b'),(3,'a'),(4,'b'),(5,'a');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task LongCountAsync_returns_64_bit_total_for_unfiltered_query()
    {
        long total = await _ctx.Query<LcRow>().LongCountAsync();
        Assert.Equal(5L, total);
    }

    [Fact]
    public async Task LongCountAsync_with_filter_returns_only_matching_rows()
    {
        long count = await _ctx.Query<LcRow>().Where(r => r.Tag == "a").LongCountAsync();
        Assert.Equal(3L, count);
    }

    [Table("LcRow")]
    public sealed class LcRow
    {
        [Key] public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
