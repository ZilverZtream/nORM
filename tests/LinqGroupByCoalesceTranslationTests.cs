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
/// Pins translation of computed key expressions inside <c>GroupBy</c> — the
/// parallel of the OrderBy COALESCE bug from c46beb9. A key like
/// <c>r.Region ?? "Unknown"</c> allocates a parameter for the fallback string;
/// the GroupBy translator must merge those parameters back into the outer
/// translator so the resulting SQL has all referenced parameters bound.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupByCoalesceTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GcRow (Id INTEGER PRIMARY KEY, Region TEXT NULL, Amount INTEGER NOT NULL);
            INSERT INTO GcRow VALUES
                (1, 'NA',   100),
                (2, 'EMEA', 200),
                (3, NULL,   50),
                (4, 'NA',   150),
                (5, NULL,   75);
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
    public async Task GroupBy_with_coalesce_fallback_buckets_nulls_under_label()
    {
        // Group by (Region ?? "Unknown"). Expected: NA → 250, EMEA → 200, Unknown → 125.
        var groups = (await _ctx.Query<GcRow>()
            .GroupBy(r => r.Region ?? "Unknown")
            .Select(g => new { Bucket = g.Key, Total = g.Sum(r => r.Amount) })
            .ToListAsync())
            .OrderBy(g => g.Bucket).ToArray();

        Assert.Equal(3, groups.Length);
        Assert.Equal(("EMEA", 200),    (groups[0].Bucket, groups[0].Total));
        Assert.Equal(("NA",   250),    (groups[1].Bucket, groups[1].Total));
        Assert.Equal(("Unknown", 125), (groups[2].Bucket, groups[2].Total));
    }

    [Table("GcRow")]
    public sealed class GcRow
    {
        [Key] public int Id { get; set; }
        public string? Region { get; set; }
        public int Amount { get; set; }
    }
}
