using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

namespace nORM.Tests;

/// <summary>
/// Diagnostic probes for the DistinctBy implementation. Surveys which
/// "distinct by key" rewrite patterns nORM already supports so the actual
/// DistinctBy translator can layer on top of one rather than emit a new
/// SQL shape from scratch. Each probe runs the candidate pattern and
/// reports outcome (translates / throws / silent-wrongness).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProbeDistinctByRewriteCandidatesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private readonly ITestOutputHelper _output;

    public LinqProbeDistinctByRewriteCandidatesTests(ITestOutputHelper output) => _output = output;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ProbeItem (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO ProbeItem VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<ProbeItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Probe_GroupBy_then_Select_key_only_returns_distinct_keys()
    {
        // Baseline: q.GroupBy(k).Select(g => g.Key) is the standard
        // "distinct keys" pattern. Should return {"A", "B"}.
        string[]? keys = null;
        System.Exception? ex = null;
        try
        {
            var result = await _ctx.Query<ProbeItem>()
                .GroupBy(i => i.Category)
                .Select(g => g.Key)
                .OrderBy(k => k)
                .ToListAsync();
            keys = result.ToArray();
            _output.WriteLine($"GroupBy.Select(g => g.Key) -> [{string.Join(", ", keys)}]");
        }
        catch (System.Exception caught)
        {
            ex = caught;
            _output.WriteLine($"GroupBy.Select(g => g.Key) THREW: {ex.GetType().Name}: {ex.Message}");
        }

        Assert.Null(ex);
        Assert.Equal(new[] { "A", "B" }, keys);
    }

    [Fact]
    public async Task Probe_Select_then_Distinct_on_key_returns_distinct_keys()
    {
        // Alternative pattern: q.Select(k).Distinct(). Less efficient but
        // equivalent for distinct-keys. Should also return {"A", "B"}.
        var result = await _ctx.Query<ProbeItem>()
            .Select(i => i.Category)
            .Distinct()
            .OrderBy(k => k)
            .ToListAsync();
        _output.WriteLine($"Select.Distinct -> [{string.Join(", ", result)}]");
        Assert.Equal(new[] { "A", "B" }, result.ToArray());
    }

    [Table("ProbeItem")]
    public sealed class ProbeItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
