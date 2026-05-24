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
/// Probes <c>string.Split</c> used in Where predicates. SQL has no portable
/// split-to-array primitive (SQL Server STRING_SPLIT requires 2016+, MySQL
/// 8.x, Postgres has string_to_array, SQLite has no equivalent), so the
/// only safe behaviors are:
///   * Throw <c>NormUnsupportedFeatureException</c> with an actionable
///     message pointing at workarounds (Contains/LIKE for substring tests,
///     client-eval via AsEnumerable for actual splitting, a computed/
///     persisted column for repeated splits, or SQL JSON/STRING_SPLIT for
///     specific providers).
///   * Translate to a provider-specific function on providers that have one.
///
/// Silent-wrongness shape: translator falls through to client-side
/// materialization + in-memory Split on the full table.
///
/// Pins the throw-or-translate contract -- a future translator addition
/// can flip the throw assertion to a result one.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringSplitProbeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WssItem (Id INTEGER PRIMARY KEY, Tags TEXT NOT NULL);
            INSERT INTO WssItem VALUES
                (1, 'red,blue,green'),
                (2, 'red,yellow'),
                (3, 'blue,green'),
                (4, 'only-red');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WssItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_string_Split_Contains_fails_fast_with_nORM_typed_error()
    {
        // Strict: string.Split in Where has no portable SQL equivalent so the
        // translator must fail-fast with a nORM-typed error rather than fall
        // through to client-eval (full table materialization + in-memory
        // Split = catastrophic on large tables). The error must mention the
        // unsupported shape so users can pivot to a translatable alternative
        // (Contains/LIKE for substring tests, or precompute a relation).
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await _ctx.Query<WssItem>()
                .Where(i => i.Tags.Split(',', StringSplitOptions.None).Contains("red"))
                .OrderBy(i => i.Id)
                .ToListAsync());

        Assert.True(
            ex is NormException || ex is InvalidOperationException || ex is NotSupportedException,
            $"string.Split threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
        var msg = ex.Message.ToLowerInvariant();
        Assert.True(
            msg.Contains("split") || msg.Contains("not supported") || msg.Contains("translat")
                || msg.Contains("client"),
            $"string.Split error lacks actionable hint: {ex.Message}");
    }

    [Fact]
    public async Task Where_with_Contains_substring_works_as_documented_alternative_to_Split()
    {
        // Pin the documented workaround: substring-Contains via LIKE %x%.
        // Imperfect (matches "only-red" too because 'red' is a substring),
        // but the test asserts what the alternative DOES return so users can
        // see the trade-off.
        var result = await _ctx.Query<WssItem>()
            .Where(i => i.Tags.Contains("red"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        // Substring matches: Id 1 (red,blue,green), 2 (red,yellow), 4 (only-red).
        Assert.Equal(new[] { 1, 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WssItem")]
    public sealed class WssItem
    {
        [Key] public int Id { get; set; }
        public string Tags { get; set; } = string.Empty;
    }
}
