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
/// Pins <c>p.IntColumn.ToString()</c> as a projection -- the explicit
/// numeric-to-string spelling that surfaced as a gap in e761a00. Users
/// reach for ToString() naturally when string-building; if it routes to
/// the client-eval guard, the entire query throws.
///
/// Two expected paths once translated:
///   * <c>Select(p =&gt; p.Age.ToString())</c> -> CAST(Age AS TEXT)
///     returns a list of strings ("42", "85", "41").
///   * <c>Select(p =&gt; p.First + p.Age.ToString())</c> -> concat the
///     CAST result with the string column ("Ada42", "Grace85",
///     "Alan41").
///
/// Silent-wrongness shapes:
///   * Translator emits Age as a bare number column in a string context,
///     SQLite returns "42" coerced numerically -> looks right on probe
///     but could differ on culture-sensitive formatting (decimal /
///     double).
///   * ToString(IFormatProvider) overload silently dropped, locale
///     leaks into result.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionIntToStringTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PitsPerson (Id INTEGER PRIMARY KEY, First TEXT NOT NULL, Age INTEGER NOT NULL);
            INSERT INTO PitsPerson VALUES
                (1, 'Ada', 42),
                (2, 'Grace', 85),
                (3, 'Alan', 41);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PitsPerson>().HasKey(p => p.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_int_column_ToString_returns_string_representations()
    {
        // Bare int.ToString() in projection. Expected: ["42", "85", "41"].
        // Silent-wrongness: empty strings or numeric-coerced "0" rows.
        var result = await _ctx.Query<PitsPerson>()
            .OrderBy(p => p.Id)
            .Select(p => p.Age.ToString())
            .ToListAsync();
        Assert.Equal(new[] { "42", "85", "41" }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_column_plus_int_ToString_concatenates_to_full_strings()
    {
        // The explicit form the e761a00 pin noted as currently throwing.
        // Post-fix: should equal the implicit p.First + p.Age path.
        var result = await _ctx.Query<PitsPerson>()
            .OrderBy(p => p.Id)
            .Select(p => p.First + p.Age.ToString())
            .ToListAsync();
        Assert.Equal(new[] { "Ada42", "Grace85", "Alan41" }, result.ToArray());
    }

    [Table("PitsPerson")]
    public sealed class PitsPerson
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public int Age { get; set; }
    }
}
