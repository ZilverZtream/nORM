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
/// Pins <c>string.Concat</c> and string '+' usage in projection. Two
/// shapes most users reach for:
///   * <c>Select(p =&gt; string.Concat(p.First, " ", p.Last))</c>
///   * <c>Select(p =&gt; p.First + " " + p.Last)</c>
///
/// Memory item SQL-1 covers CONCAT portability (SqliteProvider overrides
/// with <c>||</c>). This pin verifies the projection-side emit -- the
/// translator must produce a column expression equivalent to
/// <c>col1 || ' ' || col2</c> on SQLite (or <c>CONCAT(...)</c> on
/// SQL Server/MySQL).
///
/// Silent-wrongness shapes:
///   * Translator drops Concat / + entirely -> returns first arg only,
///     or some other column, or NULL.
///   * Numeric coercion: a `+` on a string column gets emitted as SQL
///     '+' which on SQLite returns 0 for non-numeric TEXT (silent zero).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringConcatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscPerson (Id INTEGER PRIMARY KEY, First TEXT NOT NULL, Last TEXT NOT NULL);
            INSERT INTO PscPerson VALUES
                (1, 'Ada', 'Lovelace'),
                (2, 'Grace', 'Hopper'),
                (3, 'Alan', 'Turing');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscPerson>().HasKey(p => p.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_with_string_plus_operator_returns_concatenated_full_names()
    {
        // `+` on string columns is the most natural spelling. Expected
        // output: "Ada Lovelace", "Grace Hopper", "Alan Turing".
        // Silent-wrongness probes:
        //   * dropped + -> returns just First or just Last
        //   * emitted as numeric + on TEXT -> returns "0" per row
        var result = await _ctx.Query<PscPerson>()
            .OrderBy(p => p.Id)
            .Select(p => p.First + " " + p.Last)
            .ToListAsync();
        Assert.Equal(new[] { "Ada Lovelace", "Grace Hopper", "Alan Turing" }, result.ToArray());
    }

    [Fact]
    public async Task Select_with_string_Concat_static_method_returns_concatenated_full_names()
    {
        // string.Concat(a, b, c) static form -- different visitor route than `+`,
        // worth pinning separately so a regression in one path doesn't slip
        // past the other. ETSV handles this at ~line 1333 via provider's concat
        // primitive; SCV reaches it via the same provider route. Strict pin --
        // no throw-or-correct contract; the feature works and must keep working.
        var result = await _ctx.Query<PscPerson>()
            .OrderBy(p => p.Id)
            .Select(p => string.Concat(p.First, " ", p.Last))
            .ToListAsync();
        Assert.Equal(new[] { "Ada Lovelace", "Grace Hopper", "Alan Turing" }, result.ToArray());
    }

    [Fact]
    public async Task Select_with_string_plus_constant_returns_decorated_strings()
    {
        // String + constant literal -- pins that the constant side becomes a
        // SQL string literal and concatenation still works.
        var result = await _ctx.Query<PscPerson>()
            .OrderBy(p => p.Id)
            .Select(p => "Dr. " + p.Last)
            .ToListAsync();
        Assert.Equal(new[] { "Dr. Lovelace", "Dr. Hopper", "Dr. Turing" }, result.ToArray());
    }

    [Table("PscPerson")]
    public sealed class PscPerson
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public string Last { get; set; } = string.Empty;
    }
}
