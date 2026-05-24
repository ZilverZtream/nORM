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
/// Sister of 76f04af (string + in projection silent-"0" fix). The
/// fix was applied to both VisitBinary code paths -- this file verifies
/// the predicate-side path (ExpressionToSqlVisitor) actually picks up
/// the same fix. Without it,
///   <c>Where(p =&gt; (p.First + p.Last) == "AdaLovelace")</c>
/// would compare numeric-coerced "0" to the literal string and return
/// zero rows on every query (silent-wrongness with the same shape).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringConcatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WscPerson (Id INTEGER PRIMARY KEY, First TEXT NOT NULL, Last TEXT NOT NULL);
            INSERT INTO WscPerson VALUES
                (1, 'Ada', 'Lovelace'),
                (2, 'Grace', 'Hopper'),
                (3, 'Alan', 'Turing');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WscPerson>().HasKey(p => p.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_string_plus_operator_matches_concatenated_predicate()
    {
        // Pre-fix: (p.First + p.Last) emitted as numeric SQL '+' -> "0" per
        // row -> "0" == "AdaLovelace" is false everywhere -> 0 rows.
        // Post-fix: emit as col || col concat, compares to literal, matches Id 1.
        var result = await _ctx.Query<WscPerson>()
            .Where(p => (p.First + p.Last) == "AdaLovelace")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_string_plus_constant_matches_decorated_form()
    {
        // Where ("Dr. " + p.Last) == "Dr. Hopper". Pre-fix: same numeric
        // coercion -> "0" -> 0 rows. Post-fix: matches Id 2 only.
        var result = await _ctx.Query<WscPerson>()
            .Where(p => ("Dr. " + p.Last) == "Dr. Hopper")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_three_part_string_plus_matches_full_name_with_separator()
    {
        // Three-arg concat: chain associativity must produce the same SQL
        // shape regardless of whether the compiler groups as (a + b) + c
        // or a + (b + c). Both should emit a sequence of concat calls
        // producing "Alan Turing".
        var result = await _ctx.Query<WscPerson>()
            .Where(p => (p.First + " " + p.Last) == "Alan Turing")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WscPerson")]
    public sealed class WscPerson
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public string Last { get; set; } = string.Empty;
    }
}
