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
/// Probe pin for `Where(p =&gt; p.V &gt; threshold)` where p.V is decimal? and
/// threshold is a closure-captured decimal. Two paths under test:
///   (1) the column-side decimal CAST AS REAL coercion (8d795f4) flows
///       through when one operand is a parameter, not a literal.
///   (2) the nullable column produces correct comparison semantics
///       (NULL rows don't match either `> threshold` or `<= threshold`).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDecimalNullableClosureTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdncItem (Id INTEGER PRIMARY KEY, V TEXT NULL);
            INSERT INTO WdncItem VALUES
                (1, '10.5'),
                (2, '2.0'),
                (3, '100.0'),
                (4, NULL),
                (5, '1.5');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdncItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_nullable_decimal_column_greater_than_closure_variable_filters_numerically()
    {
        var threshold = 5m;
        var ids = await _ctx.Query<WdncItem>()
            .Where(p => p.V > threshold)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        // V > 5: 10.5, 100.0 -> Ids 1, 3 (NULL row 4 excluded; 2.0 and 1.5 excluded).
        // Lex bug would include '2.0' > '5' (FALSE in lex, '2' < '5') and exclude
        // '10.5' > '5' (FALSE in lex, '1' < '5') -- inverted from numeric truth.
        Assert.Equal(new[] { 1, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdncItem")]
    public sealed class WdncItem
    {
        [Key] public int Id { get; set; }
        public decimal? V { get; set; }
    }
}
