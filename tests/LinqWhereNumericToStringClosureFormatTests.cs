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
/// Strict pin probing the same ParameterValueExtractor mis-alignment shape
/// as 407e03d: a Where predicate where one operand is a closure-captured
/// local that gets folded inline (the ToString format string), and a second
/// closure-captured local appears later in document order (the comparison
/// RHS). Without a placeholder compiled-param slot for the inline-folded
/// arg, the extractor's value list shifts by one and @cp0 binds to the
/// format string instead of the RHS string -- predicate evaluates wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNumericToStringClosureFormatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WncfItem (Id INTEGER PRIMARY KEY, Score REAL NOT NULL);
            INSERT INTO WncfItem VALUES
                (1, 3.14159),
                (2, 100.0),
                (3, 0.999);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WncfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_numeric_ToString_with_closure_format_and_closure_rhs_filters_correctly()
    {
        // Both `fmt` and `target` are closure-captured. ToString("F2") result
        // for Id 1 is "3.14"; predicate Score.ToString(fmt) == target matches
        // only Id 1.
        var fmt = "F2";
        var target = "3.14";
        var result = await _ctx.Query<WncfItem>()
            .Where(p => p.Score.ToString(fmt) == target)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WncfItem")]
    public sealed class WncfItem
    {
        [Key] public int Id { get; set; }
        public double Score { get; set; }
    }
}
