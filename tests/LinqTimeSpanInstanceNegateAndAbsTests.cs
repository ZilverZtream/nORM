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
/// Pins <c>r.Duration.Negate()</c> and <c>r.Duration.Duration()</c> (abs).
/// Both are instance methods on TimeSpan column that must lower to REAL-seconds
/// arithmetic so the materialiser's TimeSpan.FromSeconds path reconstructs
/// the value, not the misread-as-ticks numeric coercion.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeSpanInstanceNegateAndAbsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TinRow (Id INTEGER PRIMARY KEY, Span TEXT NOT NULL);
            INSERT INTO TinRow VALUES
                (1, '01:30:00'),
                (2, '-00:45:00'),
                (3, '00:00:30');
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
    public async Task TimeSpan_Negate_instance_method_on_column_returns_inverted_span()
    {
        var rows = (await _ctx.Query<TinRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Neg = r.Span.Negate() })
            .ToListAsync())
            .ToArray();
        Assert.Equal(TimeSpan.Parse("-01:30:00"), rows[0].Neg);
        Assert.Equal(TimeSpan.Parse("00:45:00"),  rows[1].Neg);   // -(-45m) = +45m
        Assert.Equal(TimeSpan.Parse("-00:00:30"), rows[2].Neg);
    }

    [Fact]
    public async Task TimeSpan_Duration_instance_method_on_column_returns_absolute_span()
    {
        var rows = (await _ctx.Query<TinRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Abs = r.Span.Duration() })
            .ToListAsync())
            .ToArray();
        Assert.Equal(TimeSpan.Parse("01:30:00"), rows[0].Abs);
        Assert.Equal(TimeSpan.Parse("00:45:00"), rows[1].Abs);    // |-45m| = +45m
        Assert.Equal(TimeSpan.Parse("00:00:30"), rows[2].Abs);
    }

    [Table("TinRow")]
    public sealed class TinRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Span { get; set; }
    }
}
