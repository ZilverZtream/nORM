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
/// Pins <c>-r.Duration</c> on a TimeSpan column in projection — the unary
/// negation form. Returns a TimeSpan negated from the stored value. Probes
/// for silent-wrongness in the materialiser's TimeSpan reading path under
/// numeric-negate emit shapes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTimeSpanColumnNegationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TnRow (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);
            INSERT INTO TnRow VALUES
                (1, '01:00:00'),
                (2, '00:30:00'),
                (3, '00:00:45');
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
    public async Task Negate_TimeSpan_column_returns_inverted_span_in_projection()
    {
        var rows = (await _ctx.Query<TnRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Neg = -r.Duration })
            .ToListAsync())
            .ToArray();
        Assert.Equal(TimeSpan.Parse("-01:00:00"), rows[0].Neg);
        Assert.Equal(TimeSpan.Parse("-00:30:00"), rows[1].Neg);
        Assert.Equal(TimeSpan.Parse("-00:00:45"), rows[2].Neg);
    }

    [Table("TnRow")]
    public sealed class TnRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
