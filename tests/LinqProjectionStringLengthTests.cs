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
/// Pins <c>p.StringColumn.Length</c> inside a Select projection. Sister of
/// <see cref="LinqProjectionDateTimePartsTests"/> — confirms that the
/// d263d3e fix to <c>SCV.VisitMember</c> has the broader scope it claims:
/// it should route ANY non-mapped member through
/// <c>_provider.TranslateFunction</c>, not just DateTime parts. SqliteProvider
/// has <c>nameof(string.Length) when args.Length == 1 =&gt; $"LENGTH({args[0]})"</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringLengthTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SlRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO SlRow VALUES
              (1, 'a'),
              (2, 'abc'),
              (3, 'abcdefghij'),
              (4, '');
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
    public async Task Projection_string_length_returns_per_row_char_count()
    {
        var rows = (await _ctx.Query<SlRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, L = p.Name.Length })
            .ToListAsync())
            .Select(r => (r.Id, r.L))
            .ToArray();
        // Silent-wrongness check: if Length is dropped and the column echoes,
        // the int materializer fails or returns garbage; if LENGTH(NULL) leaks
        // for empty strings, Id 4 would be NULL instead of 0.
        Assert.Equal(new[] { (1, 1), (2, 3), (3, 10), (4, 0) }, rows);
    }

    [Fact]
    public async Task Projection_string_length_can_be_used_in_arithmetic_expression()
    {
        // p.Name.Length * 2 — composes the member-routed function with the
        // existing arithmetic emission path.
        var rows = (await _ctx.Query<SlRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Doubled = p.Name.Length * 2 })
            .ToListAsync())
            .Select(r => (r.Id, r.Doubled))
            .ToArray();
        Assert.Equal(new[] { (1, 2), (2, 6), (3, 20), (4, 0) }, rows);
    }

    [Table("SlRow")]
    public sealed class SlRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
