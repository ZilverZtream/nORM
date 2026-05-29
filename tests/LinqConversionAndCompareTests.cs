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
/// Exercises System.Convert.* and System.String.Compare/CompareTo in WHERE predicates against a
/// real database. These shapes turn up whenever an application coerces a stored numeric/string
/// value to a different CLR type before comparing, or uses Compare to drive a tri-valued sort.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqConversionAndCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CvRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Number INTEGER NOT NULL, NumText TEXT NOT NULL);
            INSERT INTO CvRow VALUES
                (1, 'alpha',   1, '1'),
                (2, 'bravo',  10, '10'),
                (3, 'charlie',100, '100'),
                (4, 'delta',  50, '50');
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
    public async Task Convert_ToInt32_on_text_column_compares_as_integer()
    {
        var ids = (await _ctx.Query<CvRow>()
            .Where(r => Convert.ToInt32(r.NumText) > 50)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        // NumText > 50 numerically: 100 only.
        Assert.Equal(new[] { 3 }, ids);
    }

    [Fact]
    public async Task Convert_ToString_on_int_column_concatenates()
    {
        var hits = (await _ctx.Query<CvRow>()
            .Where(r => Convert.ToString(r.Number) == "100")
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3 }, hits);
    }

    [Fact]
    public async Task Convert_ChangeType_with_captured_Type_compares_as_requested_type()
    {
        var targetType = typeof(int);
        var ids = (await _ctx.Query<CvRow>()
            .Where(r => (int)Convert.ChangeType(r.NumText, targetType) >= 50)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { 3, 4 }, ids);
    }

    [Fact]
    public async Task String_Compare_static_returns_zero_for_equal_strings()
    {
        var hits = await _ctx.Query<CvRow>()
            .Where(r => string.Compare(r.Name, "delta") == 0)
            .ToListAsync();
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task String_Compare_returns_negative_for_lexically_smaller()
    {
        var ids = (await _ctx.Query<CvRow>()
            .Where(r => string.Compare(r.Name, "bravo") < 0)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        // alpha < bravo lexically; nothing else.
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task String_Compare_with_StringComparison_OrdinalIgnoreCase_matches_case_insensitively()
    {
        var comparison = StringComparison.OrdinalIgnoreCase;

        var hits = await _ctx.Query<CvRow>()
            .Where(r => string.Compare(r.Name, "BRAVO", comparison) == 0)
            .ToListAsync();

        Assert.Single(hits);
        Assert.Equal(2, hits[0].Id);
    }

    [Fact]
    public async Task String_Compare_with_captured_ignoreCase_bool_matches_case_insensitively()
    {
        var ignoreCase = true;

        var hits = await _ctx.Query<CvRow>()
            .Where(r => string.Compare(r.Name, "DELTA", ignoreCase) == 0)
            .ToListAsync();

        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task String_CompareOrdinal_uses_case_sensitive_ordinal_semantics()
    {
        var hits = await _ctx.Query<CvRow>()
            .Where(r => string.CompareOrdinal(r.Name, "BRAVO") == 0)
            .ToListAsync();

        Assert.Empty(hits);
    }

    [Fact]
    public async Task String_CompareTo_instance_form_returns_positive_for_lexically_greater()
    {
        var ids = (await _ctx.Query<CvRow>()
            .Where(r => r.Name.CompareTo("charlie") > 0)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        // delta > charlie lexically.
        Assert.Equal(new[] { 4 }, ids);
    }

    [Table("CvRow")]
    public sealed class CvRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Number { get; set; }
        public string NumText { get; set; } = string.Empty;
    }
}
