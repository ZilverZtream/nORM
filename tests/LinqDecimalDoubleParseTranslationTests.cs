using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of <c>decimal.Parse(stringCol)</c> and
/// <c>double.Parse(stringCol)</c>. Same shape as int.Parse but lowers to
/// a numeric / real CAST instead of an integer CAST. Useful when amounts
/// or rates are stored as text (legacy schemas, mixed-content fields):
/// <c>Where(x =&gt; decimal.Parse(x.Amount) &gt; 50m)</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDecimalDoubleParseTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DpRow (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL);
            INSERT INTO DpRow VALUES
                (1, '12.50'),
                (2, '7.25'),
                (3, '100.00'),
                (4, '3.14');
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
    public async Task Decimal_parse_in_where_compares_numeric_value_of_text_column()
    {
        // > 10 matches '12.50' and '100.00'. Lexicographic compare would put '100.00'
        // before '12.50' before '3.14' before '7.25' — wrong rows.
        var ids = (await _ctx.Query<DpRow>()
            .Where(r => decimal.Parse(r.Amount, CultureInfo.InvariantCulture) > 10m)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public async Task Double_parse_in_orderby_sorts_numerically_not_lexicographically()
    {
        var ids = (await _ctx.Query<DpRow>()
            .OrderBy(r => double.Parse(r.Amount, CultureInfo.InvariantCulture))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        // Numeric order: 3.14, 7.25, 12.50, 100.00 → rows 4, 2, 1, 3
        Assert.Equal(new[] { 4, 2, 1, 3 }, ids);
    }

    [Table("DpRow")]
    public sealed class DpRow
    {
        [Key] public int Id { get; set; }
        public string Amount { get; set; } = string.Empty;
    }
}
