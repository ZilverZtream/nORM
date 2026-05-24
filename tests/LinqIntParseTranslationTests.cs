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
/// Pins server-side translation of <c>int.Parse(stringCol)</c> and
/// <c>long.Parse(stringCol)</c>. Lowers to a provider-specific integer CAST:
/// <list type="bullet">
///   <item><c>Where(x =&gt; int.Parse(x.Code) &gt; 5)</c> → <c>CAST(Code AS INTEGER) &gt; @p0</c></item>
///   <item><c>OrderBy(x =&gt; int.Parse(x.Code))</c> → server-side numeric sort over a TEXT column</item>
/// </list>
/// Useful for scenarios where numeric-looking values are stored as strings
/// (legacy schemas, polymorphic Code columns).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqIntParseTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IpRow (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO IpRow VALUES
                (1, '3'),
                (2, '12'),
                (3, '7'),
                (4, '100');
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
    public async Task Int_parse_in_where_compares_numeric_value_of_text_column()
    {
        // > 5 matches '12' (12), '7' (7), '100' (100). Lexicographic comparison would put '100'
        // before '12' before '3' before '7' — so a wrong (text) comparison would return
        // different rows. Asserting the numeric ones pins the cast.
        var ids = (await _ctx.Query<IpRow>()
            .Where(r => int.Parse(r.Code) > 5)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 2, 3, 4 }, ids);
    }

    [Fact]
    public async Task Int_parse_in_orderby_sorts_numerically_not_lexicographically()
    {
        var ids = (await _ctx.Query<IpRow>()
            .OrderBy(r => int.Parse(r.Code))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        // Numeric order: 3, 7, 12, 100 → row Ids 1, 3, 2, 4
        Assert.Equal(new[] { 1, 3, 2, 4 }, ids);
    }

    [Table("IpRow")]
    public sealed class IpRow
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
