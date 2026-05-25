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
/// Sister of <see cref="LinqWhereAfterTakeTests"/> for Distinct after
/// Take/Skip. LINQ semantics: take a window of n, THEN distinct-it. If the
/// translator instead applies DISTINCT to the full SELECT before the LIMIT,
/// the LIMIT picks N rows from the distinct set — wrong count, possibly
/// wrong rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DatRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL);
            -- OrderBy(Id).Take(3) → {(1,'A'),(2,'A'),(3,'B')}.
            -- DISTINCT on Category over that window → {A, B}.
            -- A naive `SELECT DISTINCT Category … LIMIT 3` against the full table
            -- would yield {A, B, C} — 3 rows, wrong because it ignored the window.
            INSERT INTO DatRow VALUES
                (1, 'A'),
                (2, 'A'),
                (3, 'B'),
                (4, 'C'),
                (5, 'D');
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
    public async Task Distinct_after_take_dedupes_only_the_windowed_rows()
    {
        // OrderBy(Id).Take(3) → {(1,'A'),(2,'A'),(3,'B')}. Project Category → ['A','A','B'].
        // Distinct → {A, B}. Two rows.
        // Pre-fix: full-table distinct yielded {A, B, C} (3 rows), violating the window.
        var result = (await _ctx.Query<DatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Select(r => r.Category)
            .Distinct()
            .ToListAsync()).OrderBy(s => s).ToArray();
        Assert.Equal(new[] { "A", "B" }, result);
    }

    [Table("DatRow")]
    public sealed class DatRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
    }
}
