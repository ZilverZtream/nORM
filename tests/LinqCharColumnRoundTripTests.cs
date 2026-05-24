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
/// Pins materializer + WHERE round-trip on a <see cref="char"/> column. SQLite
/// stores char as a single-character TEXT; the materializer must read it back
/// as a char (not an int) and an equality predicate against a constant char
/// must match the stored row. The DateOnly class of bug (binding-format
/// mismatch) could plausibly affect chars too if Microsoft.Data.Sqlite
/// serialized char-typed parameters differently than the storage shape.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCharColumnRoundTripTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CcrRow (Id INTEGER PRIMARY KEY, Grade TEXT NOT NULL);
            INSERT INTO CcrRow VALUES
                (1, 'A'),
                (2, 'B'),
                (3, 'C'),
                (4, 'A');
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
    public async Task Char_column_round_trips_through_materializer()
    {
        var rows = (await _ctx.Query<CcrRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal('A', rows[0].Grade);
        Assert.Equal('B', rows[1].Grade);
        Assert.Equal('C', rows[2].Grade);
        Assert.Equal('A', rows[3].Grade);
    }

    [Fact]
    public async Task Char_column_equality_filter_matches_rows_with_target_grade()
    {
        // Two rows have 'A' grade.
        var rows = (await _ctx.Query<CcrRow>().Where(r => r.Grade == 'A').ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(4, rows[1].Id);
    }

    [Table("CcrRow")]
    public sealed class CcrRow
    {
        [Key] public int Id { get; set; }
        public char Grade { get; set; }
    }
}
