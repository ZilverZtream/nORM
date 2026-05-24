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
/// Pins materializer + WHERE round-trip on a <c>char?</c> column. Nullable
/// char goes through the same converter path as char (now fixed in 21e55cb),
/// but Nullable&lt;T&gt; wrap means the optimized reader call also has to
/// produce a `char?` rather than `char` — and the NULL row must materialize
/// as `null` rather than '\0'.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNullableCharColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NcrRow (Id INTEGER PRIMARY KEY, Grade TEXT NULL);
            INSERT INTO NcrRow VALUES
                (1, 'A'),
                (2, NULL),
                (3, 'B'),
                (4, NULL);
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
    public async Task Nullable_char_column_materializes_null_for_db_null_rows()
    {
        var rows = (await _ctx.Query<NcrRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal('A', rows[0].Grade);
        Assert.Null(rows[1].Grade);
        Assert.Equal('B', rows[2].Grade);
        Assert.Null(rows[3].Grade);
    }

    [Fact]
    public async Task Nullable_char_equality_against_concrete_value_excludes_nulls()
    {
        var rows = await _ctx.Query<NcrRow>().Where(r => r.Grade == 'A').ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Fact]
    public async Task Nullable_char_has_value_filter_matches_only_non_null_rows()
    {
        var rows = (await _ctx.Query<NcrRow>().Where(r => r.Grade.HasValue).ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(3, rows[1].Id);
    }

    [Table("NcrRow")]
    public sealed class NcrRow
    {
        [Key] public int Id { get; set; }
        public char? Grade { get; set; }
    }
}
