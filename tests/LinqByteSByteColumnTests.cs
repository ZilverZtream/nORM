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
/// Pins materializer + WHERE round-trip on <see cref="byte"/> and
/// <see cref="sbyte"/> columns. SQLite stores small integers as INTEGER
/// (long-typed when read back); the materializer must coerce the 64-bit
/// value down to the column's CLR type without losing the value, and
/// equality predicates against a constant byte / sbyte must match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqByteSByteColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE BsRow (Id INTEGER PRIMARY KEY, Severity INTEGER NOT NULL, Delta INTEGER NOT NULL);
            INSERT INTO BsRow VALUES
                (1, 0,   -10),
                (2, 5,   0),
                (3, 200, 100),
                (4, 255, 127);
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
    public async Task Byte_column_round_trips_through_materializer_including_boundary_values()
    {
        var rows = (await _ctx.Query<BsRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal((byte)0,   rows[0].Severity);
        Assert.Equal((byte)5,   rows[1].Severity);
        Assert.Equal((byte)200, rows[2].Severity);
        Assert.Equal((byte)255, rows[3].Severity);
    }

    [Fact]
    public async Task SByte_column_round_trips_through_materializer_with_negative_values()
    {
        var rows = (await _ctx.Query<BsRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal((sbyte)-10, rows[0].Delta);
        Assert.Equal((sbyte)0,   rows[1].Delta);
        Assert.Equal((sbyte)100, rows[2].Delta);
        Assert.Equal((sbyte)127, rows[3].Delta);
    }

    [Fact]
    public async Task Byte_equality_filter_matches_constant()
    {
        var rows = await _ctx.Query<BsRow>().Where(r => r.Severity == (byte)200).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Id);
    }

    [Table("BsRow")]
    public sealed class BsRow
    {
        [Key] public int Id { get; set; }
        public byte Severity { get; set; }
        public sbyte Delta { get; set; }
    }
}
