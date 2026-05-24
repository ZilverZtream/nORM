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
/// Pins <c>ExecuteUpdateAsync</c> with <c>SetProperty(p =&gt; p.NullableCol, (T?)null)</c>
/// — setting a nullable column back to NULL. Silent-wrongness risk if the null is
/// bound as <c>default(T)</c> (e.g. 0 for <c>int?</c>, "" for <c>string?</c>):
/// the column ends up holding a sentinel value rather than NULL, and downstream
/// <c>IS NULL</c> checks silently no longer match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteUpdateSetPropertyNullRoundtripTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE UnrRow (Id INTEGER PRIMARY KEY, Score INTEGER NULL, Note TEXT NULL);
            INSERT INTO UnrRow VALUES
              (1, 10, 'keep'),
              (2, 20, 'clear-me'),
              (3, NULL, 'already-null'),
              (4, 40, 'clear-me'),
              (5, 50, 'keep');
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
    public async Task SetProperty_nullable_int_to_null_writes_db_null_not_zero()
    {
        // Clear Score on rows where Note='clear-me' (Ids 2 and 4).
        var affected = await _ctx.Query<UnrRow>()
            .Where(r => r.Note == "clear-me")
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Score, (int?)null));
        Assert.Equal(2, affected);

        // Silent-wrongness probe: if the binder used default(int)=0, IS NULL
        // would match only Id 3 (the pre-existing null), failing the count.
        // Read via Microsoft.Data.Sqlite directly so the assertion observes the
        // actual NULL/0 distinction (entity round-trip would coalesce on read).
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM UnrRow WHERE Score IS NULL ORDER BY Id";
        await using var rdr = await cmd.ExecuteReaderAsync();
        var nullIds = new System.Collections.Generic.List<int>();
        while (await rdr.ReadAsync()) nullIds.Add(rdr.GetInt32(0));
        Assert.Equal(new[] { 2, 3, 4 }, nullIds);
    }

    [Fact]
    public async Task SetProperty_nullable_string_to_null_writes_db_null_not_empty_string()
    {
        // Clear Note on rows where Score >= 40 (Ids 4 and 5).
        var affected = await _ctx.Query<UnrRow>()
            .Where(r => r.Score >= 40)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Note, (string?)null));
        Assert.Equal(2, affected);

        // If the binder coerced null to empty string, IS NULL would miss these
        // rows and the count would be wrong.
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM UnrRow WHERE Note IS NULL ORDER BY Id";
        await using var rdr = await cmd.ExecuteReaderAsync();
        var nullIds = new System.Collections.Generic.List<int>();
        while (await rdr.ReadAsync()) nullIds.Add(rdr.GetInt32(0));
        Assert.Equal(new[] { 4, 5 }, nullIds);
    }

    [Fact]
    public async Task SetProperty_round_trip_clears_then_repopulates_value()
    {
        // Clear then set: ensures the second SetProperty path also rebinds the
        // type correctly after a null was the most recent write (parameter
        // metadata-cache contamination shape from 71-series fixes).
        var cleared = await _ctx.Query<UnrRow>()
            .Where(r => r.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Score, (int?)null));
        Assert.Equal(1, cleared);

        var repop = await _ctx.Query<UnrRow>()
            .Where(r => r.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Score, (int?)99));
        Assert.Equal(1, repop);

        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "SELECT Score FROM UnrRow WHERE Id = 1";
        var v = await cmd.ExecuteScalarAsync();
        Assert.Equal(99L, v);
    }

    [Table("UnrRow")]
    public sealed class UnrRow
    {
        [Key] public int Id { get; set; }
        public int? Score { get; set; }
        public string? Note { get; set; }
    }
}
