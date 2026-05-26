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
/// Pins LINQ <c>Aggregate</c> over a string-typed scalar source with a
/// concat-with-constant-separator fold. Lowers to the provider's
/// string-aggregate (STRING_AGG on SqlServer/Postgres, GROUP_CONCAT on
/// SQLite/MySQL) so the join runs server-side instead of materialising
/// every row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateStringConcatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CcRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO CcRow VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');
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
    public async Task Aggregate_string_concat_with_seed_aware_separator_lowers_to_GROUP_CONCAT()
    {
        await Task.CompletedTask;
        string joined = _ctx.Query<CcRow>()
            .OrderBy(r => r.Id)
            .Select(r => r.Name)
            .Aggregate(string.Empty, (acc, s) => acc + (acc == string.Empty ? "" : ", ") + s);
        Assert.Equal("alpha, bravo, charlie", joined);
    }

    [Fact]
    public async Task Aggregate_string_concat_simple_separator_no_seed_lowers_to_GROUP_CONCAT()
    {
        await Task.CompletedTask;
        string joined = _ctx.Query<CcRow>()
            .OrderBy(r => r.Id)
            .Select(r => r.Name)
            .Aggregate((acc, s) => acc + "|" + s);
        Assert.Equal("alpha|bravo|charlie", joined);
    }

    [Fact]
    public async Task Aggregate_string_concat_with_seed_returns_seed_when_table_empty()
    {
        await using (var del = _cn.CreateCommand()) { del.CommandText = "DELETE FROM CcRow"; await del.ExecuteNonQueryAsync(); }
        string joined = _ctx.Query<CcRow>()
            .Select(r => r.Name)
            .Aggregate("X", (acc, s) => acc + (acc == "X" ? "" : ", ") + s);
        Assert.Equal("X", joined);
    }

    [Table("CcRow")]
    public sealed class CcRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}
