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
/// Exercises ExecuteUpdateAsync with computed assignment expressions
/// (`SetProperty(x => x.Counter, x => x.Counter + 1)`). Previously rejected; this test
/// pins the new in-row server-side computation shape.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteUpdateComputedTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EuRow (Id INTEGER PRIMARY KEY, Counter INTEGER NOT NULL, Label TEXT NOT NULL);
            INSERT INTO EuRow VALUES
                (1, 0,  'a'),
                (2, 5,  'b'),
                (3, 10, 'a'),
                (4, 20, 'c');
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
    public async Task ExecuteUpdate_increments_counter_via_computed_expression()
    {
        var affected = await _ctx.Query<EuRow>()
            .Where(r => r.Label == "a")
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Counter, r => r.Counter + 1));
        Assert.Equal(2, affected);

        var counters = (await _ctx.Query<EuRow>().OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Counter).ToArray();
        Assert.Equal(new[] { 1, 5, 11, 20 }, counters);
    }

    [Fact]
    public async Task ExecuteUpdate_doubles_counter_via_computed_expression()
    {
        var affected = await _ctx.Query<EuRow>()
            .Where(r => r.Counter > 0)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Counter, r => r.Counter * 2));
        Assert.Equal(3, affected);

        var counters = (await _ctx.Query<EuRow>().OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Counter).ToArray();
        Assert.Equal(new[] { 0, 10, 20, 40 }, counters);
    }

    [Fact]
    public async Task ExecuteUpdate_combines_literal_and_computed_assignments_in_one_call()
    {
        var affected = await _ctx.Query<EuRow>()
            .Where(r => r.Id == 2)
            .ExecuteUpdateAsync(s => s
                .SetProperty(r => r.Counter, r => r.Counter + 100)
                .SetProperty(r => r.Label, "updated"));
        Assert.Equal(1, affected);

        var row = (await _ctx.Query<EuRow>().Where(r => r.Id == 2).ToListAsync()).Single();
        Assert.Equal(105, row.Counter);
        Assert.Equal("updated", row.Label);
    }

    [Fact]
    public async Task ExecuteUpdate_appends_to_string_column_via_server_side_concat()
    {
        var affected = await _ctx.Query<EuRow>()
            .Where(r => r.Label == "a")
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Label, r => r.Label + "!"));
        Assert.Equal(2, affected);

        var labels = (await _ctx.Query<EuRow>().OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Label).ToArray();
        Assert.Equal(new[] { "a!", "b", "a!", "c" }, labels);
    }

    [Fact]
    public async Task ExecuteUpdate_assigns_computed_value_from_two_other_columns()
    {
        // SetProperty assigns Counter = Id * 10 — pulls from a different column of the
        // same row, exercising the multi-column-reference path of the value translator.
        var affected = await _ctx.Query<EuRow>()
            .Where(r => r.Label == "a")
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Counter, r => r.Id * 10));
        Assert.Equal(2, affected);

        var byId = (await _ctx.Query<EuRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        // Id=1 (Label='a') → 10. Id=3 (Label='a') → 30. Others unchanged.
        Assert.Equal(10, byId[0].Counter);
        Assert.Equal(5,  byId[1].Counter);
        Assert.Equal(30, byId[2].Counter);
        Assert.Equal(20, byId[3].Counter);
    }

    [Table("EuRow")]
    public sealed class EuRow
    {
        [Key] public int Id { get; set; }
        public int Counter { get; set; }
        public string Label { get; set; } = string.Empty;
    }
}
