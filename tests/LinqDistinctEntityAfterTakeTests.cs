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
/// Pins <c>q.OrderBy(Id).Take(N).Distinct()</c> with an entity-row receiver
/// (no intermediate <c>Select</c> projection). LINQ semantics: take the
/// windowed sequence, THEN dedupe inside it. If the translator emits
/// `SELECT DISTINCT … LIMIT N` SQL evaluates DISTINCT first then LIMIT —
/// returning rows OUTSIDE the original windowed set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctEntityAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DeaRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO DeaRow VALUES
                (1, 10),
                (2, 10),
                (3, 20),
                (4, 30),
                (5, 999);
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
    public async Task Distinct_after_take_returns_at_most_window_size_rows()
    {
        // OrderBy(Id).Take(3) → rows 1,2,3. Each row has unique Id (primary key)
        // so Distinct over entity rows yields all 3 rows.
        // Naive translation would distinct full table (5 rows distinct by all-columns)
        // then LIMIT 3 → picks first 3 of all 5 rows in some order. Result may include
        // rows OUTSIDE the original windowed top-3.
        var rows = (await _ctx.Query<DeaRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Distinct()
            .ToListAsync())
            .OrderBy(r => r.Id)
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(new[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("DeaRow")]
    public sealed class DeaRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
