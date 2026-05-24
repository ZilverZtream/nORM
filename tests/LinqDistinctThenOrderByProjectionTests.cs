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
/// Pins <c>Select(p =&gt; new {p.Name}).Distinct().OrderBy(x =&gt; x.Name)</c>.
/// Combines the SCV projection path with Distinct (e97b814) and OrderBy
/// (which usually targets entity columns). After Distinct, OrderBy must
/// reference the projection's anonymous-type field — not the underlying
/// entity column. Silent-wrongness risk if OrderBy ignores the projection
/// and tries to bind against the entity instead, or if Distinct strips
/// rows AFTER ordering instead of before.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctThenOrderByProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtoRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL);
            -- Five duplicates split across three distinct categories.
            INSERT INTO DtoRow VALUES
                (1, 'Music'),
                (2, 'Music'),
                (3, 'Books'),
                (4, 'Books'),
                (5, 'Games');
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
    public async Task Distinct_then_orderby_on_projection_returns_unique_rows_in_projection_order()
    {
        // Three distinct Category values, sorted alphabetically: Books, Games, Music.
        var rows = (await _ctx.Query<DtoRow>()
            .Select(r => r.Category)
            .Distinct()
            .OrderBy(c => c)
            .ToListAsync())
            .ToArray();
        Assert.Equal(new[] { "Books", "Games", "Music" }, rows);
    }

    [Fact]
    public async Task Distinct_anonymous_then_orderby_on_member_returns_unique_rows_in_member_order()
    {
        // Project into an anonymous type with the same field, then Distinct + OrderBy.
        var rows = (await _ctx.Query<DtoRow>()
            .Select(r => new { Cat = r.Category })
            .Distinct()
            .OrderBy(x => x.Cat)
            .ToListAsync())
            .Select(x => x.Cat)
            .ToArray();
        Assert.Equal(new[] { "Books", "Games", "Music" }, rows);
    }

    [Table("DtoRow")]
    public sealed class DtoRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
    }
}
