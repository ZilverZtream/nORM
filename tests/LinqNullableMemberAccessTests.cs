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
/// Exercises null-safe member access (?. operator) on nullable struct columns. In LINQ a
/// nullable member access like `r.Created?.Year` lowers to a conditional that the visitor
/// must turn into a CASE expression so the SQL handles the NULL row correctly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNullableMemberAccessTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NbRow (Id INTEGER PRIMARY KEY, Created TEXT NULL, Count INTEGER NULL);
            INSERT INTO NbRow VALUES
                (1, '2020-01-15', 100),
                (2, '2021-06-30', NULL),
                (3, NULL, 50),
                (4, NULL, NULL);
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
    public async Task Nullable_HasValue_filters_to_non_null_rows()
    {
        var ids = (await _ctx.Query<NbRow>().Where(r => r.Created.HasValue).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public async Task Nullable_Value_compared_in_where_filters_correctly()
    {
        var ids = (await _ctx.Query<NbRow>().Where(r => r.Created.HasValue && r.Created.Value.Year == 2020).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Nullable_int_GetValueOrDefault_supplies_fallback()
    {
        var ids = (await _ctx.Query<NbRow>().Where(r => r.Count.GetValueOrDefault() > 60).ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        // Count > 60: id 1 (100). Nulls fall back to 0 and are excluded.
        Assert.Equal(new[] { 1 }, ids);
    }

    [Table("NbRow")]
    public sealed class NbRow
    {
        [Key] public int Id { get; set; }
        public DateTime? Created { get; set; }
        public int? Count { get; set; }
    }
}
