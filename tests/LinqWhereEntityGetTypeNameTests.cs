using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Strict pin for <c>entity.GetType().Name</c> inside WHERE -- the sister
/// of the SCV fold from 149fa9a. Since nORM doesn't emit a runtime type
/// discriminator at query-time, the value is statically determined by
/// the parameter's declared type:
///   * <c>p.GetType().Name == "PtonItem"</c> always evaluates true.
///   * <c>p.GetType().Name == "Other"</c>  always evaluates false.
/// Translating to a constant string literal lets SQLite's planner
/// short-circuit the predicate via constant folding.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereEntityGetTypeNameTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WgtnItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
            INSERT INTO WgtnItem VALUES
                (1, 'alpha'),
                (2, 'beta');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WgtnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_entity_GetType_Name_equals_declared_type_returns_all_rows()
    {
        var ids = await _ctx.Query<WgtnItem>()
            .Where(p => p.GetType().Name == "WgtnItem")
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_entity_GetType_Name_equals_other_type_returns_no_rows()
    {
        var ids = await _ctx.Query<WgtnItem>()
            .Where(p => p.GetType().Name == "SomethingElse")
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Empty(ids);
    }

    [Table("WgtnItem")]
    public sealed class WgtnItem
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }
}
