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
/// Strict pin for OrderBy on a nullable column. SQL Server puts NULLs
/// last for ASC by default; SQLite puts NULLs first; Postgres puts NULLs
/// last. .NET sorting puts null first for ASC, last for DESC. For nORM
/// to give deterministic cross-provider behavior, the translator should
/// emit explicit NULLS FIRST / NULLS LAST.
///
/// This pin documents current SQLite behavior so a future provider-
/// normalizing change surfaces. Silent-wrongness shape: user assumes
/// .NET ordering, gets SQLite default which differs on DESC.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByNullableColumnContractTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObnItem (Id INTEGER PRIMARY KEY, Score INTEGER NULL);
            INSERT INTO ObnItem VALUES
                (1, 10),
                (2, NULL),
                (3, 20),
                (4, NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<ObnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task OrderBy_ascending_nullable_column_puts_nulls_first_matching_dotnet_semantics()
    {
        // .NET LINQ-to-Objects sorts null first for ASC. SQLite ASC default
        // is NULLs first too, so they happen to align. Pin documents this.
        var result = await _ctx.Query<ObnItem>()
            .OrderBy(i => i.Score)
            .ThenBy(i => i.Id)
            .Select(i => new { i.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2, 4, 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task OrderByDescending_nullable_column_puts_nulls_last_matching_dotnet_semantics()
    {
        // .NET LINQ-to-Objects sorts null LAST for DESC. SQLite DESC default
        // is NULLs LAST too. So same alignment. (SQL Server differs!)
        var result = await _ctx.Query<ObnItem>()
            .OrderByDescending(i => i.Score)
            .ThenBy(i => i.Id)
            .Select(i => new { i.Id })
            .ToListAsync();
        Assert.Equal(new[] { 3, 1, 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("ObnItem")]
    public sealed class ObnItem
    {
        [Key] public int Id { get; set; }
        public int? Score { get; set; }
    }
}
