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
/// Verifies ExecuteDeleteAsync composes with a correlated EXISTS subquery in the predicate.
/// The "delete parents without children" shape is one of the most common UPDATE/DELETE
/// idioms; it should translate to `DELETE FROM parent WHERE NOT EXISTS (SELECT 1 FROM child
/// WHERE child.fk = parent.pk)`.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteDeleteCorrelatedTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EdUser  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE EdOrder (Id INTEGER PRIMARY KEY, UserId INTEGER NOT NULL, Total INTEGER NOT NULL);
            INSERT INTO EdUser  VALUES (1,'Alice'),(2,'Bob'),(3,'Carol'),(4,'Dave');
            INSERT INTO EdOrder VALUES (10,1,100),(11,1,200),(20,3,50);
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
    public async Task ExecuteDelete_with_correlated_NotAny_subquery_removes_only_unmatched_parents()
    {
        var affected = await _ctx.Query<EdUser>()
            .Where(u => !_ctx.Query<EdOrder>().Any(o => o.UserId == u.Id))
            .ExecuteDeleteAsync();
        Assert.Equal(2, affected); // Bob (Id=2) and Dave (Id=4) had no orders.

        var remaining = (await _ctx.Query<EdUser>().OrderBy(u => u.Id).ToListAsync())
            .Select(u => u.Name).ToArray();
        Assert.Equal(new[] { "Alice", "Carol" }, remaining);
    }

    [Fact]
    public async Task ExecuteDelete_with_correlated_Any_subquery_removes_matched_parents()
    {
        var affected = await _ctx.Query<EdUser>()
            .Where(u => _ctx.Query<EdOrder>().Any(o => o.UserId == u.Id && o.Total >= 100))
            .ExecuteDeleteAsync();
        Assert.Equal(1, affected); // Only Alice has an order with Total >= 100.

        var remaining = (await _ctx.Query<EdUser>().OrderBy(u => u.Id).ToListAsync())
            .Select(u => u.Name).ToArray();
        Assert.Equal(new[] { "Bob", "Carol", "Dave" }, remaining);
    }

    [Table("EdUser")]
    public sealed class EdUser
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("EdOrder")]
    public sealed class EdOrder
    {
        [Key] public int Id { get; set; }
        public int UserId { get; set; }
        public int Total { get; set; }
    }
}
