using System;
using System.Collections.Generic;
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
/// Probes the ParameterValueExtractor mis-alignment shape (407e03d /
/// eeff6e7 / cf39b61) for the IN-list constant-fold path in ETSV. The
/// IN-list handler folds the closure-captured collection inline and
/// emits its items as @p inline-const params. If the predicate ALSO
/// has a second closure-captured RHS, the extractor's value list
/// shifts by one (the captured collection counts as a closure value)
/// and @cp0 binds to the collection reference instead of the second-
/// closure value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereInListClosureAndOtherClosureTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WilcItem (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO WilcItem VALUES
                (1, 10, 'red'),
                (2, 50, 'red'),
                (3, 75, 'blue'),
                (4, 50, 'red');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WilcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_InList_closure_AND_other_column_equals_closure_filters_correctly()
    {
        // ids = {1, 2, 4} -> matches Id 1, 2, 4.
        // tagWanted = "red" -> matches Id 1, 2, 4.
        // Combined AND -> {1, 2, 4}.
        var ids = new[] { 1, 2, 4 };
        var tagWanted = "red";
        var result = await _ctx.Query<WilcItem>()
            .Where(p => ids.Contains(p.Id) && p.Tag == tagWanted)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WilcItem")]
    public sealed class WilcItem
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
