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
/// Probes the ParameterValueExtractor mis-alignment shape (407e03d /
/// eeff6e7) for EmitLikePredicate: a Where with a closure-captured LIKE
/// pattern AND a second closure-captured RHS later in document order.
/// If the like handler folds the pattern inline without reserving a
/// compiled-param slot, @cp0 binds to the pattern value instead of the
/// other RHS -- silently filters wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereLikeClosureAndOtherClosureTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WlcItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO WlcItem VALUES
                (1, 'alpha', 'red'),
                (2, 'beta',  'red'),
                (3, 'beta-2','blue'),
                (4, 'gamma', 'red');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WlcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_StartsWith_closure_AND_other_column_equals_closure_filters_correctly()
    {
        // prefix = "be" -> Id 2, 3 match StartsWith.
        // tagWanted = "red" -> Id 1, 2, 4 match Tag equality.
        // Combined AND -> {2}.
        var prefix = "be";
        var tagWanted = "red";
        var result = await _ctx.Query<WlcItem>()
            .Where(p => p.Name.StartsWith(prefix) && p.Tag == tagWanted)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WlcItem")]
    public sealed class WlcItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Tag { get; set; } = string.Empty;
    }
}
