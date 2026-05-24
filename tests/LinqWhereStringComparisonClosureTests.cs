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
/// eeff6e7 / cf39b61 / 04a0003 / 7d6d7ac) for ETSV's IsIgnoreCase
/// StringComparison fold (line ~85). When the comparison enum is
/// closure-captured AND the predicate has another closure-captured
/// RHS later, the fold consumes the comparison without reserving a
/// compiled-param slot -> extractor list shifts.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringComparisonClosureTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO WsccItem VALUES
                (1, 'alpha', 'red'),
                (2, 'BETA',  'red'),
                (3, 'beta',  'blue'),
                (4, 'gamma', 'red');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WsccItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_StartsWith_with_closure_StringComparison_AND_other_closure_RHS_filters_correctly()
    {
        // prefix = "be" (matches both Id 2 'BETA' and Id 3 'beta' case-insensitively).
        // cmp = OrdinalIgnoreCase (the StringComparison).
        // tagWanted = "red" -> Id 1, 2, 4.
        // Combined Where: StartsWith(prefix, cmp) AND Tag == tagWanted -> {2}.
        var prefix = "be";
        var cmp = StringComparison.OrdinalIgnoreCase;
        var tagWanted = "red";
        var result = await _ctx.Query<WsccItem>()
            .Where(p => p.Name.StartsWith(prefix, cmp) && p.Tag == tagWanted)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WsccItem")]
    public sealed class WsccItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Tag { get; set; } = string.Empty;
    }
}
