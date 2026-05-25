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
/// Strict pin + implement-first for <c>bool.Parse(string)</c> in projection.
/// Common pattern: a legacy column stores "True"/"False" text; project as
/// bool. Sister to numeric.Parse (a192623) and DateTime.Parse (5c1a22d).
///
/// .NET bool.Parse is case-insensitive: "True", "true", "TRUE" all -> true;
/// "False", "false", "FALSE" -> false. Map via case-insensitive comparison.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionBoolParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PbpItem (Id INTEGER PRIMARY KEY, FlagText TEXT NOT NULL);
            INSERT INTO PbpItem VALUES
                (1, 'True'),
                (2, 'False'),
                (3, 'true'),
                (4, 'FALSE');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PbpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_bool_Parse_string_column_projects_bool_per_row()
    {
        var result = await _ctx.Query<PbpItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, B = bool.Parse(p.FlagText) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].B);    // "True"
        Assert.False(result[1].B);   // "False"
        Assert.True(result[2].B);    // "true" (case-insensitive)
        Assert.False(result[3].B);   // "FALSE" (case-insensitive)
    }

    [Table("PbpItem")]
    public sealed class PbpItem
    {
        [Key] public int Id { get; set; }
        public string FlagText { get; set; } = string.Empty;
    }
}
