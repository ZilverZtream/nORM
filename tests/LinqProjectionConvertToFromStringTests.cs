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
/// Strict pins for <c>Convert.ToInt32(string)</c> and
/// <c>Convert.ToBoolean(string)</c> in projection. Sister to the int.Parse
/// and bool.Parse cluster: many codebases mix Convert.ToXyz and X.Parse
/// interchangeably for legacy reasons. The semantics match:
///   * Convert.ToInt32(string) == int.Parse(string) for valid input.
///   * Convert.ToBoolean(string) == bool.Parse(string), case-insensitive
///     against "True"/"False" (and accepts whitespace).
/// SQL emission is identical to the Parse equivalents: CAST AS INTEGER
/// for ToInt32, (LOWER(TRIM(s)) = 'true') for ToBoolean (TRIM matches
/// .NET semantics which accept whitespace in bool.Parse and Convert
/// .ToBoolean).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionConvertToFromStringTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PctsItem (Id INTEGER PRIMARY KEY, NumText TEXT NOT NULL, FlagText TEXT NOT NULL);
            INSERT INTO PctsItem VALUES
                (1, '42', 'True'),
                (2, '-17', 'False'),
                (3, '0', 'true');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PctsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Convert_ToInt32_string_column_projects_int_per_row()
    {
        var r = await _ctx.Query<PctsItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, N = Convert.ToInt32(p.NumText) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(42, r[0].N);
        Assert.Equal(-17, r[1].N);
        Assert.Equal(0, r[2].N);
    }

    [Fact]
    public async Task Select_Convert_ToBoolean_string_column_projects_bool_per_row()
    {
        var r = await _ctx.Query<PctsItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, B = Convert.ToBoolean(p.FlagText) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].B);
        Assert.False(r[1].B);
        Assert.True(r[2].B);
    }

    [Table("PctsItem")]
    public sealed class PctsItem
    {
        [Key] public int Id { get; set; }
        public string NumText { get; set; } = string.Empty;
        public string FlagText { get; set; } = string.Empty;
    }
}
