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
/// Strict pin for <c>Math.Clamp</c> (double + int overloads) in projection.
/// Spec: Clamp(v, min, max) = Min(Max(v, min), max), assuming min &lt;= max
/// (the .NET ArgumentException for min &gt; max is a runtime guard, not part
/// of the expression semantics). SQLite's MIN/MAX scalar functions cover
/// this directly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathClampTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmclItem (Id INTEGER PRIMARY KEY, V REAL NOT NULL, Iv INTEGER NOT NULL);
            INSERT INTO PmclItem VALUES
                (1, -5.0,  -8),    -- both below low bound
                (2,  3.5,   3),    -- in range
                (3, 12.0,  20);    -- both above high bound
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmclItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Clamp_double_clips_to_bounds_per_row()
    {
        var r = await _ctx.Query<PmclItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Clamp(p.V, 0.0, 10.0) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(0.0, r[0].V, 6);    // -5 clamped to 0
        Assert.Equal(3.5, r[1].V, 6);    // in range
        Assert.Equal(10.0, r[2].V, 6);   // 12 clamped to 10
    }

    [Fact]
    public async Task Select_Math_Clamp_int_clips_to_bounds_per_row()
    {
        var r = await _ctx.Query<PmclItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Clamp(p.Iv, 0, 10) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(0, r[0].V);    // -8 clamped to 0
        Assert.Equal(3, r[1].V);    // in range
        Assert.Equal(10, r[2].V);   // 20 clamped to 10
    }

    [Table("PmclItem")]
    public sealed class PmclItem
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
        public int Iv { get; set; }
    }
}
