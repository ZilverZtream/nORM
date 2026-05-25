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
/// Strict pin for <c>Math.ScaleB(double, int)</c> in projection. Spec:
/// returns x * 2^n. SQLite's pow() built-in (3.35+ math extension)
/// covers this directly via POW(2.0, n).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathScaleBTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmsbItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL, N INTEGER NOT NULL);
            INSERT INTO PmsbItem VALUES
                (1, 1.0,  3),    -- 1 * 2^3 = 8
                (2, 1.5,  4),    -- 1.5 * 16 = 24
                (3, -2.0, 2),    -- -2 * 4 = -8
                (4, 5.0, -1);    -- 5 * 0.5 = 2.5
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmsbItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_ScaleB_double_int_columns_returns_x_times_two_to_n_per_row()
    {
        var r = await _ctx.Query<PmsbItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.ScaleB(p.X, p.N) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(Math.ScaleB(1.0, 3), r[0].V, 6);
        Assert.Equal(Math.ScaleB(1.5, 4), r[1].V, 6);
        Assert.Equal(Math.ScaleB(-2.0, 2), r[2].V, 6);
        Assert.Equal(Math.ScaleB(5.0, -1), r[3].V, 6);
    }

    [Table("PmsbItem")]
    public sealed class PmsbItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
        public int N { get; set; }
    }
}
