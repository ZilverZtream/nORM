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
/// Strict pin for <c>Math.IEEERemainder(double, double)</c> in projection.
/// Spec: x - y * round(x/y, MidpointRounding.ToEven) -- banker's rounding
/// on the quotient. SQLite's native ROUND() uses round-half-away-from-zero,
/// so a naive `x - y*ROUND(x/y)` disagrees at exact-half ties. Test data
/// deliberately exercises both common (non-tie) and exact-tie shapes so
/// the implementation must emit a banker's-rounding equivalent.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathIEEERemainderTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmieItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL, Y REAL NOT NULL);
            INSERT INTO PmieItem VALUES
                (1, 10.0, 3.0),     -- 10/3=3.333; banker=3; remainder=1
                (2, 5.0,  2.0),     -- 5/2=2.5;    banker=2 (even); remainder=1
                (3, 7.0,  2.0),     -- 7/2=3.5;    banker=4 (even); remainder=-1
                (4, -7.0, 2.0),     -- -7/2=-3.5;  banker=-4;       remainder=1
                (5, 9.5,  2.0);     -- 9.5/2=4.75; banker=5;        remainder=-0.5
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmieItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_IEEERemainder_two_columns_matches_dotnet_per_row()
    {
        var r = await _ctx.Query<PmieItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.IEEERemainder(p.X, p.Y) }).ToListAsync();
        Assert.Equal(5, r.Count);
        Assert.Equal(Math.IEEERemainder(10.0, 3.0), r[0].V, 6);
        Assert.Equal(Math.IEEERemainder(5.0, 2.0), r[1].V, 6);
        Assert.Equal(Math.IEEERemainder(7.0, 2.0), r[2].V, 6);
        Assert.Equal(Math.IEEERemainder(-7.0, 2.0), r[3].V, 6);
        Assert.Equal(Math.IEEERemainder(9.5, 2.0), r[4].V, 6);
    }

    [Table("PmieItem")]
    public sealed class PmieItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
    }
}
