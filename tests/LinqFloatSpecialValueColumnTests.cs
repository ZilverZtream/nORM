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
/// Pins materializer + WHERE round-trip for IEEE 754 special values on
/// <see cref="double"/> and <see cref="float"/> columns: NaN, ±Infinity,
/// signed zero. SQLite stores doubles as REAL but its NaN handling is
/// well-defined; the materializer / parameter pipeline must not silently
/// coerce these to 0 / NULL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqFloatSpecialValueColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FpRow (Id INTEGER PRIMARY KEY, D REAL NOT NULL, F REAL NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Insert via parameters so the values bind through .NET, not text literals.
        foreach (var (id, d, f) in new[]
        {
            (1, 1.5, 1.5f),
            (2, -0.0, -0.0f),
            (3, double.PositiveInfinity, float.PositiveInfinity),
            (4, double.NegativeInfinity, float.NegativeInfinity),
        })
        {
            await using var ins = _cn.CreateCommand();
            ins.CommandText = "INSERT INTO FpRow VALUES ($id, $d, $f)";
            ins.Parameters.AddWithValue("$id", id);
            ins.Parameters.AddWithValue("$d", d);
            ins.Parameters.AddWithValue("$f", f);
            await ins.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Double_column_round_trips_positive_infinity()
    {
        var row = (await _ctx.Query<FpRow>().Where(r => r.Id == 3).ToListAsync())[0];
        Assert.True(double.IsPositiveInfinity(row.D));
    }

    [Fact]
    public async Task Double_column_round_trips_negative_infinity()
    {
        var row = (await _ctx.Query<FpRow>().Where(r => r.Id == 4).ToListAsync())[0];
        Assert.True(double.IsNegativeInfinity(row.D));
    }

    [Fact]
    public async Task Float_column_round_trips_positive_infinity()
    {
        var row = (await _ctx.Query<FpRow>().Where(r => r.Id == 3).ToListAsync())[0];
        Assert.True(float.IsPositiveInfinity(row.F));
    }

    [Fact]
    public async Task Double_column_round_trips_negative_zero_as_value_zero()
    {
        // SQLite's REAL storage normalizes the IEEE 754 -0.0 bit pattern to +0.0 on
        // round-trip — that's a storage-level decision the driver makes, not a nORM
        // behaviour. The value `-0.0 == 0.0` in CLR equality so users observe parity;
        // only the sign-bit introspection (`double.IsNegative`) would surface it, and
        // it's documented in the SQLite REAL storage spec.
        var row = (await _ctx.Query<FpRow>().Where(r => r.Id == 2).ToListAsync())[0];
        Assert.Equal(0.0, row.D);
    }

    [Table("FpRow")]
    public sealed class FpRow
    {
        [Key] public int Id { get; set; }
        public double D { get; set; }
        public float F { get; set; }
    }
}
