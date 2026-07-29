using System;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Convert.ToInt16/ToByte/ToSByte in a projection must translate server-side under the default Throw
/// policy, exactly like Convert.ToInt32/ToInt64 — the SCV emitter (TryVisitConvertToIntegral) supports
/// them, but the translatability probe (TranslateFunction) only listed ToInt32/ToInt64, so a supported
/// operation spuriously fell to client-eval and threw. All use banker's rounding, matching .NET.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ConvertToSmallIntegralProjectionTests
{
    [Table("CsiRow")] public class Row { [Key] public int Id { get; set; } public double V { get; set; } }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params double[] vals)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "CREATE TABLE CsiRow (Id INTEGER PRIMARY KEY, V REAL NOT NULL);"; cmd.ExecuteNonQuery(); }
        var ctx = new DbContext(cn, new SqliteProvider());
        int id = 1;
        foreach (var v in vals) ctx.Add(new Row { Id = id++, V = v });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    [Fact]
    public async Task ConvertToByte_projection_translates_and_bankers_rounds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, 2.5, 3.5, 10.9);
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Convert.ToByte(r.V)).ToList();
        var oracle = new[] { 2.5, 3.5, 10.9 }.Select(v => Convert.ToByte(v)).ToList(); // 2,4,11 (banker's)
        Assert.Equal(oracle, actual);
    }

    [Fact]
    public async Task ConvertToInt16_projection_translates_and_bankers_rounds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, 2.5, 3.5, -2.5);
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Convert.ToInt16(r.V)).ToList();
        var oracle = new[] { 2.5, 3.5, -2.5 }.Select(v => Convert.ToInt16(v)).ToList(); // 2,4,-2
        Assert.Equal(oracle, actual);
    }

    [Fact]
    public async Task ConvertToSByte_projection_translates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, 5.5, -5.5);
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Convert.ToSByte(r.V)).ToList();
        var oracle = new[] { 5.5, -5.5 }.Select(v => Convert.ToSByte(v)).ToList(); // 6,-6
        Assert.Equal(oracle, actual);
    }
}
