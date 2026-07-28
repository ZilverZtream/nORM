using System;
using System.Collections.Generic;
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
/// Decimal modulo must compute the true remainder, matching C#. SQLite's `%` operator casts both
/// operands to integer first, so a bare `decimalCol % n` silently returned the INTEGER remainder
/// (10.5 % 3 == 0 instead of 1.5) — a silent-wrong result. Decimal modulo now routes through the
/// same provider floating-point-remainder hook the double/float path uses.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DecimalModuloTranslationTests
{
    [Table("DmRow")] public class DmRow { [Key] public int Id { get; set; } public decimal V { get; set; } }

    private static async Task<DbContext> NewCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "CREATE TABLE DmRow (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);"; cmd.ExecuteNonQuery(); }
        return await Task.FromResult(new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Projected_decimal_modulo_keeps_fractional_remainder()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        ctx.Add(new DmRow { Id = 1, V = 10.5m });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<DmRow>().Select(x => x.V % 3m).ToList().Single();
        Assert.Equal(10.5m % 3m, r);   // 1.5, not the integer 10 % 3 == 1
        Assert.Equal(1.5m, r);
    }

    [Fact]
    public async Task Decimal_modulo_predicate_keeps_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        ctx.Add(new DmRow { Id = 1, V = 10.5m });
        await ctx.SaveChangesAsync();
        // 10.5 % 3 == 1.5 (> 1). The integer-truncated 10 % 3 == 1 would fail 1 > 1 and drop the row.
        var ids = ctx.Query<DmRow>().Where(x => x.V % 3m > 1m).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());
    }
}
