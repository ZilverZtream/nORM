using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for decimal ordering and precision (Query/LINQ matrix cell: OrderBy x decimal operand).
///
/// SQLite has no native DECIMAL type; nORM stores decimal as canonical TEXT. Ordering is numeric (not
/// lexical) and matches .NET across magnitude, sign, zero, trailing-zero equality, and precision to
/// ~16 significant digits. STORAGE and EXACT EQUALITY / GROUP BY / DISTINCT are full 28-digit precision
/// (canonical-text keys). ORDER BY on SQLite currently coerces the sort key via CAST AS REAL, so two
/// decimals that agree to ~16 significant digits but differ beyond that can sort by the tiebreak rather
/// than by true value - a known precision asymmetry vs the exact-equality path (see NH-0102). Server
/// providers (SQL Server / PostgreSQL / MySQL) order the native DECIMAL exactly.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DecimalOrderingContractTests
{
    [Table("DecOrdContract")]
    private sealed class D { [Key] public int Id { get; set; } public decimal Val { get; set; } }

    private static SqliteConnection Open()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = "CREATE TABLE DecOrdContract (Id INTEGER PRIMARY KEY, Val TEXT);";
        c.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task OrderBy_decimal_matches_dotnet_for_magnitude_sign_and_moderate_precision()
    {
        var rows = new (int Id, decimal Val)[]
        {
            (1, 2m),
            (2, 10.5m),                     // magnitude: not lexical ('10.5' < '2.0' lexically)
            (3, -3.14m),
            (4, 0m),
            (5, 1.10m),                     // trailing zero == 1.1
            (6, 1.1m),
            (7, -10m),
            (8, 100000000000000000.5m),     // 18 integer digits
            (9, 1.00000000000001m),         // ~15 sig digits: distinct under REAL, ordered correctly
            (10, 1.00000000000002m),
        };
        using var ctx = new DbContext(Open(), new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new D { Id = id, Val = val });

        var norm = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        Assert.Equal(new[] { 7, 3, 4, 9, 10, 5, 6, 1, 2, 8 }, norm);
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
    }

    [Fact]
    public async Task Decimal_storage_and_exact_equality_are_full_precision()
    {
        // Two decimals that agree to 16 significant digits but differ at the 17th. These collapse to the
        // same double, so they would be indistinguishable under a REAL coercion - but storage and exact
        // equality use full-precision canonical text, so they round-trip and filter precisely.
        var a = 1.00000000000000005m;
        var b = 1.00000000000000006m;
        using var ctx = new DbContext(Open(), new SqliteProvider());
        await ctx.InsertAsync(new D { Id = 1, Val = a });
        await ctx.InsertAsync(new D { Id = 2, Val = b });

        var back = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().OrderBy(e => e.Id).Select(e => e.Val).ToList();
        Assert.Equal(a, back[0]);
        Assert.Equal(b, back[1]);
        Assert.NotEqual(back[0], back[1]);   // full-precision round-trip: not merged

        // Exact equality distinguishes them (full-precision canonical key), unlike a REAL coercion.
        var hitA = ((INormQueryable<D>)ctx.Query<D>())
            .AsNoTracking().Where(e => e.Val == a).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, hitA);
    }
}
