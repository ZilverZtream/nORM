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
/// Contract for TimeSpan / bool / enum / char ordering (Query/LINQ matrix cell).
///
/// All four order CONSISTENTLY with .NET's default comparer. TimeSpan is stored as canonical 'c' TEXT
/// and coerced to a numeric sort key, so multi-day and negative durations order by value (not lexically,
/// which would put "10.00:00:00" before "9.23:59:59"). bool orders false &lt; true. enum orders by its
/// UNDERLYING integral value including negative members (not declaration order). char orders by code
/// point (SQLite BINARY over UTF-8 preserves code-point order, including non-ASCII BMP characters).
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScalarTypeOrderingContractTests
{
    private enum E { A = 5, B = -3, C = 0, D = 100 }

    [Table("TsOrdContract")] private sealed class TS { [Key] public int Id { get; set; } public TimeSpan Val { get; set; } }
    [Table("BoolOrdContract")] private sealed class BL { [Key] public int Id { get; set; } public bool Val { get; set; } }
    [Table("EnumOrdContract")] private sealed class EN { [Key] public int Id { get; set; } public E Val { get; set; } }
    [Table("CharOrdContract")] private sealed class CH { [Key] public int Id { get; set; } public char Val { get; set; } }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = ddl;
        c.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task OrderBy_timespan_matches_dotnet_including_multiday_and_negative()
    {
        var rows = new (int Id, TimeSpan Val)[]
        {
            (1, TimeSpan.FromDays(10)),
            (2, new TimeSpan(9, 23, 59, 59)),
            (3, TimeSpan.FromDays(-5)),
            (4, TimeSpan.Zero),
            (5, new TimeSpan(0, 0, 0, 0, 500)),   // 0.5s
            (6, TimeSpan.FromDays(1)),
            (7, TimeSpan.FromDays(-1)),
        };
        using var ctx = new DbContext(Open("CREATE TABLE TsOrdContract (Id INTEGER PRIMARY KEY, Val TEXT);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new TS { Id = id, Val = val });

        var norm = ((INormQueryable<TS>)ctx.Query<TS>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        Assert.Equal(new[] { 3, 7, 4, 5, 6, 2, 1 }, norm);
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
        // Not the lexical text order of the canonical 'c' strings (which mis-orders -1 vs -5 and 10 vs 9).
        Assert.NotEqual(new[] { 7, 3, 4, 5, 6, 1, 2 }, norm);
    }

    [Fact]
    public async Task OrderBy_bool_matches_dotnet()
    {
        var rows = new (int Id, bool Val)[] { (1, true), (2, false), (3, true), (4, false) };
        using var ctx = new DbContext(Open("CREATE TABLE BoolOrdContract (Id INTEGER PRIMARY KEY, Val INTEGER);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new BL { Id = id, Val = val });

        var norm = ((INormQueryable<BL>)ctx.Query<BL>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 2, 4, 1, 3 }, norm);   // false (2,4) before true (1,3)
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
    }

    [Fact]
    public async Task OrderBy_enum_orders_by_underlying_signed_value()
    {
        var rows = new (int Id, E Val)[] { (1, E.A), (2, E.B), (3, E.C), (4, E.D) };   // 5, -3, 0, 100
        using var ctx = new DbContext(Open("CREATE TABLE EnumOrdContract (Id INTEGER PRIMARY KEY, Val INTEGER);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new EN { Id = id, Val = val });

        var norm = ((INormQueryable<EN>)ctx.Query<EN>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 2, 3, 1, 4 }, norm);   // -3, 0, 5, 100
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
    }

    [Fact]
    public async Task OrderBy_char_matches_dotnet_code_point_order()
    {
        var rows = new (int Id, char Val)[]
        {
            (1, 'z'), (2, 'A'), (3, '0'), (4, ' '), (5, '~'), (6, 'é'), (7, 'Z'),
        };
        using var ctx = new DbContext(Open("CREATE TABLE CharOrdContract (Id INTEGER PRIMARY KEY, Val TEXT);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new CH { Id = id, Val = val });

        var norm = ((INormQueryable<CH>)ctx.Query<CH>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        // ' '(32) '0'(48) 'A'(65) 'Z'(90) 'z'(122) '~'(126) U+00E9 (233, a non-ASCII BMP char)
        Assert.Equal(new[] { 4, 3, 2, 7, 1, 5, 6 }, norm);
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
    }
}
