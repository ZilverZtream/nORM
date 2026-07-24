using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Differential harness that closes the "read fast path silently disagrees with the full translator"
/// family (decimal scale, string collation, DateTimeOffset, TimeSpan — commits 2b2ebc67 … 3b0ebbe3).
///
/// <para>The full translator is nORM's reference implementation: it normalizes every awkward stored
/// type (canonical decimal text, UTC-instant for DateTimeOffset, fractional-seconds for TimeSpan, …).
/// A read fast path is only an optimization, so it MUST return the identical result — any divergence is
/// a fast-path bug. This test seeds each type's column with <b>non-canonical TEXT via raw SQL</b> (the
/// LINQ-parity fuzzer can't: it seeds through nORM's own canonicalizing writes), then for a battery of
/// predicates asserts that the simple-<c>Where</c>, filtered-ordered, and <c>Count</c> fast paths agree
/// with the full translator (forced by a projection). This catches the whole class — current and future
/// types — instead of one type at a time. To extend coverage for a new type, add a column, a seed value
/// with an equal-but-differently-formatted sibling, and predicates to <see cref="Predicates"/>.</para>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FastPathFullTranslatorDifferentialTests
{
    [Table("Diff_Test")]
    public sealed class DiffRow
    {
        [Key] public int Id { get; set; }
        public decimal Dec { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Ts { get; set; }
        public string Str { get; set; } = "";
        public int Num { get; set; }
        public bool Flag { get; set; }
        public DateOnly Do { get; set; }
        [Column("T_o")] public TimeOnly To { get; set; }
    }

    // Each row carries a NON-CANONICAL text form so a raw `col = @p` / `col < @p` on the fast path would
    // diverge from the full translator's normalized comparison. Columns Dec/Dto/Ts/Str are TEXT.
    // Dto rows 1&2 are the same instant in different offsets; Ts rows 1&2 are the same duration in
    // different fractional formats; Dec has trailing-zero scales; Ts rows 5&6 are multi-day (lexical trap).
    // DateOnly rows are canonical "yyyy-MM-dd" (lexical == chronological). TimeOnly rows 1&2 are the same
    // time in canonical and extra-fraction TEXT ("12:00:00" vs "12:00:00.0000000") — the equality trap.
    private static readonly string SeedSql =
        "CREATE TABLE Diff_Test (Id INTEGER PRIMARY KEY, Dec TEXT NOT NULL, Dto TEXT NOT NULL, " +
        "Ts TEXT NOT NULL, Str TEXT NOT NULL, Num INTEGER NOT NULL, Flag INTEGER NOT NULL, " +
        "Do TEXT NOT NULL, T_o TEXT NOT NULL);" +
        "INSERT INTO Diff_Test (Id, Dec, Dto, Ts, Str, Num, Flag, Do, T_o) VALUES " +
        "(1, '24.500',  '2026-05-25 12:30:45.123+00:00', '1.00:00:00',          'abc', 5,  1, '2026-05-25', '12:00:00')," +
        "(2, '24.5',    '2026-05-25 14:30:45.123+02:00', '1.00:00:00.0000000',  'xyz', 5,  0, '2026-05-25', '12:00:00.0000000')," +
        "(3, '100.00',  '2026-05-25 09:00:00.000+00:00', '2.00:00:00',          'abc', 10, 1, '2026-05-26', '13:30:00')," +
        "(4, '79.99',   '2026-05-25 12:30:46.000+00:00', '0.12:00:00',          'def', 0,  0, '2026-05-24', '00:00:00')," +
        "(5, '429.000', '2026-05-24 00:00:00.000+00:00', '10.00:00:00',         'ghi', 20, 1, '2026-05-25', '23:59:59')," +
        "(6, '9.9',     '2026-05-26 23:59:59.999+00:00', '9.23:59:59',          'jkl', -3, 0, '2026-05-27', '06:15:00');";

    // Predicates over the awkward types. Built as Expression<Func<DiffRow,bool>> so both the fast path
    // (Where / Where+OrderBy / Count) and the full translator (Where+Select) evaluate the same tree.
    private static readonly (string Name, Expression<Func<DiffRow, bool>> Pred)[] Predicates =
    {
        ("dec == 24.5",        r => r.Dec == 24.5m),
        ("dec == 100",         r => r.Dec == 100m),
        ("dec == 429",         r => r.Dec == 429m),
        ("dec < 100",          r => r.Dec < 100m),
        ("dec >= 79.99",       r => r.Dec >= 79.99m),
        ("dto == utc-dt",      r => r.Dto == new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc)),
        ("dto == dto",         r => r.Dto == new DateTimeOffset(2026, 5, 25, 12, 30, 45, 123, TimeSpan.Zero)),
        ("ts == 1day",         r => r.Ts == TimeSpan.FromDays(1)),
        ("ts < 2day",          r => r.Ts < TimeSpan.FromDays(2)),
        ("ts > 9d23h59m59s",   r => r.Ts > new TimeSpan(9, 23, 59, 59)),
        ("str == abc",         r => r.Str == "abc"),
        ("num == 5",           r => r.Num == 5),
        ("num < 10",           r => r.Num < 10),
        ("flag == true",       r => r.Flag),
        ("dateonly == 5-25",   r => r.Do == new DateOnly(2026, 5, 25)),
        ("dateonly < 5-26",    r => r.Do < new DateOnly(2026, 5, 26)),
        ("timeonly == 12:00",  r => r.To == new TimeOnly(12, 0, 0)),
        ("timeonly < 13:00",   r => r.To < new TimeOnly(13, 0, 0)),
    };

    // The TRUE values the seeded TEXT represents (rows 1&2 share a decimal value, a DateTimeOffset instant,
    // a TimeSpan duration, and a TimeOnly — each stored in a different non-canonical TEXT form). Running the
    // predicate over this LINQ-to-objects reference is the ABSOLUTE oracle: it catches a bug where BOTH the
    // fast path and the full translator agree on the wrong answer (which a consistency-only differential
    // cannot — that was how the product-wide TimeOnly bug slipped a purely fast-path-vs-full-translator check).
    private static readonly DiffRow[] Reference =
    {
        new() { Id = 1, Dec = 24.500m, Dto = new DateTimeOffset(2026, 5, 25, 12, 30, 45, 123, TimeSpan.Zero),      Ts = TimeSpan.FromDays(1),      Str = "abc", Num = 5,  Flag = true,  Do = new DateOnly(2026, 5, 25), To = new TimeOnly(12, 0, 0) },
        new() { Id = 2, Dec = 24.5m,   Dto = new DateTimeOffset(2026, 5, 25, 14, 30, 45, 123, TimeSpan.FromHours(2)), Ts = TimeSpan.FromDays(1),    Str = "xyz", Num = 5,  Flag = false, Do = new DateOnly(2026, 5, 25), To = new TimeOnly(12, 0, 0) },
        new() { Id = 3, Dec = 100.00m, Dto = new DateTimeOffset(2026, 5, 25, 9, 0, 0, 0, TimeSpan.Zero),           Ts = TimeSpan.FromDays(2),      Str = "abc", Num = 10, Flag = true,  Do = new DateOnly(2026, 5, 26), To = new TimeOnly(13, 30, 0) },
        new() { Id = 4, Dec = 79.99m,  Dto = new DateTimeOffset(2026, 5, 25, 12, 30, 46, 0, TimeSpan.Zero),        Ts = TimeSpan.FromHours(12),    Str = "def", Num = 0,  Flag = false, Do = new DateOnly(2026, 5, 24), To = new TimeOnly(0, 0, 0) },
        new() { Id = 5, Dec = 429.000m,Dto = new DateTimeOffset(2026, 5, 24, 0, 0, 0, 0, TimeSpan.Zero),           Ts = TimeSpan.FromDays(10),     Str = "ghi", Num = 20, Flag = true,  Do = new DateOnly(2026, 5, 25), To = new TimeOnly(23, 59, 59) },
        new() { Id = 6, Dec = 9.9m,    Dto = new DateTimeOffset(2026, 5, 26, 23, 59, 59, 999, TimeSpan.Zero),      Ts = new TimeSpan(9, 23, 59, 59), Str = "jkl", Num = -3, Flag = false, Do = new DateOnly(2026, 5, 27), To = new TimeOnly(6, 15, 0) },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand()) { c.CommandText = SeedSql; c.ExecuteNonQuery(); }
        return new DbContext(cn, new SqliteProvider());
    }

    public static IEnumerable<object[]> Cases() => Predicates.Select(p => new object[] { p.Name });

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Fast_paths_agree_with_full_translator(string name)
    {
        var predicate = Predicates.First(p => p.Name == name).Pred;
        using var ctx = NewCtx();

        // ABSOLUTE oracle: the predicate over the true in-memory values (LINQ-to-objects).
        var oracle = Reference.Where(predicate.Compile()).Select(r => r.Id).OrderBy(x => x).ToList();

        // Full translator (the projection defeats every read fast path). Checked against the absolute
        // oracle so a product-wide bug (fast path AND full translator both wrong) is caught, not just a
        // fast-path-vs-full-translator divergence.
        var fullTranslator = (await ctx.Query<DiffRow>().Where(predicate).Select(r => new { r.Id }).ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToList();
        Assert.Equal(oracle, fullTranslator);

        // Simple-Where fast path (async list, no ordering).
        var simpleWhere = (await ctx.Query<DiffRow>().Where(predicate).ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToList();
        Assert.Equal(oracle, simpleWhere);

        // Filtered-ordered fast path (Where + single OrderBy on Id).
        var filteredOrdered = (await ctx.Query<DiffRow>().Where(predicate).OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Id).ToList();
        Assert.Equal(oracle, filteredOrdered);

        // Direct-count fast path.
        var count = await ctx.Query<DiffRow>().CountAsync(predicate);
        Assert.Equal(oracle.Count, count);
    }
}
