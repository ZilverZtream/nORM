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
/// Verifies set-operation (UNION / INTERSECT / EXCEPT) dedup over TEXT-stored temporal columns on
/// SQLite is by VALUE, not by lexical TEXT. Set operators dedup on the projection key
/// (SelectClauseVisitor → provider <c>ExactKeySql</c> / DTO instant-key), so a type whose exact-key
/// canonicalization is missing would treat the same value in a different TEXT scale as two distinct
/// set members. EXCEPT is the sharpest probe: a same-value/different-scale row in the left side that
/// a raw-text compare cannot find on the right survives the difference, inflating the result.
///
/// <para>Rows 1 &amp; 2 of each column are the same value in a different TEXT form; the halves
/// <c>Id &lt;= 3</c> (left) and <c>Id &gt;= 2</c> (right) overlap so every left value is present on the
/// right by value → a correct EXCEPT is empty, a lexical one is not. Checked against a LINQ-to-objects
/// oracle over the true values.</para>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOperationTemporalDedupTests
{
    [Table("SetOpTemporal_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public DateTime Dt { get; set; }
        public TimeSpan Ts { get; set; }
        public DateTimeOffset Dto { get; set; }
        [Column("T_o")] public TimeOnly To { get; set; }
    }

    // Rows 1&2 share a value written in a different TEXT scale/offset; rows 3&4 are distinct.
    private const string SeedSql =
        "CREATE TABLE SetOpTemporal_Test (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, Ts TEXT NOT NULL, " +
        "Dto TEXT NOT NULL, T_o TEXT NOT NULL);" +
        "INSERT INTO SetOpTemporal_Test (Id, Dt, Ts, Dto, T_o) VALUES " +
        "(1, '2026-05-25 12:00:00',         '1.00:00:00',         '2026-05-25 12:00:00+00:00', '12:00:00')," +
        "(2, '2026-05-25 12:00:00.0000000', '1.00:00:00.0000000', '2026-05-25 14:00:00+02:00', '12:00:00.0000000')," +
        "(3, '2026-05-25 13:00:00',         '2.00:00:00',         '2026-05-25 13:00:00+00:00', '13:00:00')," +
        "(4, '2026-05-25 14:00:00',         '0.06:00:00',         '2026-05-25 15:00:00+00:00', '14:00:00');";

    private static readonly Row[] Reference =
    {
        new() { Id = 1, Dt = new DateTime(2026, 5, 25, 12, 0, 0), Ts = TimeSpan.FromDays(1), Dto = new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero),         To = new TimeOnly(12, 0, 0) },
        new() { Id = 2, Dt = new DateTime(2026, 5, 25, 12, 0, 0), Ts = TimeSpan.FromDays(1), Dto = new DateTimeOffset(2026, 5, 25, 14, 0, 0, TimeSpan.FromHours(2)), To = new TimeOnly(12, 0, 0) },
        new() { Id = 3, Dt = new DateTime(2026, 5, 25, 13, 0, 0), Ts = TimeSpan.FromDays(2), Dto = new DateTimeOffset(2026, 5, 25, 13, 0, 0, TimeSpan.Zero),         To = new TimeOnly(13, 0, 0) },
        new() { Id = 4, Dt = new DateTime(2026, 5, 25, 14, 0, 0), Ts = TimeSpan.FromHours(6), Dto = new DateTimeOffset(2026, 5, 25, 15, 0, 0, TimeSpan.Zero),        To = new TimeOnly(14, 0, 0) },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand()) { c.CommandText = SeedSql; c.ExecuteNonQuery(); }
        return new DbContext(cn, new SqliteProvider());
    }

    private static int Oracle<TAnon>(string op, Func<Row, TAnon> sel)
    {
        var left = Reference.Where(r => r.Id <= 3).Select(sel);
        var right = Reference.Where(r => r.Id >= 2).Select(sel);
        return op switch
        {
            "Union" => left.Union(right).Count(),
            "Intersect" => left.Intersect(right).Count(),
            "Except" => left.Except(right).Count(),
            _ => throw new ArgumentOutOfRangeException(nameof(op)),
        };
    }

    private static async Task<int> Norm<TAnon>(DbContext ctx, string op, Expression<Func<Row, TAnon>> sel)
        where TAnon : class
    {
        var left = ctx.Query<Row>().Where(r => r.Id <= 3).Select(sel);
        var right = ctx.Query<Row>().Where(r => r.Id >= 2).Select(sel);
        var q = op switch
        {
            "Union" => left.Union(right),
            "Intersect" => left.Intersect(right),
            "Except" => left.Except(right),
            _ => throw new ArgumentOutOfRangeException(nameof(op)),
        };
        return (await q.ToListAsync()).Count;
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var col in new[] { "Dt", "Ts", "Dto", "To" })
            foreach (var op in new[] { "Union", "Intersect", "Except" })
                yield return new object[] { col, op };
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Set_operation_dedup_is_by_value(string col, string op)
    {
        using var ctx = NewCtx();
        var (norm, oracle) = col switch
        {
            "Dt" => (await Norm(ctx, op, r => new { r.Dt }), Oracle(op, r => new { r.Dt })),
            "Ts" => (await Norm(ctx, op, r => new { r.Ts }), Oracle(op, r => new { r.Ts })),
            "Dto" => (await Norm(ctx, op, r => new { r.Dto }), Oracle(op, r => new { r.Dto })),
            "To" => (await Norm(ctx, op, r => new { r.To }), Oracle(op, r => new { r.To })),
            _ => throw new ArgumentOutOfRangeException(nameof(col)),
        };
        Assert.Equal(oracle, norm);
    }
}
