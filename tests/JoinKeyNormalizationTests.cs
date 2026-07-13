using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Join keys must match by VALUE, the way LINQ's comparer does: decimal keys are
/// scale-insensitive (10.5m equals 10.50m) and DateTimeOffset keys are instant-based
/// (same moment at different offsets is equal). SQLite stores both as TEXT, so a raw
/// ON-equality silently drops join pairs whose text differs.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class JoinKeyNormalizationTests
{
    [Table("JoinKey_Order")]
    private class Order
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public decimal Amount { get; set; }
        public DateTimeOffset At { get; set; }
    }

    [Table("JoinKey_Rebate")]
    private class Rebate
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public decimal Amount { get; set; }
        public DateTimeOffset At { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Deliberately scale-variant decimal text and offset-variant DTO text:
            // 10.5 == 10.50 (decimal), and 12:00+02:00 == 10:00+00:00 (instant).
            cmd.CommandText = """
                CREATE TABLE JoinKey_Order (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Amount TEXT NOT NULL,
                    At TEXT NOT NULL
                );
                CREATE TABLE JoinKey_Rebate (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Amount TEXT NOT NULL,
                    At TEXT NOT NULL
                );
                INSERT INTO JoinKey_Order (Amount, At) VALUES
                    ('10.5',  '2020-01-01 12:00:00+02:00'),
                    ('7',     '2020-06-01 00:00:00+00:00');
                INSERT INTO JoinKey_Rebate (Amount, At) VALUES
                    ('10.50', '2020-01-01 10:00:00+00:00'),
                    ('7.0',   '2020-06-01 02:00:00+02:00');
                """;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Order[] OrdersRef =
    {
        new Order { Id = 1, Amount = 10.5m, At = new DateTimeOffset(2020, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)) },
        new Order { Id = 2, Amount = 7m, At = new DateTimeOffset(2020, 6, 1, 0, 0, 0, TimeSpan.Zero) },
    };

    private static readonly Rebate[] RebatesRef =
    {
        new Rebate { Id = 1, Amount = 10.50m, At = new DateTimeOffset(2020, 1, 1, 10, 0, 0, TimeSpan.Zero) },
        new Rebate { Id = 2, Amount = 7.0m, At = new DateTimeOffset(2020, 6, 1, 2, 0, 0, TimeSpan.FromHours(2)) },
    };

    [Fact]
    public void Inner_join_on_decimal_key_matches_scale_variants()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var expected = OrdersRef.Join(RebatesRef, o => o.Amount, r => r.Amount, (o, r) => new { o.Id, RebateId = r.Id })
            .OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.RebateId}").ToList();
        var actual = ctx.Query<Order>()
            .Join(ctx.Query<Rebate>(), o => o.Amount, r => r.Amount, (o, r) => new { o.Id, RebateId = r.Id })
            .ToList().OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.RebateId}").ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Inner_join_on_datetimeoffset_key_matches_same_instant()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var expected = OrdersRef.Join(RebatesRef, o => o.At, r => r.At, (o, r) => new { o.Id, RebateId = r.Id })
            .OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.RebateId}").ToList();
        var actual = ctx.Query<Order>()
            .Join(ctx.Query<Rebate>(), o => o.At, r => r.At, (o, r) => new { o.Id, RebateId = r.Id })
            .ToList().OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.RebateId}").ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Group_join_on_datetimeoffset_key_matches_same_instant()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var expected = OrdersRef.GroupJoin(RebatesRef, o => o.At, r => r.At, (o, rs) => new { o.Id, Count = rs.Count() })
            .OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.Count}").ToList();
        var actual = ctx.Query<Order>()
            .GroupJoin(ctx.Query<Rebate>(), o => o.At, r => r.At, (o, rs) => new { o.Id, Count = rs.Count() })
            .ToList().OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.Count}").ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Group_join_on_decimal_key_matches_scale_variants()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var expected = OrdersRef.GroupJoin(RebatesRef, o => o.Amount, r => r.Amount, (o, rs) => new { o.Id, Count = rs.Count() })
            .OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.Count}").ToList();
        var actual = ctx.Query<Order>()
            .GroupJoin(ctx.Query<Rebate>(), o => o.Amount, r => r.Amount, (o, rs) => new { o.Id, Count = rs.Count() })
            .ToList().OrderBy(x => x.Id).Select(x => $"{x.Id}:{x.Count}").ToList();
        Assert.Equal(expected, actual);
    }
}
