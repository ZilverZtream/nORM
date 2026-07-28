using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: Join keys whose CLR type is a VALUE-CONVERTER model type. BuildOnEquality routes
/// decimal / TimeOnly / DateTimeOffset join keys through provider canonicalization keyed on the key
/// selector body's CLR type — but a converter property's body type is the MODEL type (Money), not the
/// stored provider type (decimal). If the canonicalization is skipped, the ON clause compares raw TEXT
/// and silently drops rows that are numerically equal but stored at a different scale.
/// Oracle = the same join lambda over in-memory Lists (values read through the same converter).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class JoinConverterKeyAdversarialTests
{
    public readonly record struct Money(decimal Amount);

    private sealed class MoneyConverter : ValueConverter<Money, decimal>
    {
        public override object ConvertToProvider(Money value) => value.Amount;
        public override object ConvertFromProvider(decimal value) => new Money(value);
    }

    public enum Status { Open, Closed, Pending }

    private sealed class StatusStringConverter : ValueConverter<Status, string>
    {
        public override object ConvertToProvider(Status value) => value.ToString();
        public override object ConvertFromProvider(string value) => Enum.Parse<Status>(value);
    }

    [Table("JcOrderE")]
    public class OrderE { [Key] public int Id { get; set; } public Status S { get; set; } public string Info { get; set; } = ""; }
    [Table("JcRateE")]
    public class RateE { [Key] public int Id { get; set; } public Status S { get; set; } public string Label { get; set; } = ""; }

    [Table("JcOrderD")]
    public class OrderD { [Key] public int Id { get; set; } public Money Amount { get; set; } public string Info { get; set; } = ""; }
    [Table("JcRateD")]
    public class RateD { [Key] public int Id { get; set; } public Money Amount { get; set; } public string Label { get; set; } = ""; }

    // Plain-decimal control (no converter): key type IS decimal, so ExactKeySql applies.
    [Table("JcOrderP")]
    public class OrderP { [Key] public int Id { get; set; } public decimal Amount { get; set; } public string Info { get; set; } = ""; }
    [Table("JcRateP")]
    public class RateP { [Key] public int Id { get; set; } public decimal Amount { get; set; } public string Label { get; set; } = ""; }

    private static DbContext NewCtx(SqliteConnection cn, DbContextOptions? opts, params string[] ddl)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = string.Join(";", ddl) + ";";
            cmd.ExecuteNonQuery();
        }
        return opts == null ? new DbContext(cn, new SqliteProvider()) : new DbContext(cn, new SqliteProvider(), opts);
    }

    /// <summary>
    /// CONTROL: plain decimal join key with scale-different TEXT ('10.5' vs '10.50', '20.0' vs '20').
    /// ExactKeySql canonicalizes both sides -> the numerically equal keys join. Should PASS.
    /// </summary>
    [Fact]
    public void PlainDecimal_join_key_scale_insensitive_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn, null,
            "CREATE TABLE JcOrderP (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Info TEXT NOT NULL)",
            "CREATE TABLE JcRateP  (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Label TEXT NOT NULL)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO JcOrderP VALUES (1,'10.5','o1'),(2,'20.0','o2');" +
                "INSERT INTO JcRateP  VALUES (1,'10.50','r1'),(2,'20','r2');";
            cmd.ExecuteNonQuery();
        }

        var actual = ctx.Query<OrderP>()
            .Join(ctx.Query<RateP>(), o => o.Amount, r => r.Amount, (o, r) => o.Info + "|" + r.Label)
            .ToList().OrderBy(s => s).ToList();

        var orders = new List<OrderP> { new() { Id = 1, Amount = 10.5m, Info = "o1" }, new() { Id = 2, Amount = 20.0m, Info = "o2" } };
        var rates = new List<RateP> { new() { Id = 1, Amount = 10.50m, Label = "r1" }, new() { Id = 2, Amount = 20m, Label = "r2" } };
        var expected = orders.Join(rates, o => o.Amount, r => r.Amount, (o, r) => o.Info + "|" + r.Label).OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// CONVERTER-DECIMAL join key with scale-different TEXT. In C# Money(10.5m) == Money(10.50m) (decimal
    /// equality is value-based), so the oracle join matches both pairs. If nORM skips ExactKeySql because
    /// the key type is Money (not decimal), the ON clause compares raw TEXT and returns ZERO rows.
    /// </summary>
    [Fact]
    public void ConverterDecimal_join_key_scale_insensitive_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn, new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<OrderD>().Property(e => e.Amount).HasConversion(new MoneyConverter());
                mb.Entity<RateD>().Property(e => e.Amount).HasConversion(new MoneyConverter());
            }
        },
            "CREATE TABLE JcOrderD (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Info TEXT NOT NULL)",
            "CREATE TABLE JcRateD  (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Label TEXT NOT NULL)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO JcOrderD VALUES (1,'10.5','o1'),(2,'20.0','o2');" +
                "INSERT INTO JcRateD  VALUES (1,'10.50','r1'),(2,'20','r2');";
            cmd.ExecuteNonQuery();
        }

        var actual = ctx.Query<OrderD>()
            .Join(ctx.Query<RateD>(), o => o.Amount, r => r.Amount, (o, r) => o.Info + "|" + r.Label)
            .ToList().OrderBy(s => s).ToList();

        var orders = new List<OrderD> { new() { Id = 1, Amount = new Money(10.5m), Info = "o1" }, new() { Id = 2, Amount = new Money(20.0m), Info = "o2" } };
        var rates = new List<RateD> { new() { Id = 1, Amount = new Money(10.50m), Label = "r1" }, new() { Id = 2, Amount = new Money(20m), Label = "r2" } };
        var expected = orders.Join(rates, o => o.Amount, r => r.Amount, (o, r) => o.Info + "|" + r.Label).OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Same converter-decimal join but seeded via ctx.Add with values that C# treats as equal but that
    /// carry different decimal SCALE (10.5m vs 10.50m). Tests whether nORM's own write path plus the join
    /// comparison round-trips a numerically-equal match.
    /// </summary>
    [Fact]
    public async Task ConverterDecimal_join_key_written_by_norm_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn, new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<OrderD>().Property(e => e.Amount).HasConversion(new MoneyConverter());
                mb.Entity<RateD>().Property(e => e.Amount).HasConversion(new MoneyConverter());
            }
        },
            "CREATE TABLE JcOrderD (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Info TEXT NOT NULL)",
            "CREATE TABLE JcRateD  (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Label TEXT NOT NULL)");

        ctx.Add(new OrderD { Id = 1, Amount = new Money(10.5m), Info = "o1" });
        ctx.Add(new OrderD { Id = 2, Amount = new Money(20.0m), Info = "o2" });
        ctx.Add(new RateD { Id = 1, Amount = new Money(10.50m), Label = "r1" });
        ctx.Add(new RateD { Id = 2, Amount = new Money(20m), Label = "r2" });
        await ctx.SaveChangesAsync();

        var actual = ctx.Query<OrderD>()
            .Join(ctx.Query<RateD>(), o => o.Amount, r => r.Amount, (o, r) => o.Info + "|" + r.Label)
            .ToList().OrderBy(s => s).ToList();

        var orders = new List<OrderD> { new() { Id = 1, Amount = new Money(10.5m), Info = "o1" }, new() { Id = 2, Amount = new Money(20.0m), Info = "o2" } };
        var rates = new List<RateD> { new() { Id = 1, Amount = new Money(10.50m), Label = "r1" }, new() { Id = 2, Amount = new Money(20m), Label = "r2" } };
        var expected = orders.Join(rates, o => o.Amount, r => r.Amount, (o, r) => o.Info + "|" + r.Label).OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// The same converter-decimal key defect corrupts a LEFT JOIN (GroupJoin + DefaultIfEmpty): rows that
    /// are numerically equal but stored at a different scale fail the raw-TEXT ON comparison, so the outer
    /// row is (wrongly) reported as UNMATCHED with an empty group.
    /// </summary>
    [Fact]
    public void ConverterDecimal_left_join_key_scale_insensitive_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn, new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<OrderD>().Property(e => e.Amount).HasConversion(new MoneyConverter());
                mb.Entity<RateD>().Property(e => e.Amount).HasConversion(new MoneyConverter());
            }
        },
            "CREATE TABLE JcOrderD (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Info TEXT NOT NULL)",
            "CREATE TABLE JcRateD  (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Label TEXT NOT NULL)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO JcOrderD VALUES (1,'10.5','o1'),(2,'99.0','o2');" +
                "INSERT INTO JcRateD  VALUES (1,'10.50','r1');";
            cmd.ExecuteNonQuery();
        }

        var orders = new List<OrderD> { new() { Id = 1, Amount = new Money(10.5m), Info = "o1" }, new() { Id = 2, Amount = new Money(99.0m), Info = "o2" } };
        var rates = new List<RateD> { new() { Id = 1, Amount = new Money(10.50m), Label = "r1" } };

        var actual = (from o in ctx.Query<OrderD>()
                      join r in ctx.Query<RateD>() on o.Amount equals r.Amount into g
                      from r in g.DefaultIfEmpty()
                      select o.Info + "|" + (r != null ? r.Label : "<none>"))
                      .ToList().OrderBy(s => s).ToList();

        var expected = (from o in orders
                        join r in rates on o.Amount equals r.Amount into g
                        from r in g.DefaultIfEmpty()
                        select o.Info + "|" + (r != null ? r.Label : "<none>"))
                        .OrderBy(s => s).ToList();

        // Oracle: o1 matches r1 ("o1|r1"); o2 unmatched ("o2|<none>").
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// CLEAN-BILL companion: an enum-stored-AS-STRING join key. Both sides store the enum NAME as TEXT and
    /// SQLite's default BINARY collation compares ordinally, so plain `=` correctly matches by name. (Unlike
    /// decimal-as-TEXT, string equality is not scale-sensitive, so no canonicalization is required here.)
    /// </summary>
    [Fact]
    public async Task ConverterEnumString_join_key_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewCtx(cn, new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<OrderE>().Property(e => e.S).HasConversion(new StatusStringConverter());
                mb.Entity<RateE>().Property(e => e.S).HasConversion(new StatusStringConverter());
            }
        },
            "CREATE TABLE JcOrderE (Id INTEGER PRIMARY KEY, S TEXT NOT NULL, Info TEXT NOT NULL)",
            "CREATE TABLE JcRateE  (Id INTEGER PRIMARY KEY, S TEXT NOT NULL, Label TEXT NOT NULL)");

        var orders = new List<OrderE>
        {
            new() { Id = 1, S = Status.Open, Info = "o1" },
            new() { Id = 2, S = Status.Closed, Info = "o2" },
            new() { Id = 3, S = Status.Pending, Info = "o3" },
        };
        var rates = new List<RateE>
        {
            new() { Id = 1, S = Status.Open, Label = "r-open" },
            new() { Id = 2, S = Status.Closed, Label = "r-closed" },
            new() { Id = 3, S = Status.Closed, Label = "r-closed2" },
        };
        foreach (var o in orders) ctx.Add(new OrderE { Id = o.Id, S = o.S, Info = o.Info });
        foreach (var r in rates) ctx.Add(new RateE { Id = r.Id, S = r.S, Label = r.Label });
        await ctx.SaveChangesAsync();

        var actual = ctx.Query<OrderE>()
            .Join(ctx.Query<RateE>(), o => o.S, r => r.S, (o, r) => o.Info + "|" + r.Label)
            .ToList().OrderBy(s => s).ToList();

        var expected = orders.Join(rates, o => o.S, r => r.S, (o, r) => o.Info + "|" + r.Label).OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }
}
