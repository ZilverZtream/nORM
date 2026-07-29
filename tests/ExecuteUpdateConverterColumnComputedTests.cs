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
/// A server-side COMPUTED ExecuteUpdate SetProperty (e.g. <c>SetProperty(x => x.Col, x => x.Col + n)</c>)
/// translates to SQL that operates on the STORED (provider) representation. When Col carries a value
/// converter, the arithmetic runs in provider space and is never round-tripped through the converter — so
/// for any non-identity converter the persisted model value would be silently wrong. An arbitrary converter
/// cannot be inverted in SQL, so nORM rejects such computed forms fail-loud instead of corrupting the value.
/// The literal/captured SET form (which applies the converter to a bound parameter), a bare copy between
/// columns sharing the same converter instance, and plain converter-free arithmetic remain supported.
/// </summary>
[Trait("Category", "Fast")]
public class ExecuteUpdateConverterColumnComputedTests
{
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }     // negating converter: model 10 -> stored -10
        public int ScoreB { get; set; }    // negating converter (SAME instance as Score)
        public int Mirror { get; set; }    // plain int, NO converter
    }

    // Non-identity converter (v -> -v). Server-side arithmetic on the stored value diverges from model space.
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    // A single shared converter instance is wired to both Score and ScoreB so a bare Score->ScoreB copy is
    // provably an exact provider-to-provider copy (the allowed safe subset).
    private static readonly NegatingConverter SharedNeg = new();

    private static DbContext Create(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, " +
                              "Score INTEGER NOT NULL, ScoreB INTEGER NOT NULL DEFAULT 0, Mirror INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Row>().Property<int>(p => p.Score).HasConversion(SharedNeg);
                mb.Entity<Row>().Property<int>(p => p.ScoreB).HasConversion(SharedNeg);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static long RawScore(SqliteConnection cn, int id) => RawCol(cn, "Score", id);
    private static long RawScoreB(SqliteConnection cn, int id) => RawCol(cn, "ScoreB", id);
    private static long RawMirror(SqliteConnection cn, int id) => RawCol(cn, "Mirror", id);

    private static long RawCol(SqliteConnection cn, string col, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {col} FROM Row WHERE Id = {id}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static int ModelScore(DbContext ctx, int id)
        => ctx.Query<Row>().AsNoTracking().First(r => r.Id == id).Score;

    // ---- REJECTED: computed self-referencing arithmetic on a converter column ----

    [Fact]
    public async Task Computed_self_increment_on_converter_column_is_rejected_not_corrupted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10 };  // stored -10
        ctx.Add(row);
        await ctx.SaveChangesAsync();
        Assert.Equal(-10, RawScore(cn, row.Id));

        // `Score = (Score + 5)` on the STORED value would corrupt the model value (-10+5=-5 => model 5).
        // Reject fail-loud rather than persist the wrong value.
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Row>().Where(r => r.Id == row.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Score, x => x.Score + 5)));

        Assert.Equal(-10, RawScore(cn, row.Id)); // unchanged — nothing corrupted
        Assert.Equal(10, ModelScore(ctx, row.Id));
    }

    // Realistic scaling converter: money model-dollars <-> provider-cents (v -> v*100).
    [Table("Wallet")]
    private class Wallet
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public decimal Balance { get; set; }   // stored as integer cents
    }

    private sealed class DollarsToCentsConverter : ValueConverter<decimal, long>
    {
        public override object? ConvertToProvider(decimal v) => (long)(v * 100m);
        public override object? ConvertFromProvider(long v) => v / 100m;
    }

    private static DbContext CreateWallet(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Wallet (Id INTEGER PRIMARY KEY AUTOINCREMENT, Balance INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Wallet>().Property<decimal>(p => p.Balance).HasConversion(new DollarsToCentsConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Computed_add_on_scaling_converter_column_is_rejected_not_corrupted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateWallet(cn);
        var w = new Wallet { Balance = 10m };   // stored 1000 cents
        ctx.Add(w);
        await ctx.SaveChangesAsync();

        long RawBalance()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = $"SELECT Balance FROM Wallet WHERE Id = {w.Id}";
            return Convert.ToInt64(cmd.ExecuteScalar());
        }
        Assert.Equal(1000, RawBalance());

        // `Balance = (Balance + 5)` on stored CENTS would add 5 cents, not $5 — reject fail-loud.
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Wallet>().Where(r => r.Id == w.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Balance, x => x.Balance + 5m)));

        Assert.Equal(1000, RawBalance()); // unchanged
    }

    // ---- REJECTED: cross-column copy where SOURCE has a converter but TARGET does not ----

    [Fact]
    public async Task Cross_column_copy_from_converter_source_to_plain_target_is_rejected()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10 };  // stored -10
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // Copying the STORED -10 into a plain column would store -10 (model expected 10) — reject.
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Row>().Where(r => r.Id == row.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Mirror, x => x.Score)));

        Assert.Equal(0, RawMirror(cn, row.Id)); // unchanged (default)
    }

    // ---- ALLOWED: bare copy between columns sharing the SAME converter instance (exact provider copy) ----

    [Fact]
    public async Task Bare_copy_between_columns_with_the_same_converter_is_allowed()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10, ScoreB = 0 }; // Score stored -10, ScoreB stored 0
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        // ScoreB := Score — both use the same converter, so the provider-to-provider copy is exact.
        await ctx.Query<Row>().Where(r => r.Id == row.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.ScoreB, x => x.Score));

        Assert.Equal(-10, RawScoreB(cn, row.Id)); // stored copy is exact
        var model = ctx.Query<Row>().AsNoTracking().First(r => r.Id == row.Id);
        Assert.Equal(10, model.ScoreB);            // and the model value round-trips correctly
    }

    // ---- ALLOWED: literal SET on a converter column still applies the converter ----

    [Fact]
    public async Task Literal_set_on_converter_column_applies_the_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        await ctx.Query<Row>().Where(r => r.Id == row.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Score, 20)); // model 20 -> stored -20

        Assert.Equal(-20, RawScore(cn, row.Id));
        Assert.Equal(20, ModelScore(ctx, row.Id));
    }

    // ---- CONTROL: computed update on a PLAIN column is unaffected ----

    [Fact]
    public async Task Computed_self_increment_on_plain_column_is_correct_control()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 0, Mirror = 7 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        await ctx.Query<Row>().Where(r => r.Id == row.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Mirror, x => x.Mirror + 5));

        Assert.Equal(12, RawMirror(cn, row.Id)); // plain column: 7 + 5 = 12, correct
    }
}
