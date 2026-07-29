using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Hunt: does the IMMEDIATE raw-SQL materialization path (FromSqlRawAsync / QueryUnchangedAsync /
/// ExecuteStoredProcedureAsync — all via MaterializeRawEntity) apply a mapped column's VALUE CONVERTER
/// (ConvertFromProvider) the way the composable FromSqlRaw path and ctx.Query do? The composable path is
/// pinned to convert (FromSqlRawConverterColumnTests); this checks the immediate path for a silent divergence.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class FromSqlRawImmediateConverterHuntTests
{
    private readonly ITestOutputHelper _out;
    public FromSqlRawImmediateConverterHuntTests(ITestOutputHelper o) => _out = o;

    // Stored = -model. A non-identity converter makes an unconverted (stored) result visible.
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -(Convert.ToInt32(v));
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("HicWidget")]
    public sealed class Widget
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Score { get; set; } // stored negated via converter
        public string Name { get; set; } = "";
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE HicWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property(w => w.Score).HasConversion(new NegatingConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Seed via nORM so ConvertToProvider stores the negated value (-3,-7,-5).
        await ctx.InsertAsync(new Widget { Id = 1, Score = 3, Name = "a" });
        await ctx.InsertAsync(new Widget { Id = 2, Score = 7, Name = "b" });
        await ctx.InsertAsync(new Widget { Id = 3, Score = 5, Name = "c" });
        return (cn, ctx);
    }

    [Fact]
    public async Task immediate_FromSqlRawAsync_applies_value_converter()
    {
        var (cn, ctx) = await CtxAsync();
        using var _cn = cn; using var _ctx = ctx;

        // Reference: what the stored (provider) values actually are, read raw.
        using (var raw = cn.CreateCommand()) { raw.CommandText = "SELECT Id, Score FROM HicWidget ORDER BY Id"; using var rd = raw.ExecuteReader(); while (rd.Read()) _out.WriteLine($"stored Id={rd.GetInt32(0)} Score={rd.GetInt32(1)}"); }

        var rows = await ctx.FromSqlRawAsync<Widget>("SELECT * FROM HicWidget");
        var scores = rows.OrderBy(w => w.Id).Select(w => w.Score).ToArray();
        _out.WriteLine("FromSqlRawAsync scores = " + string.Join(",", scores));

        // The composable path and ctx.Query both yield the MODEL values 3,7,5. The immediate path must too.
        Assert.Equal(new[] { 3, 7, 5 }, scores);
    }

    [Fact]
    public async Task immediate_QueryUnchangedAsync_applies_value_converter()
    {
        var (cn, ctx) = await CtxAsync();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.QueryUnchangedAsync<Widget>("SELECT * FROM HicWidget");
        var scores = rows.OrderBy(w => w.Id).Select(w => w.Score).ToArray();
        _out.WriteLine("QueryUnchangedAsync scores = " + string.Join(",", scores));
        Assert.Equal(new[] { 3, 7, 5 }, scores);
    }

    [Fact]
    public async Task immediate_ExecuteStoredProcedureAsync_applies_value_converter()
    {
        var (cn, ctx) = await CtxAsync();
        using var _cn = cn; using var _ctx = ctx;

        // On SQLite a "stored procedure" is a SELECT with CommandType.Text.
        var rows = await ctx.ExecuteStoredProcedureAsync<Widget>("SELECT * FROM HicWidget");
        var scores = rows.OrderBy(w => w.Id).Select(w => w.Score).ToArray();
        _out.WriteLine("ExecuteStoredProcedureAsync scores = " + string.Join(",", scores));
        Assert.Equal(new[] { 3, 7, 5 }, scores);
    }

    // Oracle: the composable path (known-correct, pinned by FromSqlRawConverterColumnTests) yields 3,7,5.
    [Fact]
    public async Task composable_FromSqlRaw_oracle_yields_model_values()
    {
        var (cn, ctx) = await CtxAsync();
        using var _cn = cn; using var _ctx = ctx;

        var scores = ctx.FromSqlRaw<Widget>("SELECT * FROM HicWidget").OrderBy(w => w.Id).Select(w => w.Score).ToList();
        _out.WriteLine("composable scores = " + string.Join(",", scores));
        Assert.Equal(new[] { 3, 7, 5 }, scores);

        var q = ctx.Query<Widget>().OrderBy(w => w.Id).Select(w => w.Score).ToList();
        _out.WriteLine("ctx.Query scores = " + string.Join(",", q));
        Assert.Equal(new[] { 3, 7, 5 }, q);
    }
}
