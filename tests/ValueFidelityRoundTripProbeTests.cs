#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Round-trip fidelity for extreme and awkward values: a write followed by a read must return exactly what
/// went in. A mangled unicode string, a truncated decimal, a DateTime that lost its sub-second ticks, or a
/// clamped integer boundary is silent corruption — the core zero-data-loss concern. Writes and reads through
/// nORM, then asserts each field byte-for-byte.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ValueFidelityRoundTripProbeTests
{
    [Table("VfRow")]
    public sealed class VfRow
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
        public int I { get; set; }
        public long L { get; set; }
        public decimal Dec { get; set; }
        public double Dbl { get; set; }
        public DateTime Dt { get; set; }
        public bool Flag { get; set; }
        public Guid G { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE VfRow (Id INTEGER PRIMARY KEY, Text TEXT NOT NULL, I INTEGER NOT NULL, L INTEGER NOT NULL, Dec TEXT NOT NULL, Dbl REAL NOT NULL, Dt TEXT NOT NULL, Flag INTEGER NOT NULL, G TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<VfRow>().HasKey(x => x.Id) }, ownsConnection: true);
    }

    private static readonly VfRow[] Cases =
    {
        new() { Id = 1, Text = "", I = 0, L = 0L, Dec = 0m, Dbl = 0d, Dt = new DateTime(2000, 1, 1), Flag = false, G = Guid.Empty },
        new() { Id = 2, Text = "emoji 🎉 CJK 日本語 comb, é", I = int.MaxValue, L = long.MaxValue, Dec = 79228162514264337593543950335m, Dbl = double.MaxValue, Dt = DateTime.MaxValue, Flag = true, G = Guid.Parse("11112222-3333-4444-5555-666677778888") },
        new() { Id = 3, Text = "quote's \"here\" back\\slash\ttab\nline", I = int.MinValue, L = long.MinValue, Dec = -79228162514264337593543950335m, Dbl = double.MinValue, Dt = DateTime.MinValue, Flag = false, G = Guid.NewGuid() },
        new() { Id = 4, Text = "   leading/trailing   ", I = -1, L = -1L, Dec = 0.0000000001m, Dbl = Math.PI, Dt = new DateTime(2023, 6, 15, 12, 30, 45, 123).AddTicks(4567), Flag = true, G = Guid.NewGuid() },
        new() { Id = 5, Text = "precision 123456789012345.6789", I = 42, L = 9007199254740993L, Dec = 12345678901234.56789m, Dbl = 0.1 + 0.2, Dt = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), Flag = true, G = Guid.NewGuid() },
    };

    [Fact]
    public async System.Threading.Tasks.Task All_values_round_trip_exactly()
    {
        using var ctx = NewCtx();
        foreach (var c in Cases)
            ctx.Add(new VfRow { Id = c.Id, Text = c.Text, I = c.I, L = c.L, Dec = c.Dec, Dbl = c.Dbl, Dt = c.Dt, Flag = c.Flag, G = c.G });
        await ctx.SaveChangesAsync();

        // Fresh read — no identity-map shortcut hiding a storage defect.
        var read = ctx.Query<VfRow>().ToList().ToDictionary(r => r.Id);

        foreach (var c in Cases)
        {
            var r = read[c.Id];
            Assert.Equal(c.Text, r.Text);
            Assert.Equal(c.I, r.I);
            Assert.Equal(c.L, r.L);
            Assert.Equal(c.Dec, r.Dec);
            Assert.Equal(c.Dbl, r.Dbl);
            Assert.Equal(c.Dt, r.Dt);
            Assert.Equal(c.Flag, r.Flag);
            Assert.Equal(c.G, r.G);
        }
    }

    [Fact]
    public async System.Threading.Tasks.Task Extreme_values_survive_a_where_filter_round_trip()
    {
        using var ctx = NewCtx();
        foreach (var c in Cases)
            ctx.Add(new VfRow { Id = c.Id, Text = c.Text, I = c.I, L = c.L, Dec = c.Dec, Dbl = c.Dbl, Dt = c.Dt, Flag = c.Flag, G = c.G });
        await ctx.SaveChangesAsync();

        // A predicate binding the extreme values must match the row that holds them.
        Assert.Single(ctx.Query<VfRow>().Where(r => r.I == int.MaxValue && r.L == long.MaxValue).ToList());
        Assert.Single(ctx.Query<VfRow>().Where(r => r.I == int.MinValue).ToList());
        Assert.Single(ctx.Query<VfRow>().Where(r => r.Dec == 79228162514264337593543950335m).ToList());
        Assert.Single(ctx.Query<VfRow>().Where(r => r.Text == "emoji 🎉 CJK 日本語 comb, é").ToList());
        Assert.Single(ctx.Query<VfRow>().Where(r => r.G == Guid.Parse("11112222-3333-4444-5555-666677778888")).ToList());
    }
}
