#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A nullable column must round-trip null AS null — never coerced to 0, "", default(Guid), or false, and
/// never a non-null value flipped to null. Coercing null to a default (or vice versa) is silent corruption
/// that changes query results and downstream logic. Writes null/non-null across every nullable CLR type and
/// asserts the exact value comes back.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NullableValueFidelityProbeTests
{
    [Table("NvRow")]
    public sealed class NvRow
    {
        [Key] public int Id { get; set; }
        public int? Ni { get; set; }
        public long? Nl { get; set; }
        public string? Ns { get; set; }
        public DateTime? Nd { get; set; }
        public decimal? Ndec { get; set; }
        public double? Ndbl { get; set; }
        public bool? Nb { get; set; }
        public Guid? Ng { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NvRow (Id INTEGER PRIMARY KEY, Ni INTEGER NULL, Nl INTEGER NULL, Ns TEXT NULL, Nd TEXT NULL, Ndec TEXT NULL, Ndbl REAL NULL, Nb INTEGER NULL, Ng TEXT NULL)";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<NvRow>().HasKey(x => x.Id) }, ownsConnection: true);
    }

    private static readonly NvRow[] Cases =
    {
        new() { Id = 1 }, // all null
        new() { Id = 2, Ni = 0, Nl = 0L, Ns = "", Nd = new DateTime(2001, 2, 3, 4, 5, 6), Ndec = 0m, Ndbl = 0d, Nb = false, Ng = Guid.Empty }, // all default-valued but NON-null
        new() { Id = 3, Ni = -7, Nl = 9007199254740993L, Ns = "x🎉", Nd = new DateTime(2023, 6, 15).AddTicks(1234560), Ndec = -1.25m, Ndbl = Math.PI, Nb = true, Ng = Guid.Parse("11112222-3333-4444-5555-666677778888") },
    };

    [Fact]
    public async Task Nulls_and_zero_valued_non_nulls_round_trip_distinctly()
    {
        using var ctx = NewCtx();
        foreach (var c in Cases)
            ctx.Add(new NvRow { Id = c.Id, Ni = c.Ni, Nl = c.Nl, Ns = c.Ns, Nd = c.Nd, Ndec = c.Ndec, Ndbl = c.Ndbl, Nb = c.Nb, Ng = c.Ng });
        await ctx.SaveChangesAsync();

        var read = ctx.Query<NvRow>().ToList().ToDictionary(r => r.Id);
        foreach (var c in Cases)
        {
            var r = read[c.Id];
            Assert.Equal(c.Ni, r.Ni);
            Assert.Equal(c.Nl, r.Nl);
            Assert.Equal(c.Ns, r.Ns);
            Assert.Equal(c.Nd, r.Nd);
            Assert.Equal(c.Ndec, r.Ndec);
            Assert.Equal(c.Ndbl, r.Ndbl);
            Assert.Equal(c.Nb, r.Nb);
            Assert.Equal(c.Ng, r.Ng);
        }
    }

    [Fact]
    public async Task Null_is_distinguishable_from_zero_in_a_predicate()
    {
        using var ctx = NewCtx();
        foreach (var c in Cases)
            ctx.Add(new NvRow { Id = c.Id, Ni = c.Ni, Nl = c.Nl, Ns = c.Ns, Nd = c.Nd, Ndec = c.Ndec, Ndbl = c.Ndbl, Nb = c.Nb, Ng = c.Ng });
        await ctx.SaveChangesAsync();

        // Row 1 has NULL Ni; row 2 has Ni = 0. They must not be conflated.
        Assert.Equal(new[] { 1 }, ctx.Query<NvRow>().Where(r => r.Ni == null).OrderBy(r => r.Id).Select(r => r.Id).ToList());
        Assert.Equal(new[] { 2 }, ctx.Query<NvRow>().Where(r => r.Ni == 0).OrderBy(r => r.Id).Select(r => r.Id).ToList());
        Assert.Equal(new[] { 1 }, ctx.Query<NvRow>().Where(r => r.Ns == null).OrderBy(r => r.Id).Select(r => r.Id).ToList());
        Assert.Equal(new[] { 2 }, ctx.Query<NvRow>().Where(r => r.Ns == "").OrderBy(r => r.Id).Select(r => r.Id).ToList());
    }
}
