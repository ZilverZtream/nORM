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
/// Contract for enum write/read round-trip fidelity (write-path matrix cell: enum x underlying type x
/// defined/undefined values).
///
/// Enums round-trip by their UNDERLYING numeric value exactly like .NET semantics: defined members
/// (including negative and full-range byte/short members), UNDEFINED numeric values ((E)999 comes
/// back as (E)999 - never thrown on or clamped), and [Flags] combinations (defined and undefined)
/// all read back verbatim, and WHERE-equality matches undefined values too. A ulong-backed enum
/// follows the ulong storage contract: members in [0, long.MaxValue] round-trip exactly and a member
/// above long.MaxValue FAILS LOUD with <see cref="NormUsageException"/> at write time.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class EnumRoundTripContractTests
{
    private enum IntE { Neg = -5, Zero = 0, A = 1, B = 42 }
    private enum ByteE : byte { X = 0, Y = 255 }
    private enum ShortE : short { M = short.MinValue, P = short.MaxValue }
    private enum ULongE : ulong { Small = 1, InRange = 9_000_000_000_000_000_000, Big = 10_000_000_000_000_000_000 }
    [Flags] private enum FlagsE { None = 0, F1 = 1, F2 = 2, F4 = 4 }

    [Table("EnumRtContract")]
    private sealed class E
    {
        [Key] public int Id { get; set; }
        public IntE I { get; set; }
        public ByteE B { get; set; }
        public ShortE S { get; set; }
        public FlagsE F { get; set; }
    }

    [Table("EnumUlongContract")]
    private sealed class EU
    {
        [Key] public int Id { get; set; }
        public ULongE U { get; set; }
    }

    private static readonly (int Id, IntE I, ByteE B, ShortE S, FlagsE F)[] Rows =
    {
        (1, IntE.Neg, ByteE.X, ShortE.M, FlagsE.None),
        (2, IntE.B, ByteE.Y, ShortE.P, FlagsE.F1 | FlagsE.F4),
        (3, (IntE)999, (ByteE)7, (ShortE)(-1), (FlagsE)999),   // undefined numeric values
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE EnumRtContract (Id INTEGER PRIMARY KEY, I INTEGER NOT NULL, B INTEGER NOT NULL, S INTEGER NOT NULL, F INTEGER NOT NULL);" +
                            "CREATE TABLE EnumUlongContract (Id INTEGER PRIMARY KEY, U INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in Rows) await ctx.InsertAsync(new E { Id = r.Id, I = r.I, B = r.B, S = r.S, F = r.F });
        return ctx;
    }

    [Fact]
    public async Task Defined_undefined_and_flags_values_round_trip_verbatim()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<E>)ctx.Query<E>()).AsNoTracking().OrderBy(e => e.Id).ToList();

        for (int i = 0; i < Rows.Length; i++)
        {
            Assert.Equal(Rows[i].I, back[i].I);
            Assert.Equal(Rows[i].B, back[i].B);
            Assert.Equal(Rows[i].S, back[i].S);
            Assert.Equal(Rows[i].F, back[i].F);
        }
        Assert.Equal((IntE)999, back[2].I);            // undefined value intact, not clamped
        Assert.Equal(FlagsE.F1 | FlagsE.F4, back[1].F);
    }

    [Fact]
    public async Task Where_equality_matches_undefined_and_flag_combination_values()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<E>)ctx.Query<E>()).AsNoTracking();

        var undefined = (IntE)999;
        Assert.Equal(new[] { 3 }, q.Where(e => e.I == undefined).Select(e => e.Id).ToList());
        Assert.Equal(new[] { 2 }, q.Where(e => e.F == (FlagsE.F1 | FlagsE.F4)).Select(e => e.Id).ToList());
    }

    [Fact]
    public async Task Ulong_backed_enum_follows_the_ulong_storage_contract()
    {
        using var ctx = await SeedAsync();

        // In [0, long.MaxValue]: exact round-trip.
        await ctx.InsertAsync(new EU { Id = 1, U = ULongE.InRange });
        var back = ((INormQueryable<EU>)ctx.Query<EU>()).AsNoTracking()
            .Where(x => x.Id == 1).Select(x => x.U).Single();
        Assert.Equal(ULongE.InRange, back);

        // Above long.MaxValue: fail loud, nothing persisted for that row.
        await Assert.ThrowsAsync<NormUsageException>(() => ctx.InsertAsync(new EU { Id = 2, U = ULongE.Big }));
        Assert.Equal(1, ((INormQueryable<EU>)ctx.Query<EU>()).AsNoTracking().Count());
    }
}
