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
/// Contract for small-integer write/read round-trip fidelity (write-path matrix cell:
/// uint / ushort / byte / sbyte / short x boundary values).
///
/// Every value in each type's range round-trips exactly through nORM's write path and materializer -
/// all five ranges fit SQLite's signed 64-bit INTEGER, including uint values above int.MaxValue and
/// the negative minima - and a WHERE-equality predicate bound at a boundary value matches exactly the
/// row that was written with it (the binder and the storage agree on representation).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SmallIntegerRoundTripContractTests
{
    [Table("SmallIntRoundTrip")]
    private sealed class E
    {
        [Key] public int Id { get; set; }
        public uint U32 { get; set; }
        public ushort U16 { get; set; }
        public byte B8 { get; set; }
        public sbyte S8 { get; set; }
        public short S16 { get; set; }
    }

    private static readonly (int Id, uint U32, ushort U16, byte B8, sbyte S8, short S16)[] Rows =
    {
        (1, uint.MinValue,          ushort.MinValue, byte.MinValue, sbyte.MinValue, short.MinValue),
        (2, uint.MaxValue,          ushort.MaxValue, byte.MaxValue, sbyte.MaxValue, short.MaxValue),
        (3, (uint)int.MaxValue + 1, 32768,           128,           -1,             -1),
        (4, 42,                     42,              42,            42,             42),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE SmallIntRoundTrip (Id INTEGER PRIMARY KEY, U32 INTEGER NOT NULL, U16 INTEGER NOT NULL, B8 INTEGER NOT NULL, S8 INTEGER NOT NULL, S16 INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in Rows)
            await ctx.InsertAsync(new E { Id = r.Id, U32 = r.U32, U16 = r.U16, B8 = r.B8, S8 = r.S8, S16 = r.S16 });
        return ctx;
    }

    [Fact]
    public async Task Boundary_values_round_trip_exactly_for_all_five_types()
    {
        using var ctx = await SeedAsync();
        var back = ((INormQueryable<E>)ctx.Query<E>()).AsNoTracking().OrderBy(e => e.Id).ToList();

        Assert.Equal(Rows.Length, back.Count);
        for (int i = 0; i < Rows.Length; i++)
        {
            Assert.Equal(Rows[i].U32, back[i].U32);
            Assert.Equal(Rows[i].U16, back[i].U16);
            Assert.Equal(Rows[i].B8, back[i].B8);
            Assert.Equal(Rows[i].S8, back[i].S8);
            Assert.Equal(Rows[i].S16, back[i].S16);
        }
    }

    [Fact]
    public async Task Where_equality_at_boundaries_matches_exactly_the_written_row()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<E>)ctx.Query<E>()).AsNoTracking();

        Assert.Equal(new[] { 2 }, q.Where(e => e.U32 == uint.MaxValue).Select(e => e.Id).ToList());
        Assert.Equal(new[] { 3 }, q.Where(e => e.U32 == (uint)int.MaxValue + 1).Select(e => e.Id).ToList());
        Assert.Equal(new[] { 2 }, q.Where(e => e.U16 == ushort.MaxValue).Select(e => e.Id).ToList());
        Assert.Equal(new[] { 2 }, q.Where(e => e.B8 == byte.MaxValue).Select(e => e.Id).ToList());
        Assert.Equal(new[] { 1 }, q.Where(e => e.S8 == sbyte.MinValue).Select(e => e.Id).ToList());
        Assert.Equal(new[] { 1 }, q.Where(e => e.S16 == short.MinValue).Select(e => e.Id).ToList());
    }
}
