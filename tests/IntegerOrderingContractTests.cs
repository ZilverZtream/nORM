using System;
using System.Collections.Generic;
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
/// Contract for integer ordering (Query/LINQ matrix cell: OrderBy x integer operand).
///
/// All integer types whose full range fits in SQLite's signed 64-bit INTEGER order CONSISTENTLY with
/// .NET's default comparer: signed <c>int</c>/<c>long</c>/<c>short</c>/<c>sbyte</c> (including negative
/// and Min/Max boundaries) and unsigned <c>byte</c>/<c>uint</c> (whose maxima are below
/// <c>long.MaxValue</c>). This covers every integer CLR type EXCEPT <c>ulong</c>, whose upper half
/// (&gt; <c>long.MaxValue</c>) has no signed-64-bit representation; that boundary is tracked separately
/// as a known defect (see NH-0102 / NH-0202) and is deliberately NOT asserted here.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class IntegerOrderingContractTests
{
    [Table("IntOrd")] private sealed class EInt { [Key] public int Id { get; set; } public int Val { get; set; } }
    [Table("LongOrd")] private sealed class ELong { [Key] public int Id { get; set; } public long Val { get; set; } }
    [Table("ShortOrd")] private sealed class EShort { [Key] public int Id { get; set; } public short Val { get; set; } }
    [Table("ByteOrd")] private sealed class EByte { [Key] public int Id { get; set; } public byte Val { get; set; } }
    [Table("SByteOrd")] private sealed class ESByte { [Key] public int Id { get; set; } public sbyte Val { get; set; } }
    [Table("UIntOrd")] private sealed class EUInt { [Key] public int Id { get; set; } public uint Val { get; set; } }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = ddl;
        c.ExecuteNonQuery();
        return cn;
    }

    private static async Task AssertOrdersLikeDotnet<TEntity, TVal>(
        string ddl, IReadOnlyList<TVal> vals,
        Func<int, TVal, TEntity> make, Func<TEntity, int> id, Func<TEntity, TVal> val)
        where TEntity : class
        where TVal : IComparable<TVal>
    {
        using var ctx = new DbContext(Open(ddl), new SqliteProvider());
        var rows = vals.Select((v, i) => (Id: i + 1, Val: v)).ToList();
        foreach (var r in rows) await ctx.InsertAsync(make(r.Id, r.Val));

        var norm = ((INormQueryable<TEntity>)ctx.Query<TEntity>())
            .AsNoTracking().OrderBy(val).ThenBy(id).Select(id).ToList();
        var oracle = rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList();

        Assert.Equal(oracle, norm);
    }

    [Fact]
    public Task OrderBy_int_matches_dotnet() => AssertOrdersLikeDotnet<EInt, int>(
        "CREATE TABLE IntOrd (Id INTEGER PRIMARY KEY, Val INTEGER);",
        new[] { int.MinValue, -1, 0, 1, int.MaxValue },
        (i, v) => new EInt { Id = i, Val = v }, e => e.Id, e => e.Val);

    [Fact]
    public Task OrderBy_long_matches_dotnet() => AssertOrdersLikeDotnet<ELong, long>(
        "CREATE TABLE LongOrd (Id INTEGER PRIMARY KEY, Val INTEGER);",
        new[] { long.MinValue, -1L, 0L, 1L, long.MaxValue },
        (i, v) => new ELong { Id = i, Val = v }, e => e.Id, e => e.Val);

    [Fact]
    public Task OrderBy_short_matches_dotnet() => AssertOrdersLikeDotnet<EShort, short>(
        "CREATE TABLE ShortOrd (Id INTEGER PRIMARY KEY, Val INTEGER);",
        new short[] { short.MinValue, -1, 0, 1, short.MaxValue },
        (i, v) => new EShort { Id = i, Val = v }, e => e.Id, e => e.Val);

    [Fact]
    public Task OrderBy_byte_matches_dotnet() => AssertOrdersLikeDotnet<EByte, byte>(
        "CREATE TABLE ByteOrd (Id INTEGER PRIMARY KEY, Val INTEGER);",
        new byte[] { 0, 1, 127, 128, 255 },
        (i, v) => new EByte { Id = i, Val = v }, e => e.Id, e => e.Val);

    [Fact]
    public Task OrderBy_sbyte_matches_dotnet() => AssertOrdersLikeDotnet<ESByte, sbyte>(
        "CREATE TABLE SByteOrd (Id INTEGER PRIMARY KEY, Val INTEGER);",
        new sbyte[] { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue },
        (i, v) => new ESByte { Id = i, Val = v }, e => e.Id, e => e.Val);

    [Fact]
    public Task OrderBy_uint_matches_dotnet_including_above_int_max() => AssertOrdersLikeDotnet<EUInt, uint>(
        "CREATE TABLE UIntOrd (Id INTEGER PRIMARY KEY, Val INTEGER);",
        new uint[] { 0, 1, int.MaxValue, (uint)int.MaxValue + 1, uint.MaxValue },
        (i, v) => new EUInt { Id = i, Val = v }, e => e.Id, e => e.Val);
}
