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
/// Contract for ulong storage and ordering (Query/LINQ matrix cell: OrderBy x ulong operand, plus the
/// write-path range contract that makes it correct).
///
/// No provider has a portable native unsigned 64-bit type, so nORM stores <c>ulong</c> as signed 64-bit.
/// Values in [0, long.MaxValue] behave EXACTLY like a <c>long</c> - correct ordering, comparison,
/// arithmetic, and round-trip. A value above <c>long.MaxValue</c> has no portable representation (it
/// would wrap to a negative bit pattern, silently reversing ordering and crashing read-back), so nORM
/// FAILS LOUD with <see cref="NormUsageException"/> at write time on every path (direct, compiled,
/// bulk) rather than corrupt the column. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class UlongStorageContractTests
{
    [Table("ULongContract")]
    private sealed class E { [Key] public int Id { get; set; } public ulong Val { get; set; } }

    [Table("ULongNullableContract")]
    private sealed class EN { [Key] public int Id { get; set; } public ulong? Val { get; set; } }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = ddl;
        c.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task OrderBy_ulong_in_range_matches_dotnet_and_roundtrips()
    {
        var rows = new (int Id, ulong Val)[]
        {
            (1, 0UL),
            (2, 1UL),
            (3, 42UL),
            (4, (ulong)int.MaxValue + 5),      // above uint's signed friend, still tiny vs long
            (5, 5_000_000_000UL),              // above uint.MaxValue
            (6, (ulong)long.MaxValue),         // the maximum representable value
        };
        using var ctx = new DbContext(Open("CREATE TABLE ULongContract (Id INTEGER PRIMARY KEY, Val INTEGER);"),
            new SqliteProvider());
        foreach (var (id, val) in rows) await ctx.InsertAsync(new E { Id = id, Val = val });

        // Ordering matches .NET's default ulong comparer, including the full-width long.MaxValue.
        var norm = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(rows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);

        // Values round-trip exactly (no OverflowException, no sign wrap).
        var back = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().OrderBy(e => e.Id).Select(e => e.Val).ToList();
        Assert.Equal(rows.Select(r => r.Val).ToList(), back);
    }

    [Fact]
    public async Task Ulong_equality_filter_roundtrips_through_signed_storage()
    {
        using var ctx = new DbContext(Open("CREATE TABLE ULongContract (Id INTEGER PRIMARY KEY, Val INTEGER);"),
            new SqliteProvider());
        await ctx.InsertAsync(new E { Id = 1, Val = 5_000_000_000UL });
        await ctx.InsertAsync(new E { Id = 2, Val = (ulong)long.MaxValue });

        // The predicate literal is bound through the same signed-storage encoding, so equality matches.
        var hit = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().Where(e => e.Val == 5_000_000_000UL).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, hit);

        var maxHit = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().Where(e => e.Val == (ulong)long.MaxValue).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 2 }, maxHit);
    }

    [Fact]
    public async Task Storing_ulong_above_long_max_fails_loud_and_persists_nothing()
    {
        using var ctx = new DbContext(Open("CREATE TABLE ULongContract (Id INTEGER PRIMARY KEY, Val INTEGER);"),
            new SqliteProvider());

        var justOver = (ulong)long.MaxValue + 1;   // 9223372036854775808
        var ex = await Assert.ThrowsAsync<NormUsageException>(
            () => ctx.InsertAsync(new E { Id = 1, Val = justOver }));
        Assert.Contains("long.MaxValue", ex.Message);

        await Assert.ThrowsAsync<NormUsageException>(
            () => ctx.InsertAsync(new E { Id = 2, Val = ulong.MaxValue }));

        // Fail-loud, not silent: nothing was written.
        var count = ((INormQueryable<E>)ctx.Query<E>()).AsNoTracking().Count();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Nullable_ulong_roundtrips_including_null()
    {
        using var ctx = new DbContext(Open("CREATE TABLE ULongNullableContract (Id INTEGER PRIMARY KEY, Val INTEGER);"),
            new SqliteProvider());
        await ctx.InsertAsync(new EN { Id = 1, Val = 5_000_000_000UL });
        await ctx.InsertAsync(new EN { Id = 2, Val = null });
        await ctx.InsertAsync(new EN { Id = 3, Val = (ulong)long.MaxValue });

        var back = ((INormQueryable<EN>)ctx.Query<EN>())
            .AsNoTracking().OrderBy(e => e.Id).Select(e => e.Val).ToList();
        Assert.Equal(new ulong?[] { 5_000_000_000UL, null, (ulong)long.MaxValue }, back);

        // The range guard also applies through the nullable path.
        await Assert.ThrowsAsync<NormUsageException>(
            () => ctx.InsertAsync(new EN { Id = 4, Val = ulong.MaxValue }));
    }
}
