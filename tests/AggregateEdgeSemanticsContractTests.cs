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
/// Contract for aggregate edge semantics (Query/LINQ matrix cell: aggregates x operand edges).
///
/// Pins the empty-set, fractional-division, and overflow contracts against LINQ-to-Objects:
///
///  * Empty set, nullable overloads: <c>Sum</c> = 0, <c>Min</c>/<c>Max</c> = null, <c>Average</c> = null
///    - identical to <c>Enumerable</c>. (All-null and empty non-nullable cases are pinned by
///    <c>AggregateNullEmptySemanticsTests</c>.)
///  * <c>Average(int)</c> divides fractionally (average of {1,2} is 1.5), never integer division.
///  * <c>Sum(int)</c> whose TOTAL exceeds the int range FAILS LOUD with <see cref="OverflowException"/>
///    (the 64-bit SQL total cannot materialize into Int32) - same observable as .NET.
///  * <c>Sum(int)</c> with an INTERMEDIATE overflow but in-range total returns the correct total: SQL
///    accumulates in a wider type with no defined order, so unlike .NET's checked left-to-right
///    accumulation it does not throw. DESIGN-EXCEPTION, identical to EF Core.
///  * <c>Sum(long)</c> overflow FAILS LOUD with the provider's overflow error (SQLite: 'integer
///    overflow'). The exception type is provider-specific, not <see cref="OverflowException"/>, but
///    there is no silent wraparound. DESIGN-EXCEPTION, identical to EF Core.
///
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class AggregateEdgeSemanticsContractTests
{
    [Table("AggEdgeContract")]
    private sealed class A
    {
        [Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public int? NIntVal { get; set; }
        public long LongVal { get; set; }
    }

    private static DbContext Fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE AggEdgeContract (Id INTEGER PRIMARY KEY, IntVal INTEGER NOT NULL, NIntVal INTEGER, LongVal INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Empty_set_nullable_aggregates_match_dotnet()
    {
        using var ctx = Fresh();
        var q = (INormQueryable<A>)ctx.Query<A>();

        Assert.Equal(Array.Empty<int?>().Sum(), q.AsNoTracking().Sum(a => a.NIntVal));         // 0
        Assert.Equal(Array.Empty<int?>().Min(), q.AsNoTracking().Min(a => a.NIntVal));         // null
        Assert.Equal(Array.Empty<int?>().Max(), q.AsNoTracking().Max(a => a.NIntVal));         // null
        Assert.Equal(Array.Empty<int?>().Average(), q.AsNoTracking().Average(a => a.NIntVal)); // null
    }

    [Fact]
    public async Task Average_int_divides_fractionally()
    {
        using var ctx = Fresh();
        await ctx.InsertAsync(new A { Id = 1, IntVal = 1 });
        await ctx.InsertAsync(new A { Id = 2, IntVal = 2 });
        var q = (INormQueryable<A>)ctx.Query<A>();

        Assert.Equal(new[] { 1, 2 }.Average(), q.AsNoTracking().Average(a => a.IntVal));       // 1.5
    }

    [Fact]
    public async Task Sum_int_with_out_of_range_total_fails_loud()
    {
        using var ctx = Fresh();
        await ctx.InsertAsync(new A { Id = 1, IntVal = int.MaxValue });
        await ctx.InsertAsync(new A { Id = 2, IntVal = int.MaxValue });
        var q = (INormQueryable<A>)ctx.Query<A>();

        // The 64-bit SQL total (4294967294) cannot materialize into Int32 - fail loud, never wrap.
        Assert.Throws<OverflowException>(() => q.AsNoTracking().Sum(a => a.IntVal));
    }

    [Fact]
    public async Task Sum_int_with_intermediate_overflow_but_in_range_total_returns_total()
    {
        using var ctx = Fresh();
        await ctx.InsertAsync(new A { Id = 1, IntVal = int.MaxValue });
        await ctx.InsertAsync(new A { Id = 2, IntVal = 1 });
        await ctx.InsertAsync(new A { Id = 3, IntVal = -int.MaxValue });
        var q = (INormQueryable<A>)ctx.Query<A>();

        // SQL accumulates in a wider type with no defined order, so the in-range total succeeds.
        // .NET's checked left-to-right accumulation would throw here - the intended divergence
        // (identical to EF Core) is pinned by both assertions together.
        Assert.Equal(1, q.AsNoTracking().Sum(a => a.IntVal));
        Assert.Throws<OverflowException>(() => new[] { int.MaxValue, 1, -int.MaxValue }.Sum());
    }

    [Fact]
    public async Task Sum_long_overflow_fails_loud_with_provider_error()
    {
        using var ctx = Fresh();
        await ctx.InsertAsync(new A { Id = 1, LongVal = long.MaxValue });
        await ctx.InsertAsync(new A { Id = 2, LongVal = long.MaxValue });
        var q = (INormQueryable<A>)ctx.Query<A>();

        // SQLite raises 'integer overflow' - provider-typed but LOUD; no silent wraparound.
        var ex = Assert.ThrowsAny<Exception>(() => q.AsNoTracking().Sum(a => a.LongVal));
        Assert.Contains("overflow", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
