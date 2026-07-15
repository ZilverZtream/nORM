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
/// Contract for Guid ordering (Query/LINQ matrix cell: OrderBy x Guid operand).
///
/// A translated <c>OrderBy</c> on a Guid column is CONSISTENT with .NET
/// <c>Guid.CompareTo</c> on SQLite - and provably so, not by luck. nORM stores a Guid as its
/// canonical lowercase 8-4-4-4-12 hex string; SQLite's default BINARY collation orders that text
/// ordinally. Because each hex group is fixed-width big-endian and .NET's <c>Guid.CompareTo</c>
/// compares the leading integer fields (a/b/c) as UNSIGNED, ordinal text order equals field-wise
/// unsigned order for every Guid - so the database ordering matches .NET exactly.
///
/// This consistency is fragile: it depends on the TEXT (canonical-hex) storage. If a Guid were ever
/// stored as a little-endian 16-byte BLOB, byte-ordinal ordering would diverge from .NET (e.g.
/// <c>ffff0000-...</c> would sort FIRST instead of LAST). This test pins the current, correct
/// behaviour with adversarial high-bit values so that a storage-format regression is caught.
///
/// Scope: SQLite. SQL Server orders <c>uniqueidentifier</c> by its own field order (a provider-native
/// divergence from .NET, handled under the same provider-ordering design exception as string
/// collation); that cross-provider case belongs to the provider-mobility matrix. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GuidOrderingContractTests
{
    [Table("GuidOrdContract")]
    private sealed class E { [Key] public int Id { get; set; } public Guid Val { get; set; } }

    // High-bit / field-boundary values: a naive signed-int or little-endian-BLOB ordering would
    // reorder these differently from .NET. Id4 (ffff0000) has the high bit set in the first field.
    private static readonly (int Id, Guid Val)[] Rows =
    {
        (1, Guid.Parse("00000080-0000-0000-0000-000000000000")),
        (2, Guid.Parse("00000100-0000-0000-0000-000000000000")),
        (3, Guid.Parse("00000001-0000-0000-0000-000000000000")),
        (4, Guid.Parse("ffff0000-0000-0000-0000-000000000000")),
        (5, Guid.Parse("0000ff00-1122-3344-5566-778899aabbcc")),
        (6, Guid.Parse("7f000000-0000-0000-0000-0000000000ff")),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE GuidOrdContract (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, val) in Rows)
            await ctx.InsertAsync(new E { Id = id, Val = val });
        return ctx;
    }

    [Fact]
    public async Task OrderBy_guid_matches_dotnet_Guid_CompareTo_on_sqlite()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().OrderBy(e => e.Val).Select(e => e.Id).ToList();

        // Exact expected order - pins the concrete result. A storage-format change (e.g. to a
        // little-endian BLOB) or a client-side sort would move Id4 (ffff0000, high bit set) and break this.
        Assert.Equal(new[] { 3, 1, 2, 5, 6, 4 }, norm);

        // And it equals the .NET Guid.CompareTo oracle - the consistency this cell asserts.
        var oracle = Rows.OrderBy(r => r.Val).Select(r => r.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task OrderByDescending_guid_matches_dotnet_on_sqlite()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().OrderByDescending(e => e.Val).Select(e => e.Id).ToList();

        var oracle = Rows.OrderByDescending(r => r.Val).Select(r => r.Id).ToList();
        Assert.Equal(oracle, norm);
    }
}
