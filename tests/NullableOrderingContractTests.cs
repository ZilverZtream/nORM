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
/// Contract for nullable ordering / NULL placement (Query/LINQ matrix cell: OrderBy x nullable operand).
///
/// .NET <c>Comparer&lt;T?&gt;.Default</c> treats null as the SMALLEST value: nulls sort FIRST ascending
/// and LAST descending. nORM reproduces this on every provider. SQLite, SQL Server, and MySQL place
/// nulls first-ascending natively, so no rewrite is needed. PostgreSQL defaults to the OPPOSITE
/// (NULLS LAST for ascending), so nORM prepends a same-direction null-rank key <c>(key IS NOT NULL)</c>
/// - <c>false</c> (null) sorts before <c>true</c>, and because the rank and the key flip together the
/// null-first / null-last semantics survive <c>OrderByDescending</c> and <c>Reverse</c>. That
/// normalization is gated by the provider hook <c>RequiresExplicitNullOrderingForNullableKeys</c>,
/// which is true only for PostgreSQL. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NullableOrderingContractTests
{
    [Table("NullOrdContract")]
    private sealed class N { [Key] public int Id { get; set; } public int? Val { get; set; } }

    [Table("NullRefOrdContract")]
    private sealed class R { [Key] public int Id { get; set; } public string? Val { get; set; } }

    private static readonly (int Id, int? Val)[] IntRows =
    {
        (1, 5), (2, null), (3, -3), (4, null), (5, 0), (6, 10),
    };

    private static async Task<DbContext> SeedIntsAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE NullOrdContract (Id INTEGER PRIMARY KEY, Val INTEGER);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, val) in IntRows) await ctx.InsertAsync(new N { Id = id, Val = val });
        return ctx;
    }

    [Fact]
    public async Task OrderBy_nullable_int_places_nulls_first_ascending_matching_dotnet()
    {
        using var ctx = await SeedIntsAsync();

        var norm = ((INormQueryable<N>)ctx.Query<N>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        Assert.Equal(new[] { 2, 4, 3, 5, 1, 6 }, norm);                 // nulls (2,4) first, then -3,0,5,10
        Assert.Equal(IntRows.OrderBy(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
        Assert.Equal(new[] { 2, 4 }, norm.Take(2));                     // nulls at the front
    }

    [Fact]
    public async Task OrderByDescending_nullable_int_places_nulls_last_matching_dotnet()
    {
        using var ctx = await SeedIntsAsync();

        var norm = ((INormQueryable<N>)ctx.Query<N>())
            .AsNoTracking().OrderByDescending(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        Assert.Equal(new[] { 6, 1, 5, 3, 2, 4 }, norm);                 // 10,5,0,-3, then nulls (2,4) last
        Assert.Equal(IntRows.OrderByDescending(r => r.Val).ThenBy(r => r.Id).Select(r => r.Id).ToList(), norm);
        Assert.Equal(new[] { 2, 4 }, norm.Skip(4));                     // nulls at the back
    }

    [Fact]
    public async Task OrderBy_nullable_reference_type_places_nulls_first()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE NullRefOrdContract (Id INTEGER PRIMARY KEY, Val TEXT);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = new (int Id, string? Val)[] { (1, "b"), (2, null), (3, "a"), (4, null), (5, "c") };
        foreach (var (id, val) in rows) await ctx.InsertAsync(new R { Id = id, Val = val });

        var norm = ((INormQueryable<R>)ctx.Query<R>())
            .AsNoTracking().OrderBy(e => e.Val).ThenBy(e => e.Id).Select(e => e.Id).ToList();

        // nulls (2,4) first, then ordinal a,b,c -> 3,1,5. (Non-null string order is collation-governed;
        // lowercase a/b/c coincide with .NET here - the null PLACEMENT is what this cell pins.)
        Assert.Equal(new[] { 2, 4, 3, 1, 5 }, norm);
        Assert.Equal(new[] { 2, 4 }, norm.Take(2));
    }

    [Fact]
    public void Only_postgres_requires_explicit_null_ordering_normalization()
    {
        // The null-first/null-last normalization is applied exactly where the provider's native
        // default diverges from .NET. SQLite / SQL Server / MySQL already match; only PostgreSQL
        // (NULLS LAST ascending by default) needs the leading (key IS NOT NULL) rank entry.
        Assert.False(new SqliteProvider().RequiresExplicitNullOrderingForNullableKeys);
        Assert.False(new SqlServerProvider().RequiresExplicitNullOrderingForNullableKeys);
        Assert.False(new MySqlProvider(new SqliteParameterFactory()).RequiresExplicitNullOrderingForNullableKeys);
        Assert.True(new PostgresProvider(new SqliteParameterFactory()).RequiresExplicitNullOrderingForNullableKeys);
    }
}
