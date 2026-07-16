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
/// Contract for string predicate semantics (Query/LINQ matrix cell: string predicates x
/// case / empty pattern / null).
///
/// String predicates match .NET ordinal semantics:
///
///  * <c>Contains</c> / <c>StartsWith</c> / <c>EndsWith</c> are case-SENSITIVE (ordinal), and
///    <c>==</c> / <c>!=</c> compare ordinally; <c>!=</c> includes NULL rows like C#.
///  * An EMPTY pattern matches every NON-NULL row (like .NET) - and never a NULL row: the SQLite
///    ordinal-bypass emits an explicit non-null guard on its empty-pattern arm, since a bare
///    length-check would silently match NULL rows that .NET and LIKE-based providers exclude.
///  * NEGATED string-method predicates KEEP NULL rows: a null string matches nothing, so under the
///    two-valued C# semantics the negation of "no match" is true - the same null-rescue the negated
///    comparison and list-Contains translations apply (`NOT(match) OR col IS NULL`).
///  * <c>string.IsNullOrEmpty</c> matches null and empty; <c>== null</c> matches only null.
///
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class StringPredicateSemanticsContractTests
{
    [Table("StrPredContract")]
    private sealed class S
    {
        [Key] public int Id { get; set; }
        public string? Val { get; set; }
    }

    private static readonly (int Id, string? V)[] Rows =
    {
        (1, "banana"),
        (2, "BANANA"),
        (3, "Band"),
        (4, null),
        (5, ""),
        (6, "abc"),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE StrPredContract (Id INTEGER PRIMARY KEY, Val TEXT);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, v) in Rows) await ctx.InsertAsync(new S { Id = id, Val = v });
        return ctx;
    }

    private static int[] Ids(System.Collections.Generic.IEnumerable<int> xs) => xs.OrderBy(x => x).ToArray();

    [Fact]
    public async Task Match_predicates_are_ordinal_case_sensitive()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<S>)ctx.Query<S>()).AsNoTracking();

        Assert.Equal(new[] { 1, 3 }, Ids(q.Where(s => s.Val!.Contains("an")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 3 }, Ids(q.Where(s => s.Val!.StartsWith("Ba")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 2 }, Ids(q.Where(s => s.Val!.EndsWith("NA")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 1 }, Ids(q.Where(s => s.Val == "banana").Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 2, 3, 4, 5, 6 }, Ids(q.Where(s => s.Val != "banana").Select(s => s.Id).ToList()));
    }

    [Fact]
    public async Task Empty_pattern_matches_every_non_null_row_and_never_null()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<S>)ctx.Query<S>()).AsNoTracking();

        // .NET: any non-null string Contains/StartsWith/EndsWith "". The NULL row must NOT match.
        Assert.Equal(new[] { 1, 2, 3, 5, 6 }, Ids(q.Where(s => s.Val!.Contains("")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 1, 2, 3, 5, 6 }, Ids(q.Where(s => s.Val!.StartsWith("")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 1, 2, 3, 5, 6 }, Ids(q.Where(s => s.Val!.EndsWith("")).Select(s => s.Id).ToList()));
    }

    [Fact]
    public async Task Negated_match_predicates_keep_null_rows_like_csharp()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<S>)ctx.Query<S>()).AsNoTracking();

        // A null string matches nothing, so its negation is true - Id 4 must be present.
        Assert.Equal(new[] { 2, 4, 5, 6 }, Ids(q.Where(s => !s.Val!.Contains("an")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 1, 2, 4, 5, 6 }, Ids(q.Where(s => !s.Val!.StartsWith("Ba")).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 1, 3, 4, 5, 6 }, Ids(q.Where(s => !s.Val!.EndsWith("NA")).Select(s => s.Id).ToList()));
        Assert.Equal(
            Ids(Rows.Where(r => !(r.V == "banana")).Select(r => r.Id)),
            Ids(q.Where(s => !(s.Val == "banana")).Select(s => s.Id).ToList()));
    }

    [Fact]
    public async Task Null_literal_and_IsNullOrEmpty_match_dotnet()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<S>)ctx.Query<S>()).AsNoTracking();

        Assert.Equal(new[] { 4 }, Ids(q.Where(s => s.Val == null).Select(s => s.Id).ToList()));
        Assert.Equal(new[] { 4, 5 }, Ids(q.Where(s => string.IsNullOrEmpty(s.Val)).Select(s => s.Id).ToList()));
    }
}
