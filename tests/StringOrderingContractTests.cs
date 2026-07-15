using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for string ordering (Query/LINQ matrix cell: OrderBy x string operand).
///
/// A translated <c>OrderBy</c> on a string column follows the DATABASE column collation, not .NET
/// <c>string.CompareTo</c> (CurrentCulture) semantics - this is identical to EF Core. No SQL
/// collation equals .NET CurrentCulture, so nORM does not (and cannot portably) reproduce .NET
/// string ordering inside the database; it faithfully passes the column's collation through. On
/// SQLite the default column collation is BINARY (ordinal), so uppercase A-Z sort before lowercase
/// a-z. This test pins the pass-through - a regression that client-sorts or injects a different
/// collation is caught - and documents the intended divergence from .NET. For deterministic
/// cross-provider ordering, order by a normalized/ordinal key. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class StringOrderingContractTests
{
    [Table("StrOrdContract")]
    private sealed class E { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    private static DbContext Seed()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE StrOrdContract (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "INSERT INTO StrOrdContract (Name) VALUES ('a'),('B'),('c'),('Apple'),('apricot'),('Banana');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void OrderBy_string_follows_database_collation_not_dotnet_culture()
    {
        using var ctx = Seed();

        var norm = ((INormQueryable<E>)ctx.Query<E>())
            .AsNoTracking().OrderBy(e => e.Name).Select(e => e.Name).ToList();

        // SQLite's default column collation is BINARY (ordinal): uppercase (A-Z) sort before
        // lowercase (a-z). This is the database contract nORM passes through, not client sorting.
        Assert.Equal(new[] { "Apple", "B", "Banana", "a", "apricot", "c" }, norm);

        // Deliberately NOT the .NET culture-sensitive ordering - pins the intended divergence so it
        // can never silently change into a client-side sort.
        var dotnetCulture = new[] { "a", "B", "c", "Apple", "apricot", "Banana" }.OrderBy(x => x).ToList();
        Assert.NotEqual(dotnetCulture, norm);
    }
}
