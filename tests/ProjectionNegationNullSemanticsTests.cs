using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A negated comparison in a PROJECTION (a ternary test or a projected bool) must follow C#'s
/// three-valued semantics, exactly as the WHERE side already does: (int?)null > 5 is false, so
/// !(null > 5) is TRUE. The projection visitor emitted a bare NOT(...), which stays UNKNOWN for a NULL
/// operand and falls to the CASE's ELSE — the wrong branch. The negation must push to the comparison
/// leaves with an IS NULL rescue so a NULL row is rescued to true.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionNegationNullSemanticsTests
{
    [Table("PnnRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int? Score { get; set; }
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PnnRow (Id INTEGER PRIMARY KEY, Score INTEGER NULL);
                INSERT INTO PnnRow (Id, Score) VALUES (1, NULL), (2, 3), (3, 10);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Negated_relational_in_projection_ternary_matches_dotnet()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Row>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Band = !(r.Score > 5) ? "low" : "high" })
            .ToListAsync())
            .ToDictionary(r => r.Id, r => r.Band);

        // C#: (int?)null > 5 == false, so !(null > 5) == true -> "low".
        Assert.Equal("low", rows[1]);   // BUG: "high" — NOT(NULL) stayed UNKNOWN
        Assert.Equal("low", rows[2]);   // !(3 > 5)
        Assert.Equal("high", rows[3]);  // !(10 > 5)
    }

    [Fact]
    public async Task Negated_conjunction_in_projection_ternary_matches_dotnet()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Row>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Band = !(r.Score > 5 && r.Score < 20) ? "low" : "high" })
            .ToListAsync())
            .ToDictionary(r => r.Id, r => r.Band);

        Assert.Equal("low", rows[1]);   // !(false && ...) == true
        Assert.Equal("low", rows[2]);   // !(false && ...) == true
        Assert.Equal("high", rows[3]);  // !(true && true) == false
    }
}
