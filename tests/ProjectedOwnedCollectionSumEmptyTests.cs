using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Projecting an owned-collection <c>Sum(...)</c> over an empty collection must yield 0 (Enumerable.Sum
/// semantics), not the SQL NULL a bare SUM produces. For a non-nullable projection target the materializer
/// may coerce NULL to 0 (so the gap is invisible), but a nullable target surfaces the NULL as a wrong
/// <c>null</c> — which Enumerable.Sum never returns for an empty sequence.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectedOwnedCollectionSumEmptyTests
{
    [Table("PocOwner")]
    public class Owner
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Amount { get; set; }
    }

    private static DbContext Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PocOwner (Id INTEGER PRIMARY KEY);
                CREATE TABLE PocLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO PocOwner (Id) VALUES (1), (2);
                INSERT INTO PocLine (OwnerId, Amount) VALUES (1, 7);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Owner>().HasKey(o => o.Id);
                mb.Entity<Owner>().OwnsMany<Line>(o => o.Lines, tableName: "PocLine", foreignKey: "OwnerId");
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // Non-nullable int target: Owner 2 (empty) must project 0.
    [Fact]
    public void Projected_owned_sum_nonnullable_empty_is_zero()
    {
        using var ctx = Create();
        var byId = ctx.Query<Owner>().OrderBy(o => o.Id)
            .Select(o => new { o.Id, Total = o.Lines.Sum(l => l.Amount) })
            .ToList().ToDictionary(r => r.Id, r => r.Total);
        Assert.Equal(7, byId[1]);
        Assert.Equal(0, byId[2]);
    }

    // Nullable int target: Enumerable.Sum over an empty sequence is 0, never null.
    [Fact]
    public void Projected_owned_sum_nullable_empty_is_zero_not_null()
    {
        using var ctx = Create();
        var byId = ctx.Query<Owner>().OrderBy(o => o.Id)
            .Select(o => new { o.Id, Total = o.Lines.Sum(l => (int?)l.Amount) })
            .ToList().ToDictionary(r => r.Id, r => r.Total);
        Assert.Equal(7, byId[1]);
        Assert.Equal(0, byId[2]);
    }
}
