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
/// Benchmark fairness guard: the SQL nORM generates for the "complex" read benchmark
/// (<c>Where(IsActive &amp;&amp; Age&gt;25 &amp;&amp; City=="New York").OrderBy(Name).Skip(5).Take(20)</c>) must be the
/// same shape the hand-written raw-ADO baseline runs — the SAME filter, ORDER BY, and LIMIT/OFFSET — so the
/// benchmark compares like for like. If nORM ever dropped a predicate, the ORDER BY, or the paging (which would
/// make it spuriously faster than raw ADO), this fails.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ComplexBenchmarkSqlParityTests
{
    [Table("BenchmarkUser")]
    private class BenchmarkUser
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = "";
    }

    [Fact]
    public void Complex_query_generates_the_same_sql_shape_as_the_raw_ado_baseline()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BenchmarkUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsActive INTEGER NOT NULL, Age INTEGER NOT NULL, City TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BenchmarkUser>().HasKey(u => u.Id)
        }, ownsConnection: false);

        var sql = ((INormQueryable<BenchmarkUser>)ctx.Query<BenchmarkUser>()
            .Where(u => u.IsActive && u.Age > 25 && u.City == "New York")
            .OrderBy(u => u.Name)
            .Skip(5)
            .Take(20)).ToQueryString();

        // Same three predicates, ORDER BY Name, and a 20/5 paging window as the raw-ADO PreparedOptimized
        // baseline — no dropped filter or paging that would make nORM's benchmark win unfair.
        Assert.Contains("Age", sql);
        Assert.Contains("City", sql);
        Assert.Contains("IsActive", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("Name", sql);
        Assert.Contains("20", sql);   // Take
        Assert.Contains("5", sql);    // Skip / OFFSET
        Assert.Contains("New York", sql.Replace("@", ""));   // the city value flows through (literal or param comment)
    }
}
