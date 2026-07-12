using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A closure captured inside a navigation-aggregate filter in a projection must use the caller's
/// CURRENT value on every run — plans are cached by fingerprint, so a baked literal would make a
/// second run with a different value silently reuse the first run's filter.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NavAggregateClosurePlanCacheTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NacP")]
    private class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Kid> Kids { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NacK")]
    private class Kid
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }

    [Fact]
    public void Nav_aggregate_filter_closure_rebinds_on_each_run()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NacP (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE NacK (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO NacP VALUES (1, 'a');
                INSERT INTO NacK VALUES (1, 1, 5), (2, 1, 10);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        int SumOver(int threshold)
        {
            var t = threshold; // closure capture inside the navigation filter
            return ctx.Query<Parent>()
                .Select(p => new { p.Id, S = p.Kids.Where(k => k.Amount > t).Sum(k => k.Amount) })
                .ToList().Single().S;
        }

        Assert.Equal(10, SumOver(7)); // only the 10 passes
        Assert.Equal(15, SumOver(3)); // both pass — must NOT reuse the 7 filter
    }
}
