using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class StoredProcedureAsyncEnumerableTests
{
    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsAsyncEnumerable_streams_results()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Person(Id INTEGER, Name TEXT);" +
                             "INSERT INTO Person VALUES(1,'Alice');" +
                             "INSERT INTO Person VALUES(2,'Bob');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = new List<Person>();
        await foreach (var p in ctx.ExecuteStoredProcedureAsAsyncEnumerable<Person>("SELECT Id, Name FROM Person ORDER BY Id"))
        {
            results.Add(p);
        }
        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }
}
