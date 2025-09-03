using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CompiledQueryTests
{
    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task Compiled_query_executes_with_different_parameters()
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

        var compiled = Norm.CompileQuery((DbContext ctx, int id) => ctx.Query<Person>().Where(p => p.Id == id));

        using var ctx = new DbContext(cn, new SqliteProvider());
        var result1 = await compiled(ctx, 1);
        Assert.Single(result1);
        Assert.Equal("Alice", result1[0].Name);

        var result2 = await compiled(ctx, 2);
        Assert.Single(result2);
        Assert.Equal("Bob", result2[0].Name);
    }
}

