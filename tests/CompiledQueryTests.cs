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

    public class PersonInfo
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string City { get; set; } = string.Empty;
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

    [Fact]
    public async Task Compiled_query_with_tuple_parameter_executes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PersonInfo(Id INTEGER, Name TEXT, Age INTEGER, City TEXT);" +
                             "INSERT INTO PersonInfo VALUES(1,'Alice',30,'NY');" +
                             "INSERT INTO PersonInfo VALUES(2,'Bob',40,'LA');";
            cmd.ExecuteNonQuery();
        }

        var compiled = Norm.CompileQuery<DbContext, (int, string), PersonInfo>(
            (ctx, p) => ctx.Query<PersonInfo>().Where(x => x.Age > p.Item1 && x.City == p.Item2));

        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = await compiled(ctx, (35, "LA"));
        Assert.Single(result);
        Assert.Equal(2, result[0].Id);
    }
}

