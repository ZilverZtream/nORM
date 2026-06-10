using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class CompiledQueryTests
{
    [Xunit.Trait("Category", "Fast")]
    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Xunit.Trait("Category", "Fast")]
    public class PersonInfo
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string City { get; set; } = string.Empty;
    }

    [Xunit.Trait("Category", "Fast")]
    public class PagedPerson
    {
        public int Id { get; set; }
        public int GroupId { get; set; }
        public int Score { get; set; }
    }

    private sealed class MutableIdHolder
    {
        public int[] Values { get; set; } = Array.Empty<int>();
    }

    public static TheoryData<DatabaseProvider> LimitOffsetProviders() => new()
    {
        new SqliteProvider(),
        new MySqlProvider(new SqliteParameterFactory()),
        new PostgresProvider(new SqliteParameterFactory())
    };

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

 /// <summary>
 /// A compiled query whose plan has multiple SQL parameters can bind each member
 /// from a typed parameter object without reusing the whole object for every SQL parameter.
 /// </summary>
    [Xunit.Trait("Category", "Fast")]
    public class TwoParamQuery
    {
        public int MinAge { get; set; }
        public string City { get; set; } = string.Empty;
    }

    [Fact]
    [Xunit.Trait("Category", TestCategory.CompiledQueryStress)]
    public async Task Compiled_query_multi_param_object_binds_members_independently()
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
        using var ctx = new DbContext(cn, new SqliteProvider());

 // Custom non-tuple class with two properties → plan will have 2 compiled params.
 // Passing it as a non-tuple value must throw, not silently fan-out the same object.
        var compiled = Norm.CompileQuery<DbContext, TwoParamQuery, PersonInfo>(
            (ctx2, p) => ctx2.Query<PersonInfo>().Where(x => x.Age > p.MinAge && x.City == p.City));

        var param = new TwoParamQuery { MinAge = 25, City = "LA" };
        var result = await compiled(ctx, param);

        Assert.Single(result);
        Assert.Equal(2, result[0].Id);

        param.MinAge = 35;
        param.City = "NY";
        var noMatch = await compiled(ctx, param);
        Assert.Empty(noMatch);
    }

    [Fact]
    [Xunit.Trait("Category", TestCategory.CompiledQueryStress)]
    public async Task Compiled_query_multi_param_object_null_parameter_throws_clear_error()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PersonInfo(Id INTEGER, Name TEXT, Age INTEGER, City TEXT);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery<DbContext, TwoParamQuery?, PersonInfo>(
            (ctx2, p) => ctx2.Query<PersonInfo>().Where(x => x.Age > p!.MinAge && x.City == p.City));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => compiled(ctx, null));
        Assert.Contains("cannot be null", ex.Message);
        Assert.Contains(nameof(TwoParamQuery.MinAge), ex.Message);
    }

 /// <summary>
 /// non-regression: tuple params still work, single-param non-tuple still works.
 /// </summary>
    [Fact]
    public async Task Compiled_query_single_param_non_tuple_still_works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Person(Id INTEGER, Name TEXT);" +
                             "INSERT INTO Person VALUES(1,'Alice');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext ctx2, int id) =>
            ctx2.Query<Person>().Where(p => p.Id == id));

        var result = await compiled(ctx, 1);
        Assert.Single(result);
        Assert.Equal("Alice", result[0].Name);
    }

    [Fact]
    public async Task Compiled_query_captured_collection_change_rebuilds_plan()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Person(Id INTEGER, Name TEXT);" +
                             "INSERT INTO Person VALUES(1,'Alice');" +
                             "INSERT INTO Person VALUES(2,'Bob');" +
                             "INSERT INTO Person VALUES(3,'Carol');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var holder = new MutableIdHolder { Values = new[] { 1 } };
        var compiled = Norm.CompileQuery((DbContext ctx2, int _) =>
            ctx2.Query<Person>().Where(p => holder.Values.Contains(p.Id)));

        var first = await compiled(ctx, 0);
        Assert.Single(first);
        Assert.Equal("Alice", first[0].Name);

        holder.Values = new[] { 2, 3 };
        var second = await compiled(ctx, 0);

        Assert.Equal(2, second.Count);
        Assert.DoesNotContain(second, p => p.Id == 1);
    }

    [Fact]
    public async Task Compiled_query_where_skip_take_binds_parameters_in_sql_order()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PagedPerson(Id INTEGER, GroupId INTEGER, Score INTEGER);";
            for (int i = 1; i <= 10; i++)
                cmd.CommandText += $"INSERT INTO PagedPerson VALUES({i}, 7, {i * 10});";
            cmd.CommandText += "INSERT INTO PagedPerson VALUES(99, 8, 999);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery<DbContext, (int GroupId, int Skip, int Take), PagedPerson>(
            (ctx2, p) => ctx2.Query<PagedPerson>()
                .Where(x => x.GroupId == p.GroupId)
                .OrderBy(x => x.Score)
                .Skip(p.Skip)
                .Take(p.Take));

        var result = await compiled(ctx, (7, 2, 5));

        Assert.Equal(5, result.Count);
        Assert.Equal(new[] { 30, 40, 50, 60, 70 }, result.Select(p => p.Score).ToArray());
        Assert.All(result, p => Assert.Equal(7, p.GroupId));
    }

    [Theory]
    [MemberData(nameof(LimitOffsetProviders))]
    public async Task Compiled_query_where_skip_take_binds_parameters_across_limit_offset_providers(DatabaseProvider provider)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PagedPerson(Id INTEGER, GroupId INTEGER, Score INTEGER);";
            for (int i = 1; i <= 8; i++)
                cmd.CommandText += $"INSERT INTO PagedPerson VALUES({i}, 11, {i * 10});";
            cmd.CommandText += "INSERT INTO PagedPerson VALUES(99, 12, 999);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, provider);

        var compiled = Norm.CompileQuery<DbContext, (int GroupId, int Skip, int Take), PagedPerson>(
            (ctx2, p) => ctx2.Query<PagedPerson>()
                .Where(x => x.GroupId == p.GroupId)
                .OrderBy(x => x.Score)
                .Skip(p.Skip)
                .Take(p.Take));

        var result = await compiled(ctx, (11, 1, 3));

        Assert.Equal(3, result.Count);
        Assert.Equal(new[] { 20, 30, 40 }, result.Select(p => p.Score).ToArray());
        Assert.All(result, p => Assert.Equal(11, p.GroupId));
    }
}
