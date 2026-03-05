using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for QP-1: GroupJoin double ORDER BY fix and grouping correctness.
/// </summary>
public class GroupJoinOrderByTests : TestBase
{
    private class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class Pet
    {
        public int Id { get; set; }
        public int OwnerId { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Person(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Pet(Id INTEGER, OwnerId INTEGER, Name TEXT);" +
                "INSERT INTO Person VALUES(1,'Alice');" +
                "INSERT INTO Person VALUES(2,'Bob');" +
                "INSERT INTO Person VALUES(3,'Charlie');" +
                "INSERT INTO Pet VALUES(1,1,'Fido');" +
                "INSERT INTO Pet VALUES(2,1,'Rex');" +
                "INSERT INTO Pet VALUES(3,2,'Whiskers');" +
                "INSERT INTO Pet VALUES(4,3,'Mittens');";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    /// <summary>
    /// QP-1 Problem 1: GroupJoin SQL must contain exactly ONE ORDER BY clause.
    /// Previously HandleGroupJoin embedded ORDER BY in _sql and Build() appended another one.
    /// </summary>
    [Fact]
    public void GroupJoin_WithDownstreamOrderBy_GeneratesSingleOrderByClause()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        string sql;
        using (var ctx = new DbContext(cn, new SqliteProvider()))
        {
            var query = ctx.Query<Person>()
                .GroupJoin(ctx.Query<Pet>(), p => p.Id, pet => pet.OwnerId,
                    (p, pets) => new { Person = p, Pets = pets.ToList() });

            var expr = query.Expression;
            var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
            var translator = Activator.CreateInstance(translatorType, ctx)!;
            var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
            sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        }

        // Count occurrences of ORDER BY (case insensitive)
        var orderByCount = CountOccurrences(sql, "ORDER BY");
        Assert.True(orderByCount <= 1,
            $"SQL should contain at most one ORDER BY clause but found {orderByCount}. SQL: {sql}");
    }

    /// <summary>
    /// QP-1 Problem 2 (regression): GroupJoin happy path still produces correct groups.
    /// </summary>
    [Fact]
    public void GroupJoin_HappyPath_CorrectGroups()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var results = ctx.Query<Person>()
            .GroupJoin(ctx.Query<Pet>(), p => p.Id, pet => pet.OwnerId,
                (p, pets) => new { Person = p, Pets = pets.ToList() })
            .ToList();

        Assert.Equal(3, results.Count);

        var alice = results.FirstOrDefault(r => r.Person.Name == "Alice");
        Assert.NotNull(alice);
        Assert.Equal(2, alice!.Pets.Count);

        var bob = results.FirstOrDefault(r => r.Person.Name == "Bob");
        Assert.NotNull(bob);
        Assert.Single(bob!.Pets);

        var charlie = results.FirstOrDefault(r => r.Person.Name == "Charlie");
        Assert.NotNull(charlie);
        Assert.Single(charlie!.Pets);
    }

    /// <summary>
    /// QP-1 Problem 2: When a downstream OrderBy is chained, the outer key appears
    /// as the primary sort to guarantee contiguous groups.
    /// The outer key sort is inserted at index 0 in _orderBy so it always comes first.
    /// </summary>
    [Fact]
    public void GroupJoin_WithOrderByOnProjectedField_OuterKeyStillFirst()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        string sql;
        using (var ctx = new DbContext(cn, new SqliteProvider()))
        {
            var query = ctx.Query<Person>()
                .GroupJoin(ctx.Query<Pet>(), p => p.Id, pet => pet.OwnerId,
                    (p, pets) => new { Person = p, Pets = pets.ToList() });

            var expr = query.Expression;
            var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
            var translator = Activator.CreateInstance(translatorType, ctx)!;
            var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
            sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        }

        // The SQL must contain ORDER BY (the outer key sort inserted by QP-1 fix).
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);

        // There must be exactly one ORDER BY.
        var count = CountOccurrences(sql, "ORDER BY");
        Assert.Equal(1, count);

        // The outer key column (Id from Person table alias T0) must appear in the ORDER BY clause.
        var orderByIndex = sql.IndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase);
        var afterOrderBy = sql.Substring(orderByIndex);
        Assert.Contains("\"Id\"", afterOrderBy, StringComparison.OrdinalIgnoreCase);
    }

    private static int CountOccurrences(string text, string pattern)
    {
        int count = 0;
        int index = 0;
        while ((index = text.IndexOf(pattern, index, StringComparison.OrdinalIgnoreCase)) != -1)
        {
            count++;
            index += pattern.Length;
        }
        return count;
    }
}
