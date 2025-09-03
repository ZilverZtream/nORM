using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class GroupJoinTests
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

    [Fact]
    public void GroupJoin_returns_grouped_results()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Person(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Pet(Id INTEGER, OwnerId INTEGER, Name TEXT);" +
                "INSERT INTO Person VALUES(1,'Alice');" +
                "INSERT INTO Person VALUES(2,'Bob');" +
                "INSERT INTO Pet VALUES(1,1,'Fido');" +
                "INSERT INTO Pet VALUES(2,1,'Rex');" +
                "INSERT INTO Pet VALUES(3,2,'Whiskers');";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<Person>()
            .GroupJoin(ctx.Query<Pet>(), p => p.Id, pet => pet.OwnerId,
                (p, pets) => new { Person = p, Pets = pets.ToList() })
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Person.Name);
        Assert.Equal(2, results[0].Pets.Count);
        Assert.Equal("Bob", results[1].Person.Name);
        Assert.Single(results[1].Pets);
    }
}
