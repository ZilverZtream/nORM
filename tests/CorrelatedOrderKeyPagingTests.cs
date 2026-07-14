using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A correlated subquery used as an ORDER BY key, composed with paging: the
/// subquery ordering must be applied BEFORE the LIMIT/OFFSET so the right window
/// of rows is returned. A dropped or mis-placed order key silently pages a
/// differently-ordered (or unordered) set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedOrderKeyPagingTests
{
    [Table("CokParent_Test")]
    public class Parent
    {
        [Key] public int Id { get; set; }
    }

    [Table("CokChild_Test")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx, List<Parent> Parents, List<Child> Children) CreateDb()
    {
        var cs = $"Data Source=file:cok_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CokParent_Test (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE CokChild_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var parents = new List<Parent>();
        var children = new List<Child>();
        var childId = 1;
        // Parent i has i children (1..6), so child counts are 1,2,3,4,5,6.
        for (var i = 1; i <= 6; i++)
        {
            parents.Add(new Parent { Id = i });
            for (var k = 0; k < i; k++)
                children.Add(new Child { Id = childId++, ParentId = i, Val = k });
        }
        using (var cmd = keeper.CreateCommand())
        {
            foreach (var p in parents) { cmd.CommandText = $"INSERT INTO CokParent_Test VALUES ({p.Id})"; cmd.ExecuteNonQuery(); }
            foreach (var c in children) { cmd.CommandText = $"INSERT INTO CokChild_Test VALUES ({c.Id},{c.ParentId},{c.Val})"; cmd.ExecuteNonQuery(); }
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()), parents, children);
    }

    [Fact]
    public async Task OrderBy_correlated_count_then_paging_matches_linq()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        foreach (var (skip, take) in new[] { (0, 3), (2, 2), (4, 10), (1, 3) })
        {
            var expected = parents
                .OrderByDescending(p => children.Count(c => c.ParentId == p.Id))
                .ThenBy(p => p.Id)
                .Skip(skip).Take(take)
                .Select(p => p.Id).ToList();
            var actual = (await ctx.Query<Parent>()
                    .OrderByDescending(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id))
                    .ThenBy(p => p.Id)
                    .Skip(skip).Take(take)
                    .ToListAsync())
                .Select(p => p.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"skip={skip},take={take}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }

    [Fact]
    public async Task OrderBy_correlated_count_with_closure_then_paging_rebinds()
    {
        var (keeper, ctx, parents, children) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // The order key's subquery carries a churned closure cut; paging over the
        // reordered set must reflect the current cut, not a cached one.
        foreach (var cut in new[] { 0, 2, 4 })
        {
            var expected = parents
                .OrderByDescending(p => children.Count(c => c.ParentId == p.Id && c.Val >= cut))
                .ThenBy(p => p.Id)
                .Skip(1).Take(3)
                .Select(p => p.Id).ToList();
            var actual = (await ctx.Query<Parent>()
                    .OrderByDescending(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Val >= cut))
                    .ThenBy(p => p.Id)
                    .Skip(1).Take(3)
                    .ToListAsync())
                .Select(p => p.Id).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"cut={cut}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
        }
    }
}
