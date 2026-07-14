using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Seeded oracle machine for the value-converter × correlated-subquery family:
/// a Status column stored as a string via a converter, queried through explicit
/// ctx.Query subqueries in both predicate and projection positions with a
/// closure-captured status that churns per case. Each case runs on ONE shared
/// call site (the plan cache is exercised, not dodged) and is checked against
/// LINQ-to-Objects — catching both the fold-and-cache replay and the dropped
/// converter registration.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedSubqueryConverterFuzzTests
{
    public enum Status { Draft = 0, Open = 1, Closed = 2, Archived = 3 }

    [Table("CscfParent_Test")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
    }

    [Table("CscfChild_Test")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public Status Status { get; set; }
        public int Amount { get; set; }
    }

    private sealed class StatusConverter : IValueConverter
    {
        public Type ModelType => typeof(Status);
        public Type ProviderType => typeof(string);
        public object ConvertToProvider(object? value) => ((Status)value!).ToString();
        public object ConvertFromProvider(object? value) => Enum.Parse<Status>((string)value!);
    }

    private static readonly Status[] AllStatuses = { Status.Draft, Status.Open, Status.Closed, Status.Archived };

    [Theory]
    [InlineData(20260714)]
    [InlineData(7788)]
    [InlineData(555_010_203)]
    public async Task Converter_subqueries_match_linq_across_churned_closures(int seed)
    {
        var rng = new Random(seed);
        var cs = $"Data Source=file:cscf_{Guid.NewGuid():N}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CscfParent_Test (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL);
                CREATE TABLE CscfChild_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Status TEXT NOT NULL, Amount INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }

        // Seed an in-memory oracle mirror.
        var parents = new List<Parent>();
        var children = new List<Child>();
        var cnSeed = new SqliteConnection(cs);
        cnSeed.Open();
        var seedOpts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().Property(p => p.Status).HasConversion(new StatusConverter());
                mb.Entity<Child>().Property(c => c.Status).HasConversion(new StatusConverter());
            }
        };
        await using (var seedCtx = new DbContext(cnSeed, new SqliteProvider(), seedOpts))
        {
            for (var i = 1; i <= 8; i++)
            {
                var p = new Parent { Id = i, Status = AllStatuses[rng.Next(AllStatuses.Length)] };
                parents.Add(p);
                seedCtx.Add(p);
            }
            var childId = 1;
            foreach (var p in parents)
            {
                var count = rng.Next(0, 4);
                for (var k = 0; k < count; k++)
                {
                    var c = new Child { Id = childId++, ParentId = p.Id, Status = AllStatuses[rng.Next(AllStatuses.Length)], Amount = rng.Next(1, 100) };
                    children.Add(c);
                    seedCtx.Add(c);
                }
            }
            await seedCtx.SaveChangesAsync();
        }

        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().Property(p => p.Status).HasConversion(new StatusConverter());
                mb.Entity<Child>().Property(c => c.Status).HasConversion(new StatusConverter());
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        for (var caseIdx = 0; caseIdx < 40; caseIdx++)
        {
            var status = AllStatuses[rng.Next(AllStatuses.Length)];
            var threshold = rng.Next(0, 3);
            var shape = rng.Next(4);

            switch (shape)
            {
                case 0:
                {
                    // Projection: count children of a captured status per parent.
                    var expected = parents
                        .Select(p => (p.Id, N: children.Count(c => c.ParentId == p.Id && c.Status == status)))
                        .OrderBy(t => t.Id).ToList();
                    var actual = (await ctx.Query<Parent>()
                            .Select(p => new { p.Id, N = ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Status == status) })
                            .ToListAsync())
                        .Select(x => (x.Id, x.N)).OrderBy(t => t.Id).ToList();
                    Assert.True(expected.SequenceEqual(actual),
                        $"seed={seed} case={caseIdx} shape=proj-count status={status}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
                    break;
                }
                case 1:
                {
                    // Predicate: parents with >= threshold children of the captured status.
                    var expected = parents
                        .Where(p => children.Count(c => c.ParentId == p.Id && c.Status == status) >= threshold)
                        .Select(p => p.Id).OrderBy(i => i).ToList();
                    var actual = (await ctx.Query<Parent>()
                            .Where(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id && c.Status == status) >= threshold)
                            .ToListAsync())
                        .Select(p => p.Id).OrderBy(i => i).ToList();
                    Assert.True(expected.SequenceEqual(actual),
                        $"seed={seed} case={caseIdx} shape=pred-count status={status} thr={threshold}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
                    break;
                }
                case 2:
                {
                    // Predicate: EXISTS a child of the captured status.
                    var expected = parents
                        .Where(p => children.Any(c => c.ParentId == p.Id && c.Status == status))
                        .Select(p => p.Id).OrderBy(i => i).ToList();
                    var actual = (await ctx.Query<Parent>()
                            .Where(p => ctx.Query<Child>().Any(c => c.ParentId == p.Id && c.Status == status))
                            .ToListAsync())
                        .Select(p => p.Id).OrderBy(i => i).ToList();
                    Assert.True(expected.SequenceEqual(actual),
                        $"seed={seed} case={caseIdx} shape=pred-any status={status}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
                    break;
                }
                default:
                {
                    // Projection: sum amounts of children of the captured status.
                    var expected = parents
                        .Select(p => (p.Id, S: children.Any(c => c.ParentId == p.Id && c.Status == status)
                            ? (int?)children.Where(c => c.ParentId == p.Id && c.Status == status).Sum(c => c.Amount)
                            : null))
                        .OrderBy(t => t.Id).ToList();
                    var actual = (await ctx.Query<Parent>()
                            .Select(p => new { p.Id, S = (int?)ctx.Query<Child>().Where(c => c.ParentId == p.Id && c.Status == status).Sum(c => c.Amount) })
                            .ToListAsync())
                        .Select(x => (x.Id, x.S)).OrderBy(t => t.Id).ToList();
                    Assert.True(expected.SequenceEqual(actual),
                        $"seed={seed} case={caseIdx} shape=proj-sum status={status}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
                    break;
                }
            }
        }
    }
}
