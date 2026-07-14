using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class ProjectThenFilterTests
{
    [Table("PtfRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly (int Id, int A, int B)[] Data =
    {
        (1, 3, 4), (2, 10, 1), (3, 0, 0), (4, 5, 5), (5, 8, 2), (6, 1, 1),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PtfRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, a, b) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO PtfRow VALUES ({id},{a},{b})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, A = d.A, B = d.B });

    [Fact]
    public async Task Filter_and_order_on_computed_projection_member()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .Select(r => new { r.Id, V = r.A * 2 + r.B })
                .Where(x => x.V > 10)
                .OrderByDescending(x => x.V).ThenBy(x => x.Id)
                .ToListAsync());
            var oracle = Oracle()
                .Select(r => new { r.Id, V = r.A * 2 + r.B })
                .Where(x => x.V > 10)
                .OrderByDescending(x => x.V).ThenBy(x => x.Id)
                .ToList();
            Assert.Equal(oracle.Select(x => x.Id).ToArray(), got.Select(x => x.Id).ToArray());
            Assert.Equal(oracle.Select(x => x.V).ToArray(), got.Select(x => x.V).ToArray());
        }
        catch (Exception ex) { Assert.Fail($"FILTER-ORDER THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Filter_on_projected_member_then_take()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .Select(r => new { r.Id, Sum = r.A + r.B })
                .Where(x => x.Sum >= 5)
                .OrderBy(x => x.Sum).ThenBy(x => x.Id)
                .Take(3)
                .ToListAsync());
            var oracle = Oracle()
                .Select(r => new { r.Id, Sum = r.A + r.B })
                .Where(x => x.Sum >= 5)
                .OrderBy(x => x.Sum).ThenBy(x => x.Id)
                .Take(3)
                .ToList();
            Assert.Equal(oracle.Select(x => x.Id).ToArray(), got.Select(x => x.Id).ToArray());
            Assert.Equal(oracle.Select(x => x.Sum).ToArray(), got.Select(x => x.Sum).ToArray());
        }
        catch (Exception ex) { Assert.Fail($"FILTER-TAKE THREW: {ex.GetType().Name}: {ex.Message}"); }
    }
}
