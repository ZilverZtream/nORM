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
public class SetOpProjectionPagingTests
{
    [Table("SopRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly (int Id, int A, int B)[] Data =
    {
        (1, 8, 2), (2, 3, 9), (3, 7, 7), (4, 1, 1), (5, 10, 4), (6, 6, 6), (7, 2, 8),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SopRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, a, b) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO SopRow VALUES ({id},{a},{b})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, A = d.A, B = d.B });

    [Fact]
    public async Task Union_of_projections_ordered_and_paged()
    {
        await using var ctx = Make();
        try
        {
            // Ids where A > 5, unioned with ids where B > 5 (dedup), ordered, skip 1 take 3.
            var got = (await ctx.Query<Row>().Where(x => x.A > 5).Select(x => new { x.Id })
                .Union(ctx.Query<Row>().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id).Skip(1).Take(3)
                .ToListAsync());
            var oracle = Oracle().Where(x => x.A > 5).Select(x => new { x.Id })
                .Union(Oracle().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id).Skip(1).Take(3)
                .ToList();
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"UNION THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Concat_of_projections_keeps_duplicates_ordered()
    {
        await using var ctx = Make();
        try
        {
            // Concat keeps duplicates (an Id matching both filters appears twice).
            var got = (await ctx.Query<Row>().Where(x => x.A > 5).Select(x => new { x.Id })
                .Concat(ctx.Query<Row>().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id)
                .ToListAsync());
            var oracle = Oracle().Where(x => x.A > 5).Select(x => new { x.Id })
                .Concat(Oracle().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id)
                .ToList();
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"CONCAT THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Intersect_and_except_of_projections()
    {
        await using var ctx = Make();
        try
        {
            var gotI = (await ctx.Query<Row>().Where(x => x.A > 5).Select(x => new { x.Id })
                .Intersect(ctx.Query<Row>().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id).ToListAsync());
            var oracleI = Oracle().Where(x => x.A > 5).Select(x => new { x.Id })
                .Intersect(Oracle().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id).ToList();
            Assert.Equal(oracleI, gotI);

            var gotE = (await ctx.Query<Row>().Where(x => x.A > 5).Select(x => new { x.Id })
                .Except(ctx.Query<Row>().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id).ToListAsync());
            var oracleE = Oracle().Where(x => x.A > 5).Select(x => new { x.Id })
                .Except(Oracle().Where(x => x.B > 5).Select(x => new { x.Id }))
                .OrderBy(x => x.Id).ToList();
            Assert.Equal(oracleE, gotE);
        }
        catch (Exception ex) { Assert.Fail($"INTERSECT/EXCEPT THREW: {ex.GetType().Name}: {ex.Message}"); }
    }
}
