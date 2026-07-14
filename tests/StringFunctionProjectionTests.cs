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

/// <summary>
/// String manipulation functions in a projection (Substring, Replace, IndexOf, ToUpper/
/// ToLower, Length) must match LINQ-to-Objects — guarding the 0-based-.NET vs 1-based-SQL
/// offsets and the -1/0 absent conventions. (Substring with a length exceeding the string is
/// a deliberate throw-vs-lenient divergence, matching EF, so the data keeps indices in range.)
/// </summary>
[Trait("Category", "Fast")]
public class StringFunctionProjectionTests
{
    [Table("SfpRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly (int Id, string Name)[] Data =
    {
        (1, "Alexander"), (2, "Bobby"), (3, "Charlie-Brown"), (4, "delta"), (5, "EchoEcho"),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SfpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, name) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO SfpRow VALUES ({id}, '{name}')";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, Name = d.Name });

    [Fact]
    public async Task Substring_start_length_matches_oracle()
    {
        await using var ctx = Make();
        // Substring is 0-based in .NET; SQLite substr is 1-based — nORM must offset.
        var got = (await ctx.Query<Row>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, S = r.Name.Substring(1, 3) }).ToListAsync());
        var oracle = Oracle().OrderBy(r => r.Id).Select(r => r.Name.Substring(1, 3)).ToList();
        Assert.Equal(oracle, got.OrderBy(x => x.Id).Select(x => x.S).ToList());
    }

    [Fact]
    public async Task Substring_from_start_matches_oracle()
    {
        await using var ctx = Make();
        var got = (await ctx.Query<Row>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, S = r.Name.Substring(2) }).ToListAsync());
        var oracle = Oracle().OrderBy(r => r.Id).Select(r => r.Name.Substring(2)).ToList();
        Assert.Equal(oracle, got.OrderBy(x => x.Id).Select(x => x.S).ToList());
    }

    [Fact]
    public async Task Replace_matches_oracle()
    {
        await using var ctx = Make();
        var got = (await ctx.Query<Row>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, S = r.Name.Replace("o", "0") }).ToListAsync());
        var oracle = Oracle().OrderBy(r => r.Id).Select(r => r.Name.Replace("o", "0")).ToList();
        Assert.Equal(oracle, got.OrderBy(x => x.Id).Select(x => x.S).ToList());
    }

    [Fact]
    public async Task IndexOf_matches_oracle()
    {
        await using var ctx = Make();
        // IndexOf is 0-based (-1 if absent) in .NET; SQL instr is 1-based (0 if absent).
        var got = (await ctx.Query<Row>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, N = r.Name.IndexOf("e") }).ToListAsync());
        var oracle = Oracle().OrderBy(r => r.Id).Select(r => r.Name.IndexOf("e")).ToList();
        Assert.Equal(oracle, got.OrderBy(x => x.Id).Select(x => x.N).ToList());
    }

    [Fact]
    public async Task ToUpper_ToLower_TrimLength_matches_oracle()
    {
        await using var ctx = Make();
        var got = (await ctx.Query<Row>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, U = r.Name.ToUpper(), L = r.Name.ToLower(), Len = r.Name.Length }).ToListAsync());
        var oracle = Oracle().OrderBy(r => r.Id).Select(r => new { r.Name.Length, U = r.Name.ToUpper(), L = r.Name.ToLower() }).ToList();
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].U, got[i].U);
            Assert.Equal(oracle[i].L, got[i].L);
            Assert.Equal(oracle[i].Length, got[i].Len);
        }
    }
}
