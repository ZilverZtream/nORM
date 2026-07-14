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
/// [Flags] enum bitwise operations — the common permission/status-flag pattern — must match
/// LINQ-to-Objects in predicates and projections: (Access &amp; Flag) == Flag, (Access &amp; Flag) != 0,
/// (Access | Flag) == combo, a bitwise-AND mask projection, and the HasFlag method.
/// </summary>
[Trait("Category", "Fast")]
public class FlagsEnumBitwiseTests
{
    [Flags]
    public enum Perm { None = 0, Read = 1, Write = 2, Delete = 4, All = 7 }

    [Table("FebRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public Perm Access { get; set; }
    }

    // Access stored as its underlying int.
    private static readonly (int Id, Perm Access)[] Data =
    {
        (1, Perm.None), (2, Perm.Read), (3, Perm.Read | Perm.Write), (4, Perm.All), (5, Perm.Delete),
        (6, Perm.Write | Perm.Delete),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FebRow (Id INTEGER PRIMARY KEY, Access INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, access) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO FebRow VALUES ({id}, {(int)access})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, Access = d.Access });

    [Fact]
    public async Task HasFlag_via_and_equals_flag()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .Where(x => (x.Access & Perm.Write) == Perm.Write)
                .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
            var oracle = Oracle().Where(x => (x.Access & Perm.Write) == Perm.Write).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"AND==FLAG THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Has_flag_via_and_not_zero()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .Where(x => (x.Access & Perm.Delete) != 0)
                .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
            var oracle = Oracle().Where(x => (x.Access & Perm.Delete) != 0).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"AND!=0 THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Or_combination_equals()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .Where(x => (x.Access | Perm.Read) == Perm.All)
                .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
            var oracle = Oracle().Where(x => (x.Access | Perm.Read) == Perm.All).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"OR== THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Bitwise_and_in_projection()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>().OrderBy(x => x.Id)
                .Select(x => new { x.Id, Masked = (int)(x.Access & Perm.All) }).ToListAsync());
            var oracle = Oracle().OrderBy(x => x.Id).Select(x => (int)(x.Access & Perm.All)).ToList();
            Assert.Equal(oracle, got.OrderBy(x => x.Id).Select(x => x.Masked).ToList());
        }
        catch (Exception ex) { Assert.Fail($"PROJ-AND THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task HasFlag_method()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .Where(x => x.Access.HasFlag(Perm.Write))
                .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
            var oracle = Oracle().Where(x => x.Access.HasFlag(Perm.Write)).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"HASFLAG THREW: {ex.GetType().Name}: {ex.Message}"); }
    }
}
