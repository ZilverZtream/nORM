using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>Probe: composite (anonymous-type) join keys — translation shape and semantics.</summary>
[Xunit.Trait("Category", "Fast")]
public class CompositeJoinKeyProbeTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CjkL")]
    private class L
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string A { get; set; } = "";
        public int B { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CjkR")]
    private class R
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string A { get; set; } = "";
        public int B { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public void Composite_key_join_translates_and_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CjkL (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL);
                CREATE TABLE CjkR (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO CjkL VALUES (1,'x',1),(2,'y',2);
                INSERT INTO CjkR VALUES (1,'x',1,10),(2,'y',2,20),(3,'x',2,30);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = ctx.Query<L>()
            .Join(ctx.Query<R>(), l => new { l.A, l.B }, r => new { r.A, r.B }, (l, r) => new { l.Id, r.Val })
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.Val)).ToList();
        Assert.Equal(new[] { (1, 10), (2, 20) }, rows);
    }

    [Fact]
    public void Composite_key_groupjoin_translates_and_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CjkL (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL);
                CREATE TABLE CjkR (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO CjkL VALUES (1,'x',1),(2,'y',2);
                INSERT INTO CjkR VALUES (1,'x',1,10),(2,'y',2,20),(3,'x',1,30);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = ctx.Query<L>()
            .GroupJoin(ctx.Query<R>(), l => new { l.A, l.B }, r => new { r.A, r.B }, (l, rs) => new { l.Id, Total = rs.Sum(r => r.Val) })
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.Total)).ToList();
        Assert.Equal(new[] { (1, 40), (2, 20) }, rows);
    }
}
