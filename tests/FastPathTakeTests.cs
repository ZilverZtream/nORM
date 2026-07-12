using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The simple-query fast path accepted Take(n) but never emitted a LIMIT/TOP, so it silently
/// returned the whole table instead of the first n rows — a HIGH wrong result on the common
/// pagination pattern. The fast path must decline Take so it falls through to the full translator.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class FastPathTakeTests
{
    [Table("FtRow")]
    private class FtRow
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FtRow (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL);" +
                              "INSERT INTO FtRow VALUES (1,1),(2,1),(3,1),(4,0),(5,1);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Take_limits_row_count()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        var rows = ctx.Query<FtRow>().Take(2).ToList();
        Assert.Equal(2, rows.Count); // BUG: returned all 5
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public void Where_then_Take_limits_matching_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // 4 rows are Active; Take(2) must cap at 2.
        var rows = ctx.Query<FtRow>().Where(r => r.Active).Take(2).ToList();
        Assert.Equal(2, rows.Count); // BUG: returned all 4 active rows
        Assert.All(rows, r => Assert.True(r.Active));
    }

    [Fact]
    public void Take_zero_returns_empty()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Empty(ctx.Query<FtRow>().Take(0).ToList());
    }
}
