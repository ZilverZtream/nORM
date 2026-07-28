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

namespace nORM.Tests;

/// <summary>
/// TakeWhile/SkipWhile over a nullable-column predicate must treat a NULL
/// predicate result as "not satisfied" (C# lifted comparison returns false),
/// stopping/resuming exactly where LINQ-to-Objects does. The window-flag SQL
/// used `SUM(CASE WHEN NOT(pred) THEN 1 ELSE 0 END)`, and NOT(NULL)=NULL folds
/// to the no-break branch, so a null-predicate row never triggered the prefix
/// cutover — a silent-wrong result.
/// </summary>
[Trait("Category", "Fast")]
public class TakeSkipWhileNullPredicate3vlTests
{
    [Table("WhileNull3vl")]
    private class WhileNull3vl
    {
        [Key] public int Id { get; set; }
        public int? NV { get; set; }
    }

    // Rows in Id order: (1, NV=10), (2, NV=null), (3, NV=20)
    private static (SqliteConnection Cn, DbContext Ctx, List<WhileNull3vl> Oracle) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE WhileNull3vl (Id INTEGER PRIMARY KEY, NV INTEGER NULL)";
            cmd.ExecuteNonQuery();
        }
        var oracle = new List<WhileNull3vl>
        {
            new() { Id = 1, NV = 10 },
            new() { Id = 2, NV = null },
            new() { Id = 3, NV = 20 },
        };
        foreach (var r in oracle)
        {
            using var insert = cn.CreateCommand();
            insert.CommandText = "INSERT INTO WhileNull3vl (Id, NV) VALUES (@id, @nv)";
            insert.Parameters.AddWithValue("@id", r.Id);
            insert.Parameters.AddWithValue("@nv", (object?)r.NV ?? DBNull.Value);
            insert.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()), oracle);
    }

    [Fact]
    public async Task TakeWhile_stops_at_null_predicate_row()
    {
        var (cn, ctx, oracle) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // C#: (int?)null < 30 is false -> TakeWhile stops at the null row -> {1}.
        var expected = oracle.OrderBy(r => r.Id).TakeWhile(r => r.NV < 30).Select(r => r.Id).ToArray();
        var actual = (await ctx.Query<WhileNull3vl>().OrderBy(r => r.Id).TakeWhile(r => r.NV < 30).ToListAsync())
            .Select(r => r.Id).ToArray();

        Assert.Equal(expected, actual); // expected {1}; nORM currently returns {1,2,3}
    }

    [Fact]
    public async Task SkipWhile_resumes_at_null_predicate_row()
    {
        var (cn, ctx, oracle) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // C#: SkipWhile(r.NV < 30): 10<30 true skip, null<30 false STOP -> ids {2,3}.
        var expected = oracle.OrderBy(r => r.Id).SkipWhile(r => r.NV < 30).Select(r => r.Id).ToArray();
        var actual = (await ctx.Query<WhileNull3vl>().OrderBy(r => r.Id).SkipWhile(r => r.NV < 30).ToListAsync())
            .Select(r => r.Id).ToArray();

        Assert.Equal(expected, actual); // expected {2,3}; nORM currently returns {}
    }
}
