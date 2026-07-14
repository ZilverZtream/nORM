using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A predicate comparing a bool column against a COMPARISON RESULT
/// (x.Flag == (x.Id % 2 == 0)) must translate to provider-native boolean
/// syntax: SQL Server has no boolean expression type, so the comparison
/// operand needs a CASE (or logical-equivalence) form — a bare
/// `Flag = (Id % 2 = 0)` is a syntax error.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class BoolComparisonPredicateLiveTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BoolCmp_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public bool Flag { get; set; }
    }

    private static (Func<DbConnection>?, string?) OpenLive()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_SQLSERVER not set");
        var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
        return (() =>
        {
            var cn = (DbConnection)Activator.CreateInstance(t, cs)!;
            cn.Open();
            return cn;
        }, null);
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Bool_column_compared_to_comparison_result_translates_on_sql_server()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;

        Exec(factory!, "IF OBJECT_ID('BoolCmp_Test') IS NOT NULL DROP TABLE BoolCmp_Test");
        Exec(factory!, "CREATE TABLE BoolCmp_Test (Id INT PRIMARY KEY, IntVal INT NOT NULL, Flag BIT NOT NULL)");
        try
        {
            using var ctx = new DbContext(factory!(), new SqlServerProvider());
            for (var id = 1; id <= 8; id++)
                ctx.Add(new Row { Id = id, IntVal = id * 10, Flag = id % 3 == 0 });
            await ctx.SaveChangesAsync();

            var all = await ctx.Query<Row>().ToListAsync();
            var expected = all.Where(x => x.Flag == (x.Id % 2 == 0)).Select(x => x.Id).OrderBy(x => x).ToList();

            // SELECT with the bool-equality predicate.
            var selected = (await ctx.Query<Row>().Where(x => x.Flag == (x.Id % 2 == 0)).ToListAsync())
                .Select(x => x.Id).OrderBy(x => x).ToList();
            Assert.True(expected.SequenceEqual(selected),
                $"select: expected [{string.Join(",", expected)}] got [{string.Join(",", selected)}]");

            // ExecuteUpdate with the same predicate.
            var affected = await ctx.Query<Row>().Where(x => x.Flag == (x.Id % 2 == 0))
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.IntVal, 999));
            Assert.True(affected == expected.Count,
                $"update: expected {expected.Count} affected, got {affected}");

            using var verifyCtx = new DbContext(factory!(), new SqlServerProvider());
            var updated = (await verifyCtx.Query<Row>().ToListAsync())
                .Where(x => x.IntVal == 999).Select(x => x.Id).OrderBy(x => x).ToList();
            Assert.True(expected.SequenceEqual(updated),
                $"updated rows: expected [{string.Join(",", expected)}] got [{string.Join(",", updated)}]");
        }
        finally
        {
            Exec(factory!, "IF OBJECT_ID('BoolCmp_Test') IS NOT NULL DROP TABLE BoolCmp_Test");
        }
    }
}
