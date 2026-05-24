using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies ExpressionFingerprint produces the same key for two structurally identical
/// queries — critical for the compiled-query plan cache. Composite-key GroupBy was a
/// concrete worry: it uses a NewExpression on the lambda body, and a fingerprint that
/// silently dropped that node would collide with single-column GroupBy queries.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExpressionFingerprintStabilityTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EfRow (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Region TEXT NOT NULL);
            INSERT INTO EfRow VALUES (1,'A','EU'),(2,'A','US'),(3,'B','EU');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public void CompositeKey_GroupBy_two_separate_query_builds_produce_equal_fingerprints()
    {
        var q1 = _ctx.Query<EfRow>()
            .GroupBy(r => new { r.Category, r.Region })
            .Select(g => new { Cat = g.Key.Category, Reg = g.Key.Region, Count = g.Count() });

        var q2 = _ctx.Query<EfRow>()
            .GroupBy(r => new { r.Category, r.Region })
            .Select(g => new { Cat = g.Key.Category, Reg = g.Key.Region, Count = g.Count() });

        var fp1 = ExpressionFingerprint.Compute(q1.Expression);
        var fp2 = ExpressionFingerprint.Compute(q2.Expression);
        Assert.Equal(fp1, fp2);
    }

    [Fact]
    public void CompositeKey_GroupBy_with_different_key_columns_produces_distinct_fingerprints()
    {
        var q1 = _ctx.Query<EfRow>().GroupBy(r => new { r.Category, r.Region }).Select(g => g.Count());
        var q2 = _ctx.Query<EfRow>().GroupBy(r => new { r.Region, r.Category }).Select(g => g.Count());
        var fp1 = ExpressionFingerprint.Compute(q1.Expression);
        var fp2 = ExpressionFingerprint.Compute(q2.Expression);
        // Different column ORDER means a different GROUP BY clause and a different
        // sort/segmentation cost; fingerprints must distinguish them so the planner
        // doesn't reuse a stale plan.
        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void CompositeKey_GroupBy_with_extra_column_produces_distinct_fingerprint()
    {
        var q1 = _ctx.Query<EfRow>().GroupBy(r => new { r.Category }).Select(g => g.Count());
        var q2 = _ctx.Query<EfRow>().GroupBy(r => new { r.Category, r.Region }).Select(g => g.Count());
        var fp1 = ExpressionFingerprint.Compute(q1.Expression);
        var fp2 = ExpressionFingerprint.Compute(q2.Expression);
        Assert.NotEqual(fp1, fp2);
    }

    [Table("EfRow")]
    public sealed class EfRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
    }
}
