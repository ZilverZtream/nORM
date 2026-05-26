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
/// Pins LINQ <c>Aggregate</c> where the fold body contains a conditional
/// (ternary) expression: <c>acc + (pred ? 1 : 0)</c>. This pattern counts
/// rows matching a predicate in a single server-side pass and must lower to
/// <c>SUM(CASE WHEN pred THEN 1 ELSE 0 END)</c> — not a client-side
/// materialise-and-count.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateConditionalFoldTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    // Rows: (1,10,'a'), (2,20,'b'), (3,30,'a'), (4,40,'b'), (5,5,'a')
    // Score > 15: rows 2,3,4 → count = 3
    // Cat == 'a': rows 1,3,5 → count = 3
    // Score > 25 AND Cat == 'b': rows 4 → count = 1

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AcfRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Cat TEXT NOT NULL);
            INSERT INTO AcfRow VALUES (1,10,'a'),(2,20,'b'),(3,30,'a'),(4,40,'b'),(5,5,'a');
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
    public async Task Aggregate_conditional_fold_counts_rows_matching_numeric_predicate_server_side()
    {
        await Task.CompletedTask;
        // Rows with Score > 15: 2(20), 3(30), 4(40) → 3
        int count = _ctx.Query<AcfRow>()
            .Aggregate(0, (acc, x) => acc + (x.Score > 15 ? 1 : 0));
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task Aggregate_conditional_fold_with_equality_predicate_counts_correctly()
    {
        await Task.CompletedTask;
        // Cat == 'a': rows 1,3,5 → 3
        int count = _ctx.Query<AcfRow>()
            .Aggregate(0, (acc, x) => acc + (x.Cat == "a" ? 1 : 0));
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task Aggregate_conditional_fold_with_composite_predicate_counts_correctly()
    {
        await Task.CompletedTask;
        // Score > 25 AND Cat == 'b': row 4 → 1
        int count = _ctx.Query<AcfRow>()
            .Aggregate(0, (acc, x) => acc + (x.Score > 25 && x.Cat == "b" ? 1 : 0));
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Aggregate_conditional_fold_with_custom_weight_sums_weights()
    {
        await Task.CompletedTask;
        // Score > 15 → weight 2, else weight 1. Total = 1+2+2+2+1 = 8.
        int weighted = _ctx.Query<AcfRow>()
            .Aggregate(0, (acc, x) => acc + (x.Score > 15 ? 2 : 1));
        Assert.Equal(8, weighted);
    }

    [Fact]
    public async Task Aggregate_conditional_fold_on_empty_table_returns_seed()
    {
        await using (var del = _cn.CreateCommand()) { del.CommandText = "DELETE FROM AcfRow"; await del.ExecuteNonQueryAsync(); }
        int count = _ctx.Query<AcfRow>()
            .Aggregate(0, (acc, x) => acc + (x.Score > 15 ? 1 : 0));
        Assert.Equal(0, count);
    }

    [Table("AcfRow")]
    public sealed class AcfRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public string Cat { get; set; } = "";
    }
}
