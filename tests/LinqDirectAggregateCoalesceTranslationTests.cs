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
/// Pins direct aggregate calls (<c>Sum/Min/Max/Average</c>) whose selector
/// allocates a SQL parameter — the parallel of the OrderBy / GroupBy / Join
/// parameter-merge bugs from c46beb9 / 3c02675 / 02e6b5f. A selector with
/// <c>?? fallback</c> binds the fallback constant; the aggregate path must
/// register it as a literal (not a compiled-runtime placeholder) so the
/// command's parameter list carries the value at execution time.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDirectAggregateCoalesceTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DaRow (Id INTEGER PRIMARY KEY, Amount INTEGER NULL);
            INSERT INTO DaRow VALUES
                (1, 10),
                (2, NULL),
                (3, 20),
                (4, NULL),
                (5, 30);
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
    public async Task Sum_with_coalesce_selector_treats_nulls_as_zero()
    {
        // 10 + 0 + 20 + 0 + 30 = 60
        var total = await _ctx.Query<DaRow>().SumAsync(r => r.Amount ?? 0);
        Assert.Equal(60, total);
    }

    [Fact]
    public async Task Max_with_coalesce_selector_treats_nulls_as_min_sentinel()
    {
        // Max(10, MinValue, 20, MinValue, 30) = 30
        var max = await _ctx.Query<DaRow>().MaxAsync(r => r.Amount ?? int.MinValue);
        Assert.Equal(30, max);
    }

    [Table("DaRow")]
    public sealed class DaRow
    {
        [Key] public int Id { get; set; }
        public int? Amount { get; set; }
    }
}
