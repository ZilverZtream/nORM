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
/// Pins server-side translation of bitwise operators on integer columns. Common
/// in flag-style permission / state filtering:
/// <list type="bullet">
///   <item><c>Where(x =&gt; (x.Flags &amp; 4) == 4)</c> — bit-mask test</item>
///   <item><c>Where(x =&gt; (x.Flags | 1) == x.Flags)</c> — read-only bit check</item>
///   <item><c>Where(x =&gt; (x.Flags ^ 1) == 0)</c> — XOR equality</item>
/// </list>
/// The ExpressionTree node types are ExpressionType.And / Or / ExclusiveOr when
/// the operands are non-bool — these are bitwise, not boolean. They must lower
/// to SQL <c>&amp; | ^</c> rather than throwing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqBitwiseOperatorTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE BwRow (Id INTEGER PRIMARY KEY, Flags INTEGER NOT NULL);
            -- 1=Read, 2=Write, 4=Admin, 8=Delete; rows expose every interesting mask.
            INSERT INTO BwRow VALUES
                (1, 1),   -- Read
                (2, 3),   -- Read+Write
                (3, 5),   -- Read+Admin
                (4, 7),   -- Read+Write+Admin
                (5, 0);   -- none
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
    public async Task Bitwise_and_mask_test_filters_rows_with_bit_set()
    {
        // Admin bit (4): rows 3 (5=0101) and 4 (7=0111). 1 (0001), 2 (0011), 5 (0000) miss.
        var rows = (await _ctx.Query<BwRow>().Where(r => (r.Flags & 4) == 4).ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(3, rows[0].Id);
        Assert.Equal(4, rows[1].Id);
    }

    [Fact]
    public async Task Bitwise_or_identity_filters_rows_already_having_bit()
    {
        // `Flags | 1 == Flags` means the Read bit (1) is already set in Flags.
        // Rows 1 (1), 2 (3), 3 (5), 4 (7) have bit 1 → 4 rows match.
        var rows = (await _ctx.Query<BwRow>().Where(r => (r.Flags | 1) == r.Flags).ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(new[] { 1, 2, 3, 4 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Bitwise_xor_filters_rows_that_collapse_to_zero()
    {
        // Flags ^ 5 == 0 means Flags == 5 → row 3 only.
        var rows = await _ctx.Query<BwRow>().Where(r => (r.Flags ^ 5) == 0).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Id);
    }

    [Table("BwRow")]
    public sealed class BwRow
    {
        [Key] public int Id { get; set; }
        public int Flags { get; set; }
    }
}
