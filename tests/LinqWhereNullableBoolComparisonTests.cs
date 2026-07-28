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
/// Pins C#/LINQ-to-Objects (and EF Core default) null semantics for <c>Where</c>
/// over a nullable bool column — nORM compensates for SQL three-valued logic the
/// same way it does for <c>int? != x</c> and the <c>bool?</c> projection path:
/// <list type="bullet">
///   <item>
///     <c>== true</c> -> SQL <c>= 1</c>; NULL rows excluded (C#: <c>null == true</c>
///     is <c>false</c>; SQL <c>NULL = 1</c> is UNKNOWN — both agree, no rescue needed).
///   </item>
///   <item>
///     <c>!= true</c> -> SQL <c>(&lt;&gt; 1 OR IS NULL)</c>; NULL rows KEPT. In C#
///     <c>null != true</c> is <c>true</c>, so those rows must appear — EF Core's
///     default relational-null compensation emits the same <c>OR IS NULL</c>. A bare
///     <c>&lt;&gt; 1</c> is UNKNOWN for NULL and would SILENTLY DROP the rows (data loss).
///   </item>
/// </list>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNullableBoolComparisonTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NbcRow (Id INTEGER PRIMARY KEY, IsActive INTEGER NULL);
            INSERT INTO NbcRow VALUES
              (1, 1),    -- true
              (2, 0),    -- false
              (3, NULL), -- unset
              (4, 1),    -- true
              (5, 0),    -- false
              (6, NULL); -- unset
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
    public async Task Where_nullable_bool_equals_true_returns_only_true_rows_excluding_nulls()
    {
        // == true -> Ids {1, 4} only. NULL rows (3, 6) excluded by SQL
        // three-valued logic. False rows (2, 5) excluded trivially.
        var ids = (await _ctx.Query<NbcRow>()
            .Where(p => p.IsActive == true)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 4 }, ids);
    }

    [Fact]
    public async Task Where_nullable_bool_not_equals_true_keeps_null_rows_matching_csharp_and_ef()
    {
        // != true -> Ids {2, 3, 5, 6}: false rows {2,5} AND null rows {3,6}. In C#
        // `null != true` is true, so NULL rows are KEPT; nORM emits (<> 1 OR IS NULL),
        // matching LINQ-to-Objects and EF Core's default null compensation. A bare
        // `<> 1` would silently drop rows 3 and 6 (data loss).
        var ids = (await _ctx.Query<NbcRow>()
            .Where(p => p.IsActive != true)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 3, 5, 6 }, ids);
    }

    [Fact]
    public async Task Where_nullable_bool_explicit_or_isnull_matches_bare_not_equal()
    {
        // The explicit `|| == null` is now equivalent to a bare `!= true` (nORM adds the
        // IS NULL rescue automatically); pin that both forms return the same rows.
        var ids = (await _ctx.Query<NbcRow>()
            .Where(p => p.IsActive != true || p.IsActive == null)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 3, 5, 6 }, ids);
    }

    [Table("NbcRow")]
    public sealed class NbcRow
    {
        [Key] public int Id { get; set; }
        public bool? IsActive { get; set; }
    }
}
