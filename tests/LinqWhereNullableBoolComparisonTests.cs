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
/// Pins SQL three-valued-logic semantics for <c>Where</c> over a nullable
/// bool column. Two distinct results to lock in so future translator changes
/// don't silently flip the NULL handling:
/// <list type="bullet">
///   <item>
///     <c>== true</c> -> SQL <c>= 1</c>; NULL rows excluded (matches both
///     SQL and EF Core behavior — NULL = 1 is UNKNOWN). Safe.
///   </item>
///   <item>
///     <c>!= true</c> -> SQL <c>&lt;&gt; 1</c>; NULL rows ALSO excluded
///     (SQL: NULL &lt;&gt; 1 is UNKNOWN). This diverges from naive C#
///     intuition where <c>null != true</c> is <c>true</c>. Document and
///     pin the SQL behavior so the test catches any well-meaning
///     "fix" that adds <c>OR col IS NULL</c> on the side.
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
    public async Task Where_nullable_bool_not_equals_true_excludes_nulls_per_sql_three_valued_logic()
    {
        // != true -> Ids {2, 5} only -- NOT {2, 3, 5, 6}.
        // SQL evaluates NULL <> 1 as UNKNOWN which WHERE treats as false,
        // so NULL rows drop out. To get the "naive C#" set including NULLs,
        // the user must write `p.IsActive != true || p.IsActive == null`
        // (next test) -- this divergence is the silent-wrongness risk and
        // the assertion locks in the actual SQL behavior so future
        // translator changes can't quietly flip it.
        var ids = (await _ctx.Query<NbcRow>()
            .Where(p => p.IsActive != true)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 5 }, ids);
    }

    [Fact]
    public async Task Where_nullable_bool_explicit_or_isnull_includes_null_rows()
    {
        // To get "not true OR unset" semantics, callers must write the
        // null check explicitly. Pin this as the supported escape hatch.
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
