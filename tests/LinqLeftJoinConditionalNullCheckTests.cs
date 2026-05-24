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
/// Pins the parked silent-wrongness bug from 5b6605e: in a query-syntax LEFT JOIN
/// (<c>GroupJoin + SelectMany + DefaultIfEmpty</c>), a projection that
/// defensively null-checks the inner entity — <c>c == null ? null : c.Tag</c>
/// — mis-binds the projected column to the inner's first column (Id) rather
/// than the named property (Tag). Idiomatic equivalent <c>c.Tag</c> alone
/// works (LEFT JOIN naturally NULLs unmatched columns, so the null-check is
/// semantically redundant) and is the recommended workaround until the
/// conditional path is fixed.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqLeftJoinConditionalNullCheckTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LjcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE LjcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO LjcParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            INSERT INTO LjcChild  VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1');
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
    public async Task Left_join_projection_with_inner_null_check_throws_with_workaround_hint()
    {
        // Until the conditional-on-inner-entity translation is fixed, this shape
        // must surface a clear NormUnsupportedFeatureException pointing at the
        // bare-`c.Tag` workaround — silent miscolumn binding is unacceptable.
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await (from p in _ctx.Query<LjcParent>()
                   join c in _ctx.Query<LjcChild>() on p.Id equals c.ParentId into g
                   from c in g.DefaultIfEmpty()
                   select new { Parent = p.Name, Tag = c == null ? null : c.Tag })
                  .ToListAsync();
        });
        Assert.Contains("LEFT JOIN", ex.Message, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("c.Tag", ex.Message, System.StringComparison.Ordinal);
    }

    [Table("LjcParent")]
    public sealed class LjcParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("LjcChild")]
    public sealed class LjcChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
