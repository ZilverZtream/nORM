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
/// Ninth in the post-Take/Skip silent-wrongness probe family — set operations
/// (Union / Concat / Intersect / Except) over a windowed left source.
/// <c>q.Take(2).Union(other)</c> must union the windowed 2 rows with `other`.
/// SQL emit risk: with each side translated in an isolated sub-context, the
/// LIMIT on the left should bake into the sub-SQL — but the outer composition
/// is `&lt;left&gt; UNION &lt;right&gt;` and SQLite parses LIMIT after the
/// last SELECT, which can attach the LIMIT to the union instead of the left
/// side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSetOpsAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SoaLeft  (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            CREATE TABLE SoaRight (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO SoaLeft  VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D');
            INSERT INTO SoaRight VALUES (1,'X'),(2,'Y');
            -- Left.Take(2) = {A, B}. Union with Right = {A, B, X, Y}.
            -- If LIMIT attaches to UNION instead of left: emits something like
            -- "(SELECT Code FROM L) UNION (SELECT Code FROM R) LIMIT 2" → {A, B} only.
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
    public async Task Union_after_take_unions_windowed_left_with_full_right()
    {
        // Left.OrderBy(Id).Take(2) → {A, B}. Right is the full {X, Y}.
        // Union semantics: {A, B, X, Y}.
        var result = (await _ctx.Query<SoaLeft>()
            .OrderBy(l => l.Id)
            .Take(2)
            .Select(l => l.Code)
            .Union(_ctx.Query<SoaRight>().Select(r => r.Code))
            .ToListAsync()).OrderBy(c => c).ToArray();
        Assert.Equal(new[] { "A", "B", "X", "Y" }, result);
    }

    [Table("SoaLeft")]
    public sealed class SoaLeft
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    [Table("SoaRight")]
    public sealed class SoaRight
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
