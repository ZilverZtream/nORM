using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probes <c>DateTime.AddDays(N)</c> applied to a column in a Where
/// predicate. Common idioms:
///   * <c>q.Where(r =&gt; r.Stamp.AddDays(7) &lt; DateTime.UtcNow)</c>
///     -- "rows whose stamp +7 days is now in the past" (i.e. stamp older
///     than a week, expressed via the natural "expires in 7 days" framing)
///   * <c>q.Where(r =&gt; r.Stamp &gt; DateTime.UtcNow.AddDays(-7))</c>
///     -- the equivalent but with the AddDays on the constant side
///
/// nORM memory item #75 fixed a SqliteProvider bug where negative deltas
/// produced an invalid '+-3 days' SQLite modifier; the fix was in the
/// projection / scalar code path. This file pins the Where-predicate
/// path: the SQL must apply the column-relative arithmetic correctly
/// (whether via <c>strftime/datetime(col, '+N days')</c> or by
/// re-anchoring the threshold to the client-side equivalent).
///
/// Silent-wrongness shapes:
///   * Translator drops AddDays silently -> comparison reduces to
///     `col &lt; now`, includes much more than intended.
///   * Translator emits a constant 0 days offset -> behaviorally the same
///     as dropping AddDays.
///   * SQLite-specific bug from memory #75 surfacing here too -> "no rows"
///     or runtime error from malformed +-N days modifier.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeAddDaysArithmeticTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdaItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var now = DateTime.UtcNow;
        await using (var insert = _cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO WdaItem (Id, Stamp) VALUES ($id, $stamp)";
            var idParam = insert.CreateParameter(); idParam.ParameterName = "$id"; insert.Parameters.Add(idParam);
            var stampParam = insert.CreateParameter(); stampParam.ParameterName = "$stamp"; insert.Parameters.Add(stampParam);
            // 5 rows around -30d/-10d/-3d/now/+3d so we can cleanly test
            // "older than 7 days" predicates.
            var stamps = new (int id, DateTime stamp)[]
            {
                (1, now.AddDays(-30)),
                (2, now.AddDays(-10)),
                (3, now.AddDays(-3)),
                (4, now),
                (5, now.AddDays(3)),
            };
            foreach (var (id, stamp) in stamps)
            {
                idParam.Value = id;
                stampParam.Value = stamp.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
                await insert.ExecuteNonQueryAsync();
            }
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_AddDays_on_constant_side_returns_recent_rows_strictly()
    {
        // Constant-side AddDays evaluates client-side at translate time
        // (it's just a DateTime constant by the time the lambda is walked).
        // Strict: rows with Stamp > UtcNow.AddDays(-7) -> Ids {3, 4, 5}.
        var cutoff = DateTime.UtcNow.AddDays(-7);
        var result = await _ctx.Query<WdaItem>()
            .Where(r => r.Stamp > cutoff)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_DateTime_UtcNow_AddDays_inline_returns_recent_rows_strictly()
    {
        // Inline DateTime.UtcNow.AddDays(-7). The translator must fold both
        // .UtcNow and .AddDays into a single client-side parameter.
        var result = await _ctx.Query<WdaItem>()
            .Where(r => r.Stamp > DateTime.UtcNow.AddDays(-7))
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_AddDays_on_column_side_returns_older_rows_strictly()
    {
        // Originally pinned as throw-or-correct; verified to translate via the
        // provider's datetime(col, '+N days') template (see memory item #75 and
        // SqliteProvider TranslateFunction). r.Stamp.AddDays(7) < UtcNow ->
        // rows where Stamp + 7d is in the past -> Stamp older than a week ago.
        // Expected: Id 1 (-30d), Id 2 (-10d).
        var result = await _ctx.Query<WdaItem>()
            .Where(r => r.Stamp.AddDays(7) < DateTime.UtcNow)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WdaItem")]
    public sealed class WdaItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
