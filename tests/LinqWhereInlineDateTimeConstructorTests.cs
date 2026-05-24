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
/// Probe + strict pin: an inline <c>new DateTime(year, month, day, hour,
/// minute, second, ms, kind)</c> constructor on the RHS of a Where
/// comparison previously emitted broken SQL (Sqlite error 'near "@p2":
/// syntax error'). Hoisting the constructor to a local variable worked
/// fine, so the LINQ provider was failing to fold the NewExpression to
/// a constant before parameter binding.
///
/// The fix path is in ExpressionToSqlVisitor (or upstream constant-folder):
/// detect <c>NewExpression(DateTime/DateTimeOffset/etc.)</c> with all-constant
/// arguments and evaluate it at translation time so it binds as a single
/// parameter rather than emitting raw constructor SQL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereInlineDateTimeConstructorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WicItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, new DateTime(2026, 1, 15, 0, 0, 0, DateTimeKind.Utc)),
            (2, new DateTime(2026, 2, 20, 12, 30, 0, DateTimeKind.Utc)),
            (3, new DateTime(2026, 3, 1, 23, 59, 59, DateTimeKind.Utc)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WicItem (Id, Stamp) VALUES ($id, $s)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        foreach (var (id, stamp) in stamps)
        {
            pid.Value = id;
            ps.Value = stamp.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WicItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_inline_DateTime_constructor_seven_arg_kind_compares_correctly()
    {
        // Previously: SqliteException 'near "@p2": syntax error' because the
        // 7-arg DateTime constructor wasn't folded to a constant before
        // parameter binding -- the translator emitted unbound parameter
        // markers inline into the SQL.
        var result = await _ctx.Query<WicItem>()
            .Where(i => i.Stamp > new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_inline_DateTime_constructor_three_arg_compares_correctly()
    {
        // 3-arg overload (year, month, day) -- common shorthand for "just a
        // date" comparisons.
        var result = await _ctx.Query<WicItem>()
            .Where(i => i.Stamp < new DateTime(2026, 2, 1))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_inline_DateTime_constructor_six_arg_compares_correctly()
    {
        // 6-arg (year, month, day, hour, minute, second) -- the most-common
        // "give me a specific instant" form.
        var result = await _ctx.Query<WicItem>()
            .Where(i => i.Stamp >= new DateTime(2026, 2, 20, 12, 30, 0))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WicItem")]
    public sealed class WicItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
