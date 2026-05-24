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
/// Probes the ParameterValueExtractor mis-alignment shape (407e03d /
/// eeff6e7 / cf39b61 / 04a0003) for the 2d6b915 NewExpression
/// constant-fold path. When all ctor args are closure-captured locals,
/// the fold consumes each MemberExpression argument inline (computing
/// the resulting value at translation time and emitting it as one @p
/// inline-const param) -- but the extractor still walks each closure
/// MemberAccess and adds N values to its list, shifting subsequent
/// @cp bindings.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNewExpressionClosureArgsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WnecItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL, Tag TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var anchor = new DateTime(2026, 2, 20, 12, 0, 0, DateTimeKind.Utc);
        var rows = new (int id, DateTime stamp, string tag)[]
        {
            (1, anchor.AddDays(-30), "red"),
            (2, anchor.AddDays(1),   "red"),
            (3, anchor.AddDays(5),   "blue"),
            (4, anchor.AddDays(10),  "red"),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WnecItem (Id, Stamp, Tag) VALUES ($id, $s, $t)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        var pt = insert.CreateParameter(); pt.ParameterName = "$t"; insert.Parameters.Add(pt);
        foreach (var (id, st, tg) in rows)
        {
            pid.Value = id;
            ps.Value = st.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            pt.Value = tg;
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WnecItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_inline_DateTime_ctor_with_closure_args_AND_other_closure_filters_correctly()
    {
        // new DateTime(y, m, d) compares to anchor (2026-02-20). Rows after
        // that (Id 2, 3, 4) match Stamp > cutoff. tagWanted = "red" filters
        // to {2, 4}.
        var y = 2026;
        var m = 2;
        var d = 20;
        var tagWanted = "red";
        var result = await _ctx.Query<WnecItem>()
            .Where(p => p.Stamp > new DateTime(y, m, d) && p.Tag == tagWanted)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WnecItem")]
    public sealed class WnecItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
