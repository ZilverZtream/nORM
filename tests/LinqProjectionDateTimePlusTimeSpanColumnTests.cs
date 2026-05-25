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
/// Probes <c>DateTime + TimeSpan</c> shift where the TimeSpan is a column
/// (not a closure-captured constant). 407e03d handled the constant case;
/// the column case needs the SQL engine to do the arithmetic at runtime.
/// SQLite has datetime(col, modifier) where modifier can be 'N seconds'
/// constructed from the integer total-seconds of the column.
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval / throw.
///   * SQL '+' on TEXT coerces to 0 -> returns the bound TimeSpan text or
///     the DateTime text unchanged.
///   * Wrong scale (e.g. minutes instead of seconds).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimePlusTimeSpanColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtcItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL, Duration TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        var rows = new (int id, DateTime stamp, TimeSpan dur)[]
        {
            (1, anchor, TimeSpan.FromHours(1)),
            (2, anchor.AddDays(1), TimeSpan.FromMinutes(30)),
            (3, anchor.AddDays(2), TimeSpan.FromMinutes(15)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PdtcItem (Id, Stamp, Duration) VALUES ($id, $s, $d)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        var pd = insert.CreateParameter(); pd.ParameterName = "$d"; insert.Parameters.Add(pd);
        foreach (var (id, st, du) in rows)
        {
            pid.Value = id;
            ps.Value = st.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            pd.Value = du.ToString("c");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_plus_TimeSpan_column_projects_shifted_DateTime_per_row()
    {
        var result = await _ctx.Query<PdtcItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.Stamp + p.Duration })
            .ToListAsync();
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        Assert.Equal(3, result.Count);
        Assert.Equal(anchor.AddHours(1), result[0].S);
        Assert.Equal(anchor.AddDays(1).AddMinutes(30), result[1].S);
        Assert.Equal(anchor.AddDays(2).AddMinutes(15), result[2].S);
    }

    [Table("PdtcItem")]
    public sealed class PdtcItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
