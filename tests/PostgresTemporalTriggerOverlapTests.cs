using System.ComponentModel.DataAnnotations;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The Postgres temporal UPDATE/DELETE trigger closed the prior open history row only when no row
/// with __ValidFrom = v_now already existed. Because now() is transaction-stable, a second write to
/// the same key in one transaction skipped that close, leaving two rows open (overlapping validity —
/// an ambiguous "current" version). The guard is removed so open rows are always closed (a harmless
/// zero-width [v_now, v_now] intermediate, matching SQLite/MySQL, invisible to AsOf).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class PostgresTemporalTriggerOverlapTests
{
    private class PttRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public void Postgres_temporal_trigger_closes_open_rows_without_overlap_guard()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(PttRow));

        var pg = new PostgresProvider(new SqliteParameterFactory());
        var sql = pg.GenerateTemporalTriggersSql(mapping);

        // The overlap-causing guard must be gone...
        Assert.DoesNotContain("NOT EXISTS", sql);
        // ...but the trigger must still close open rows (ValidTo = the sentinel) before inserting the
        // new version, so exactly one row stays open per key.
        Assert.Contains("__ValidTo", sql);
        Assert.Contains("9999-12-31", sql);
    }
}
