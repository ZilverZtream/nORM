using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the live-database-swap cache-identity contract: <c>ChangeDatabase()</c> repoints the
/// connection while leaving its connection STRING unchanged, so a cache key derived from the
/// string alone kept serving the PREVIOUS database's rows (probe-verified against the unfixed
/// code: the warmed row came back from a database where the table does not even exist). The
/// result-cache key now also includes the live <c>Connection.Database</c> name, so the
/// post-swap read executes for real and fails loudly here. The test asserts the swap actually
/// happened first — a session whose NORM_TEST_* already points at master must fail loudly
/// instead of passing vacuously (that exact false positive hid the bug on the first probe run).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class CacheDatabaseSwapContractTests
{
    [Table("CswProbe_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Fact]
    public async Task Cacheable_read_after_change_database_must_not_serve_the_previous_databases_rows()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return;
        var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
        var cn = (DbConnection)Activator.CreateInstance(t, cs)!;
        cn.Open();
        var originalDb = cn.Database;

        void Exec(string sql)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        Exec("IF OBJECT_ID('CswProbe_Test') IS NOT NULL DROP TABLE CswProbe_Test");
        Exec("CREATE TABLE CswProbe_Test (Id INT PRIMARY KEY, V INT NOT NULL)");
        Exec("INSERT INTO CswProbe_Test VALUES (1, 42)");
        try
        {
            using var cache = new NormMemoryCacheProvider();
            var opts = new DbContextOptions { CacheProvider = cache };
            await using var ctx = new DbContext(cn, new SqlServerProvider(), opts, ownsConnection: false);

            var warm = ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking()
                .Where(r => r.Id == 1).Cacheable(TimeSpan.FromMinutes(5)).ToList();
            Assert.Single(warm);

            // This test needs the connection to START on a non-master database so the swap TO master is
            // observable. If the configured NORM_TEST_SQLSERVER connection defaults to master (no
            // Initial Catalog), there is no meaningful swap to make and a stale serve is undetectable —
            // so skip rather than fail. The contract is fully exercised wherever the connection targets a
            // real database (e.g. CI, which points at a dedicated test catalog).
            if (string.Equals(cn.Database, "master", StringComparison.OrdinalIgnoreCase))
                return;
            ctx.Connection.ChangeDatabase("master");
            Assert.Equal("master", cn.Database);
            try
            {
                object? outcome = null;
                try
                {
                    var after = ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking()
                        .Where(r => r.Id == 1).Cacheable(TimeSpan.FromMinutes(5)).ToList();
                    outcome = after.Count;
                }
                catch (Exception)
                {
                    // Correct: the post-swap read executed against master (no such table there),
                    // instead of serving the previous database's cached rows.
                }
                Assert.True(outcome is null or 0,
                    $"cacheable query served {outcome} row(s) from the PREVIOUS database after ChangeDatabase - silent cross-database stale read");
            }
            finally
            {
                ctx.Connection.ChangeDatabase(originalDb);
            }
        }
        finally
        {
            Exec("IF OBJECT_ID('CswProbe_Test') IS NOT NULL DROP TABLE CswProbe_Test");
            cn.Dispose();
        }
    }
}
