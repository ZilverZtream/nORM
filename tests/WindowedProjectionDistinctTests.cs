using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Distinct over a Take/Skip-windowed projection. The windowed wrap must
/// re-install the shape-defining Select as the plan projection (or the
/// materializer binds entity columns to the projected constructor and
/// throws), and on case-insensitive-collation providers the wrap must dedup
/// byte-wise like C# Distinct() instead of folding case.
/// </summary>
[System.ComponentModel.DataAnnotations.Schema.Table("WinDis_Test")]
public class WinDisRow
{
    [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
    public string? Nick { get; set; }
    public int V { get; set; }
}

[Trait("Category", TestCategory.Fast)]
public class WindowedProjectionDistinctTests
{
    [Fact]
    public async Task Windowed_anonymous_distinct_materializes_and_dedups()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WinDis_Test (Id INTEGER PRIMARY KEY, Nick TEXT NULL, V INTEGER NOT NULL);
                INSERT INTO WinDis_Test VALUES (1,'alpha',1),(2,'ALPHA',1),(3,'alpha',1),(4,NULL,2),(5,'beta',2)
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = (await ctx.Query<WinDisRow>().OrderBy(r => r.Id).Take(5)
                .Select(r => new { r.Nick, r.V })
                .Distinct()
                .ToListAsync())
            .OrderBy(x => x.Nick, StringComparer.Ordinal).ThenBy(x => x.V).ToList();

        // C# ordinal distinct: (null,2), (ALPHA,1), (alpha,1), (beta,2).
        Assert.Equal(4, rows.Count);
        Assert.Equal(new[] { (string?)null, "ALPHA", "alpha", "beta" }, rows.Select(x => x.Nick).ToArray());
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class WindowedProjectionDistinctLiveTests
{
    [Fact]
    public async Task Windowed_anonymous_distinct_keeps_case_variants_on_ci_collation()
    {
        var cs = Environment.GetEnvironmentVariable("NORM_TEST_MYSQL");
        if (string.IsNullOrEmpty(cs)) return;

        Func<DbConnection> factory = () =>
        {
            var cn = new MySqlConnector.MySqlConnection(cs);
            cn.Open();
            return cn;
        };

        void Exec(string sql)
        {
            using var cn = factory();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        Exec("DROP TABLE IF EXISTS WinDis_Test");
        Exec("CREATE TABLE WinDis_Test (Id INT PRIMARY KEY, Nick VARCHAR(32) NULL, V INT NOT NULL)");
        Exec("INSERT INTO WinDis_Test VALUES (1,'alpha',1),(2,'ALPHA',1),(3,'alpha',1),(4,NULL,2),(5,'beta',2)");
        try
        {
            await using var ctx = new DbContext(factory(), new MySqlProvider());

            var rows = (await ctx.Query<WinDisRow>().OrderBy(r => r.Id).Take(5)
                    .Select(r => new { r.Nick, r.V })
                    .Distinct()
                    .ToListAsync())
                .OrderBy(x => x.Nick, StringComparer.Ordinal).ThenBy(x => x.V).ToList();
            Assert.Equal(4, rows.Count);
            Assert.Equal(new[] { (string?)null, "ALPHA", "alpha", "beta" }, rows.Select(x => x.Nick).ToArray());

            // Renamed member: the sub-select aliases the output by the projection
            // member name, and the wrap's GROUP BY must reference that alias.
            var renamed = (await ctx.Query<WinDisRow>().OrderBy(r => r.Id).Take(5)
                    .Select(r => new { N = r.Nick, r.V })
                    .Distinct()
                    .ToListAsync())
                .OrderBy(x => x.N, StringComparer.Ordinal).ThenBy(x => x.V).ToList();
            Assert.Equal(4, renamed.Count);
            Assert.Equal(new[] { (string?)null, "ALPHA", "alpha", "beta" }, renamed.Select(x => x.N).ToArray());
        }
        finally
        {
            Exec("DROP TABLE IF EXISTS WinDis_Test");
        }
    }
}
