using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Enabling temporal versioning on a table that ALREADY CONTAINS rows must not make those rows invisible to
/// AsOf. The AsOf read reconstructs from the history table only, and the INSERT trigger captures a row only
/// when it is inserted after the trigger exists — so a row that pre-dated the bootstrap had no history row
/// and AsOf(now) silently returned nothing for it. The bootstrap must backfill existing live rows into
/// history as an open (current) version, matching SQL Server's SET SYSTEM_VERSIONING = ON.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalBootstrapBackfillTests
{
    [Table("TbbProduct")]
    public class Product
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static async Task<DateTime> ServerNow(SqliteConnection cn)
    {
        using var c = cn.CreateCommand();
        c.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f','now')";
        return DateTime.SpecifyKind(DateTime.Parse((string)(await c.ExecuteScalarAsync())!, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    [Fact]
    public async Task AsOf_sees_rows_that_existed_before_versioning_was_enabled()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;

        // Pre-existing data written OUT OF BAND, before any nORM temporal bootstrap.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TbbProduct (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "INSERT INTO TbbProduct VALUES (1, 'Pre'), (2, 'Existing');";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Product>().HasKey(p => p.Id) };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Force the temporal bootstrap (history table + triggers + backfill) to run.
        var live = await ctx.Query<Product>().OrderBy(p => p.Id).ToListAsync();
        Assert.Equal(2, live.Count);   // live query sees both

        var now = await ServerNow(cn);
        var asOf = (await ((INormQueryable<Product>)ctx.Query<Product>()).AsOf(now).OrderBy(p => p.Id).ToListAsync())
            .Select(p => p.Id).ToArray();

        // AsOf(now) must equal current live state — the pre-existing rows must not silently vanish.
        Assert.Equal(new[] { 1, 2 }, asOf);
    }
}
