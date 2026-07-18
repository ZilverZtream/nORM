using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the synchronous <c>Database.BeginTransaction()</c> and <c>Database.GetDbConnection()</c> EF-parity
/// facades: a sync transaction commits/rolls back the unit of work, a second begin fails loud while one is
/// active, and GetDbConnection hands back the very connection the context uses (so raw ADO/Dapper interop
/// shares the same connection and active transaction). GetDbConnection is disallowed under strict provider
/// mobility.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DatabaseTransactionAndConnectionContractTests
{
    [Table("DtcWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:dtc_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DtcWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO DtcWidget VALUES (1, 'a');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static int RowCount(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM DtcWidget";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task BeginTransaction_commit_persists_the_unit_of_work()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        using (var tx = ctx.Database.BeginTransaction())
        {
            ctx.Add(new Widget { Id = 2, Name = "b" });
            await ctx.SaveChangesAsync();
            tx.Commit();
        }

        Assert.Equal(2, RowCount(keeper));   // committed, visible to an independent reader
    }

    [Fact]
    public async Task BeginTransaction_rollback_discards_the_unit_of_work()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        using (var tx = ctx.Database.BeginTransaction())
        {
            ctx.Add(new Widget { Id = 3, Name = "c" });
            await ctx.SaveChangesAsync();
            tx.Rollback();
        }

        Assert.Equal(1, RowCount(keeper));   // rolled back, only the seed row remains
    }

    [Fact]
    public void BeginTransaction_throws_when_one_is_already_active()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        using var ctx = make();

        using var tx = ctx.Database.BeginTransaction();
        Assert.Throws<NormUsageException>(() => ctx.Database.BeginTransaction());
    }

    [Fact]
    public void GetDbConnection_returns_the_context_connection_and_shares_its_transaction()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        using var ctx = make();

        DbConnection cn = ctx.Database.GetDbConnection();
        Assert.NotNull(cn);

        // Raw ADO on the context connection, enlisted in the context's active transaction, then rolled
        // back — the classic "drop to Dapper on the same connection" interop path.
        using (var tx = ctx.Database.BeginTransaction())
        {
            using (var raw = cn.CreateCommand())
            {
                raw.Transaction = ctx.Database.CurrentTransaction;
                raw.CommandText = "INSERT INTO DtcWidget VALUES (4, 'd')";
                raw.ExecuteNonQuery();
            }
            tx.Rollback();
        }

        Assert.Equal(1, RowCount(keeper));   // the raw insert shared the rolled-back transaction
    }

    [Fact]
    public void GetDbConnection_is_blocked_under_strict_provider_mobility()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var opts = new DbContextOptions().UseStrictProviderMobility();
        opts.OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Database.GetDbConnection());
    }
}
