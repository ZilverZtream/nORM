using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//+ Verifies that BulkInsertAsync respects ambient transactions (ownedTx guard),
//and that UseAffectedRowsSemantics is correct for MySqlProvider.
//
//Note: The ownedTx integration tests use SQLite because MySqlConnector is unavailable
//in this environment. SQLite's BulkInsertAsync already uses the ownedTx guard pattern.
//The UseAffectedRowsSemantics assertion validates the override exists on MySqlProvider.
//</summary>
public class MysqlBulkTransactionIsolationTests
{
    [Table("MysqlBulkItem")]
    private class MysqlBulkItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE MysqlBulkItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static long CountRows(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM MysqlBulkItem";
        return System.Convert.ToInt64(cmd.ExecuteScalar());
    }

 // ─── UseAffectedRowsSemantics property ───────────────────

    [Fact]
    public void MySqlProvider_UseAffectedRowsSemantics_IsTrue()
    {
 // + MySqlProvider must declare affected-row semantics.
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.True(provider.UseAffectedRowsSemantics);
    }

 // ─── BulkInsertAsync without outer tx commits on success ───────

    [Fact]
    public async Task BulkInsertAsync_NoOuterTransaction_CommitsRows()
    {
 // non-regression: when no outer transaction is active, BulkInsertAsync
 // creates its own transaction (ownedTx=true) and commits it.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var items = new[]
        {
            new MysqlBulkItem { Id = 1, Name = "Alpha" },
            new MysqlBulkItem { Id = 2, Name = "Beta" },
        };

        await ctx.BulkInsertAsync(items);

        Assert.Equal(2L, CountRows(cn));
    }

 // ─── BulkInsertAsync inside outer tx respects rollback ─────────

    [Fact]
    public async Task BulkInsertAsync_WhenOuterTxRolledBack_RowsAbsent()
    {
 // BulkInsertAsync must participate in an outer transaction.
 // When the outer tx rolls back, inserted rows must disappear.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

 // Begin outer transaction via the context facade
        await using var outerTx = await ctx.Database.BeginTransactionAsync();

        var items = new[]
        {
            new MysqlBulkItem { Id = 10, Name = "Gamma" },
            new MysqlBulkItem { Id = 11, Name = "Delta" },
        };

        await ctx.BulkInsertAsync(items);

 // Roll back the outer transaction — rows must not be visible
        await outerTx.RollbackAsync();

        Assert.Equal(0L, CountRows(cn));
    }

    [Fact]
    public async Task BulkInsertAsync_WhenOuterTxCommitted_RowsPresent()
    {
 // non-regression: BulkInsertAsync inside a committed outer tx keeps rows.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        await using var outerTx = await ctx.Database.BeginTransactionAsync();

        var items = new[]
        {
            new MysqlBulkItem { Id = 20, Name = "Epsilon" },
        };

        await ctx.BulkInsertAsync(items);
        await outerTx.CommitAsync();

        Assert.Equal(1L, CountRows(cn));
    }

    [Fact]
    public async Task BulkInsertAsync_MixedInSaveChanges_RolledBackTogether()
    {
 // BulkInsertAsync and SaveChangesAsync must share the same outer transaction.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        await using var outerTx = await ctx.Database.BeginTransactionAsync();

 // Insert via SaveChangesAsync
        ctx.Add(new MysqlBulkItem { Id = 30, Name = "Zeta" });
        await ctx.SaveChangesAsync();

 // Insert via BulkInsertAsync
        await ctx.BulkInsertAsync(new[] { new MysqlBulkItem { Id = 31, Name = "Eta" } });

 // Roll back everything
        await outerTx.RollbackAsync();

        Assert.Equal(0L, CountRows(cn));
    }
}
