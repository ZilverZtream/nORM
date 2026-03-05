using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SG-1: Verifies that UseAffectedRowsSemantics is correct per provider, and that
/// concurrency checks are skipped for providers reporting affected (not matched) rows.
/// </summary>
public class ConcurrencyRowcountSemanticsTests
{
    // ─── Provider property assertions ─────────────────────────────────────

    [Fact]
    public void MySqlProvider_UseAffectedRowsSemantics_IsTrue()
    {
        // SG-1: MySQL returns affected rows, not matched rows — must skip the count check.
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.True(provider.UseAffectedRowsSemantics);
    }

    [Fact]
    public void SqliteProvider_UseAffectedRowsSemantics_IsFalse()
    {
        // SG-1: SQLite returns matched rows — concurrency check is valid.
        var provider = new SqliteProvider();
        Assert.False(provider.UseAffectedRowsSemantics);
    }

    [Fact]
    public void SqlServerProvider_UseAffectedRowsSemantics_IsFalse()
    {
        // SG-1: SQL Server returns matched rows — concurrency check is valid.
        var provider = new SqlServerProvider();
        Assert.False(provider.UseAffectedRowsSemantics);
    }

    // ─── Concurrency detection still fires for SQLite ─────────────────────

    [Table("SgConcUser")]
    private class SgConcUser
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [Timestamp]
        public byte[] RowVersion { get; set; } = System.Array.Empty<byte>();
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateSqliteContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SgConcUser (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, RowVersion BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task SqliteProvider_ConcurrencyConflict_ThrowsDbConcurrencyException()
    {
        // SG-1 non-regression: concurrency detection still fires for SQLite (matched-row semantics).
        var (cn, ctx) = CreateSqliteContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var user = new SgConcUser { Name = "Alice", RowVersion = new byte[] { 1 } };
        ctx.Add(user);
        await ctx.SaveChangesAsync();

        // Simulate external update that changes the RowVersion
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE SgConcUser SET RowVersion = @rv WHERE Id = 1";
        cmd.Parameters.AddWithValue("@rv", new byte[] { 99 });
        cmd.ExecuteNonQuery();

        // Mark the entity as modified
        user.Name = "Bob";
        var entry = ctx.ChangeTracker.Entries.Single();
        typeof(ChangeTracker)
            .GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .Invoke(ctx.ChangeTracker, new object[] { entry });

        // Must still throw because SQLite uses matched-row semantics
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task SqliteProvider_SameValueUpdate_StillDetectsConflict()
    {
        // SG-1 non-regression: SQLite matched-row semantics correctly reports 0 for stale RowVersion.
        var (cn, ctx) = CreateSqliteContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var user = new SgConcUser { Name = "Charlie", RowVersion = new byte[] { 5 } };
        ctx.Add(user);
        await ctx.SaveChangesAsync();

        // Simulate concurrent modification
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE SgConcUser SET RowVersion = @rv WHERE Id = 1";
        cmd.Parameters.AddWithValue("@rv", new byte[] { 55 });
        cmd.ExecuteNonQuery();

        // Update the entity — stale RowVersion means the UPDATE WHERE clause won't match.
        user.Name = "Charlie Updated";
        var entry = ctx.ChangeTracker.Entries.Single();
        typeof(ChangeTracker)
            .GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .Invoke(ctx.ChangeTracker, new object[] { entry });

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }
}
