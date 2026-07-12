using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Bulk delete of entities carrying a concurrency token must include the token
/// in its WHERE clause so a row another writer has updated (token bumped) is NOT
/// deleted — matching the existing bulk-update contract (skip stale rows, return
/// the reduced affected count; no throw). Previously bulk delete matched by
/// primary key only and silently destroyed the newer row.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkOccEnforcementTests
{
    [Table("BoccRow")]
    private class BoccRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    /// <summary>
    /// Forces the shared provider-agnostic BatchedDeleteAsync fallback (which the
    /// SqlServer/Postgres/MySql providers use under UseBatchedBulkOps) to run on an
    /// in-memory SQLite connection, so its OCC behavior is unit-testable without a
    /// live provider.
    /// </summary>
    private sealed class BatchedFallbackSqliteProvider : SqliteProvider
    {
        public override Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, System.Collections.Generic.IEnumerable<T> entities, System.Threading.CancellationToken ct)
            => BatchedDeleteAsync(ctx, m, entities, ct);
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateBatchedContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BoccRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL);" +
                "INSERT INTO BoccRow (Id, Payload, Token) VALUES (1,'a',X'00000001'),(2,'b',X'00000001');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<BoccRow>() };
        return (cn, new DbContext(cn, new BatchedFallbackSqliteProvider(), opts));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BoccRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL);" +
                "INSERT INTO BoccRow (Id, Payload, Token) VALUES (1,'a',X'00000001'),(2,'b',X'00000001');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<BoccRow>() };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static int RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM BoccRow";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Bulk_delete_of_many_token_rows_does_not_exceed_the_parameter_limit()
    {
        // Each OCC-token row binds 2 parameters (key + token). Well beyond SQLite's
        // 999-variable limit at one param/row, this many rows would overflow if the
        // batch size ignored the per-row parameter cost.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BoccRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL, Token BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
            for (var i = 1; i <= 900; i++)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO BoccRow (Id, Payload, Token) VALUES ({i}, 'p{i}', X'00000001')";
                ins.ExecuteNonQuery();
            }
        }
        using var _cn = cn;
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<BoccRow>() };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var rows = Enumerable.Range(1, 900)
            .Select(i => new BoccRow { Id = i, Payload = $"p{i}", Token = new byte[] { 0, 0, 0, 1 } })
            .ToArray();

        var deleted = await ctx.BulkDeleteAsync(rows);

        Assert.Equal(900, deleted);
        Assert.Equal(0, RowCount(cn));
    }

    [Fact]
    public async Task Bulk_delete_with_stale_token_skips_the_row_and_deletes_nothing()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Detached entity carrying a STALE token (DB row 1 holds X'00000001').
        var stale = new BoccRow { Id = 1, Payload = "a", Token = new byte[] { 0, 0, 0, 2 } };

        var deleted = await ctx.BulkDeleteAsync(new[] { stale });

        // Token mismatch → row skipped, reduced count returned, nothing destroyed.
        Assert.Equal(0, deleted);
        Assert.Equal(2, RowCount(cn));
    }

    [Fact]
    public async Task Bulk_delete_with_matching_token_succeeds()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var current = new BoccRow { Id = 1, Payload = "a", Token = new byte[] { 0, 0, 0, 1 } };

        var deleted = await ctx.BulkDeleteAsync(new[] { current });

        Assert.Equal(1, deleted);
        Assert.Equal(1, RowCount(cn));
    }

    [Fact]
    public async Task Bulk_delete_mixed_tokens_removes_only_the_matching_row()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var good = new BoccRow { Id = 1, Payload = "a", Token = new byte[] { 0, 0, 0, 1 } };
        var stale = new BoccRow { Id = 2, Payload = "b", Token = new byte[] { 0, 0, 0, 9 } };

        var deleted = await ctx.BulkDeleteAsync(new[] { good, stale });

        // Only row 1 (matching token) removed; row 2 (stale) survives.
        Assert.Equal(1, deleted);
        Assert.Equal(1, RowCount(cn));
    }

    // ── Shared BatchedDeleteAsync fallback (SqlServer/Postgres/MySql under UseBatchedBulkOps) ──

    [Fact]
    public async Task Batched_fallback_bulk_delete_with_stale_token_skips_the_row()
    {
        var (cn, ctx) = CreateBatchedContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var stale = new BoccRow { Id = 1, Payload = "a", Token = new byte[] { 0, 0, 0, 2 } };
        var deleted = await ctx.BulkDeleteAsync(new[] { stale });

        Assert.Equal(0, deleted);
        Assert.Equal(2, RowCount(cn));
    }

    [Fact]
    public async Task Batched_fallback_bulk_delete_mixed_tokens_removes_only_matching()
    {
        var (cn, ctx) = CreateBatchedContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var good = new BoccRow { Id = 1, Payload = "a", Token = new byte[] { 0, 0, 0, 1 } };
        var stale = new BoccRow { Id = 2, Payload = "b", Token = new byte[] { 0, 0, 0, 9 } };
        var deleted = await ctx.BulkDeleteAsync(new[] { good, stale });

        Assert.Equal(1, deleted);
        Assert.Equal(1, RowCount(cn));
    }
}
