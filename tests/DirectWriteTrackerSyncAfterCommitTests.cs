using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The direct-write path (Insert/Update/DeleteAsync under a retry policy) commits its owned transaction and
/// then syncs the change tracker. If that post-commit sync throws (a faulting entity getter during snapshot
/// capture), it must NOT roll back the already-committed transaction: the write is durable, so the failure
/// must surface as the original exception, not as an AggregateException from rolling back a completed
/// transaction. SaveChanges hoists its accept phase out of the try for exactly this reason; the direct-write
/// path must too.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DirectWriteTrackerSyncAfterCommitTests
{
    private static bool _armThrow;

    [Table("DwtAcct")]
    public class Acct
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Balance { get; set; }
        private List<Line> _lines = new();
        // A snapshot-only getter: read by AcceptChanges' owned-collection capture (post-commit) but not by
        // the scalar UPDATE. Armed to throw only AFTER the entity is loaded, so the write commits first.
        public List<Line> Lines
        {
            get { if (_armThrow) throw new InvalidOperationException("boom in Lines getter"); return _lines; }
            set => _lines = value;
        }
    }

    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DwtAcct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Balance INTEGER NOT NULL);
                CREATE TABLE DwtLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, AcctId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO DwtAcct (Id, Balance) VALUES (1, 100);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            // A retry policy routes direct writes through WriteWithTransactionAsync (the owned-transaction path).
            RetryPolicy = new RetryPolicy { MaxRetries = 2, BaseDelay = TimeSpan.FromMilliseconds(1), ShouldRetry = ex => ex is DbException },
            OnModelCreating = mb =>
            {
                mb.Entity<Acct>().HasKey(a => a.Id);
                mb.Entity<Acct>().OwnsMany<Line>(a => a.Lines, tableName: "DwtLine", foreignKey: "AcctId");
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Post_commit_tracker_sync_throw_does_not_roll_back_the_committed_write()
    {
        _armThrow = false;
        var (cn, ctx) = Create();
        using var _cn = cn;
        await using var _ctx = ctx;

        var acct = await ctx.Query<Acct>().FirstAsync(a => a.Id == 1); // tracked; Lines snapshot captured (empty)
        acct.Balance = 150;
        _armThrow = true; // make the post-commit AcceptChanges owned-collection snapshot throw

        // The UPDATE commits durably; the post-commit tracker sync throws. The caller must see the ORIGINAL
        // getter exception, NOT an AggregateException from rolling back the already-committed transaction.
        var ex = await Record.ExceptionAsync(() => ctx.UpdateAsync(acct));
        _armThrow = false;

        Assert.NotNull(ex);
        Assert.IsNotType<AggregateException>(ex);   // BUG: catch rolls back the committed tx -> AggregateException
        Assert.Contains("boom", ex!.Message, StringComparison.Ordinal);

        // The UPDATE must be durable regardless of the tracker-sync failure. Read the raw committed value.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Balance FROM DwtAcct WHERE Id = 1";
        Assert.Equal(150L, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}
