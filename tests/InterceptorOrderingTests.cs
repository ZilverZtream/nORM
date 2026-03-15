using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that <c>SavedChangesAsync</c> fires AFTER the transaction has been committed,
/// meaning the interceptor can observe the inserted row in a separate connection.
/// </summary>
public class InterceptorOrderingTests
{
    [Table("InterceptorOrderPost")]
    private class Post
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    private class VisibilityCheckInterceptor : ISaveChangesInterceptor
    {
        private readonly string _connectionString;
        public bool RowWasVisibleInSavedChanges { get; private set; }

        public VisibilityCheckInterceptor(string connectionString)
            => _connectionString = connectionString;

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public async Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken)
        {
            // Open a NEW connection to verify the row is already committed (visible).
            using var cn2 = new SqliteConnection(_connectionString);
            cn2.Open();
            using var cmd = cn2.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM InterceptorOrderPost";
            var count = (long)(await cmd.ExecuteScalarAsync(cancellationToken))!;
            RowWasVisibleInSavedChanges = count > 0;
        }
    }

    [Fact]
    public async Task SavedChangesAsync_FiresAfterCommit_RowIsVisible()
    {
        // Use a file-backed SQLite DB so a second connection can see the committed data.
        var dbPath = System.IO.Path.GetTempFileName();
        var connStr = $"Data Source={dbPath}";
        bool rowVisible = false;
        try
        {
            // Set up schema
            {
                using var setupCn = new SqliteConnection(connStr);
                setupCn.Open();
                using var setupCmd = setupCn.CreateCommand();
                setupCmd.CommandText = "CREATE TABLE InterceptorOrderPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL)";
                setupCmd.ExecuteNonQuery();
            }
            SqliteConnection.ClearAllPools(); // release setup connection from pool

            var interceptor = new VisibilityCheckInterceptor(connStr);
            var options = new DbContextOptions();
            options.SaveChangesInterceptors.Add(interceptor);

            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = new DbContext(cn, new SqliteProvider(), options);

                var post = new Post { Title = "Committed" };
                ctx.Add(post);
                await ctx.SaveChangesAsync();

                rowVisible = interceptor.RowWasVisibleInSavedChanges;
            }
            SqliteConnection.ClearAllPools(); // release all connections before file deletion

            Assert.True(rowVisible,
                "SavedChangesAsync should fire after commit so the row is visible to other connections.");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            try { System.IO.File.Delete(dbPath); } catch { /* best-effort cleanup */ }
        }
    }
}
