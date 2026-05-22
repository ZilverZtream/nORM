using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class SyncPolicyTests
{
    private sealed class SyncUser
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void Sync_query_helpers_do_not_post_to_current_synchronization_context()
    {
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(new ThrowingSynchronizationContext());
        try
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE SyncUser(Id INTEGER, Name TEXT);" +
                                  "INSERT INTO SyncUser VALUES(1,'A');" +
                                  "INSERT INTO SyncUser VALUES(2,'B');";
                cmd.ExecuteNonQuery();
            }

            using var ctx = new DbContext(cn, new SqliteProvider());

            var users = ctx.Query<SyncUser>().Where(u => u.Id > 0).ToListSync();
            var count = ctx.Query<SyncUser>().CountSync();

            Assert.Equal(2, users.Count);
            Assert.Equal(2, count);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    [Fact]
    public void DbContext_has_no_public_synchronous_save_changes_api()
    {
        var syncSaveChanges = typeof(DbContext)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m.Name == "SaveChanges" && !m.Name.EndsWith("Async", StringComparison.Ordinal))
            .ToArray();

        Assert.Empty(syncSaveChanges);
    }

    private sealed class ThrowingSynchronizationContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object? state)
            => throw new InvalidOperationException("Sync query helper posted to the current SynchronizationContext.");

        public override void Send(SendOrPostCallback d, object? state)
            => throw new InvalidOperationException("Sync query helper sent work to the current SynchronizationContext.");
    }
}
