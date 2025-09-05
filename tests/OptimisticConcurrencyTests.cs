using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class OptimisticConcurrencyTests
{
    public class User
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [Timestamp]
        public byte[] RowVersion { get; set; } = System.Array.Empty<byte>();
    }

    [Fact]
    public async Task SaveChangesAsync_throws_on_update_concurrency_conflict()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, RowVersion BLOB NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var user = new User { Name = "Alice", RowVersion = new byte[] { 1 } };
        ctx.Add(user);
        await ctx.SaveChangesAsync();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE User SET RowVersion = @p0 WHERE Id = 1;";
            cmd.Parameters.AddWithValue("@p0", new byte[] { 2 });
            cmd.ExecuteNonQuery();
        }

        user.Name = "Bob";
        var entry = ctx.ChangeTracker.Entries.Single();
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task SaveChangesAsync_throws_on_delete_concurrency_conflict()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, RowVersion BLOB NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var user = new User { Name = "Alice", RowVersion = new byte[] { 1 } };
        ctx.Add(user);
        await ctx.SaveChangesAsync();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE User SET RowVersion = @p0 WHERE Id = 1;";
            cmd.Parameters.AddWithValue("@p0", new byte[] { 2 });
            cmd.ExecuteNonQuery();
        }

        ctx.Remove(user);
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }
}
