using System;
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

public class SaveChangesInterceptorTests
{
    public class User
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime? ModifiedAt { get; set; }
    }

    private class AuditInterceptor : ISaveChangesInterceptor
    {
        public int SavingCalls { get; private set; }
        public int SavedCalls { get; private set; }
        public int LastResult { get; private set; }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken)
        {
            SavingCalls++;
            foreach (var entry in entries)
            {
                if (entry.Entity is User u)
                {
                    if (entry.State == EntityState.Added)
                        u.CreatedAt = DateTime.UtcNow;
                    if (entry.State == EntityState.Modified)
                        u.ModifiedAt = DateTime.UtcNow;
                }
            }
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken)
        {
            SavedCalls++;
            LastResult = result;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Interceptor_runs_before_and_after_saving()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, CreatedAt TEXT, ModifiedAt TEXT);";
            cmd.ExecuteNonQuery();
        }

        var interceptor = new AuditInterceptor();
        var options = new DbContextOptions();
        options.SaveChangesInterceptors.Add(interceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var user = new User { Name = "Alice" };
        ctx.Add(user);
        var result = await ctx.SaveChangesAsync();

        Assert.Equal(1, result);
        Assert.NotEqual(default, user.CreatedAt);
        Assert.Equal(1, interceptor.SavingCalls);
        Assert.Equal(1, interceptor.SavedCalls);
        Assert.Equal(1, interceptor.LastResult);

        user.Name = "Bob";
        var entry = ctx.ChangeTracker.Entries.Single();
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
        await ctx.SaveChangesAsync();
        Assert.NotNull(user.ModifiedAt);
        Assert.Equal(2, interceptor.SavingCalls);
        Assert.Equal(2, interceptor.SavedCalls);
    }
}
