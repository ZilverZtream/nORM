using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Fast path must forward CancellationToken to all DB calls.
/// </summary>
public class FastPathCancellationTests
{
    public class Widget
    {
        [Key]
        public int Id { get; set; }
        public bool IsEnabled { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static DbContext CreateAndSeed(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE \"Widget\"(Id INTEGER PRIMARY KEY, IsEnabled INTEGER, Name TEXT);" +
            "INSERT INTO \"Widget\" VALUES(1,1,'A');" +
            "INSERT INTO \"Widget\" VALUES(2,0,'B');";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task FastPath_Where_CancelledToken_ThrowsOperationCancelled()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => ctx.Query<Widget>().Where(w => w.IsEnabled).ToListAsync(cts.Token));
        }
    }

    [Fact]
    public async Task FastPath_Take_CancelledToken_ThrowsOperationCancelled()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => ctx.Query<Widget>().Take(1).ToListAsync(cts.Token));
        }
    }

    [Fact]
    public async Task FastPath_Count_CancelledToken_ThrowsOperationCancelled()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => ctx.Query<Widget>().CountAsync(cts.Token));
        }
    }

    [Fact]
    public async Task FastPath_Where_ValidToken_CompletesSuccessfully()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            using var cts = new CancellationTokenSource();

            var results = await ctx.Query<Widget>().Where(w => w.IsEnabled).ToListAsync(cts.Token);
            Assert.Single(results);
            Assert.True(results[0].IsEnabled);
        }
    }

    [Fact]
    public async Task FastPath_Take_ValidToken_CompletesSuccessfully()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            using var cts = new CancellationTokenSource();

            var results = await ctx.Query<Widget>().Take(1).ToListAsync(cts.Token);
            Assert.Single(results);
        }
    }
}
