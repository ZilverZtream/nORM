using System;
using System.Globalization;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Temporal (system-versioned) writes under a caller-owned transaction. A committed in-transaction update
/// must create history so a prior-timestamp AsOf still reconstructs the old value; a ROLLED-BACK
/// in-transaction update must leave neither the live change nor a trigger-created history row, so AsOf never
/// serves a never-committed version; and an A -> B -> A update sequence across saves in one transaction must
/// land the final value (the repeated-edit baseline behaviour) while temporal versioning is active.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalUnderTransactionTests
{
    [Table("TmtRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static (SqliteConnection, DbContext) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TmtRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        opts.EnableTemporalVersioning();
        return (cn, new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false));
    }

    private static async Task<DateTime> ServerNowAsync(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    private static int LiveV(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT V FROM TmtRow WHERE Id = 1";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static async Task<int> AsOfVAsync(DbContext ctx, DateTime t) =>
        (await ((INormQueryable<Row>)ctx.Query<Row>()).AsOf(t).ToListAsync()).Single(r => r.Id == 1).V;

    [Fact]
    public async Task Committed_in_transaction_update_creates_history_reachable_by_as_of()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var row = new Row { Id = 1, V = 10 }; ctx.Add(row); await ctx.SaveChangesAsync();
        await Task.Delay(60); var t1 = await ServerNowAsync(cn); await Task.Delay(60);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            row.V = 99; ctx.Update(row); await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        }

        Assert.Equal(99, LiveV(cn));
        Assert.Equal(10, await AsOfVAsync(ctx, t1));
    }

    [Fact]
    public async Task Rolled_back_in_transaction_update_leaves_no_live_change_and_no_phantom_history()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var row = new Row { Id = 1, V = 10 }; ctx.Add(row); await ctx.SaveChangesAsync();
        await Task.Delay(60);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            row.V = 99; ctx.Update(row); await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        await Task.Delay(60); var t2 = await ServerNowAsync(cn);
        Assert.Equal(10, LiveV(cn));
        Assert.Equal(10, await AsOfVAsync(ctx, t2));   // never the phantom 99
    }

    [Fact]
    public async Task Repeated_update_back_to_original_in_transaction_persists_final_value_with_temporal()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var row = new Row { Id = 1, V = 10 }; ctx.Add(row); await ctx.SaveChangesAsync();

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            row.V = 99; ctx.Update(row); await ctx.SaveChangesAsync();
            row.V = 10; ctx.Update(row); await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        }

        Assert.Equal(10, LiveV(cn));
    }
}
