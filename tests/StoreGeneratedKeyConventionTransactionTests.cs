using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A store-generated convention key must be safe across the transactional write-path scenarios that
/// previously caused silent lost updates or duplicate inserts for other key strategies (see the write-path
/// bugfix ledger): a second <c>SaveChanges</c> inside one caller-owned transaction must not re-insert the
/// still-Added row, and modifying an already-inserted still-Added entity and re-saving must persist the
/// change rather than silently drop it. Both are protected by value-based flag/baseline machinery that is not
/// gated on <c>IsDbGenerated</c>, so a convention key (not <c>IsDbGenerated</c> on the column) inherits it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StoreGeneratedKeyConventionTransactionTests
{
    [Table("SgktRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }   // convention key
        public string Name { get; set; } = "";
        public int Amount { get; set; }
    }

    private static SqliteConnection Db()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SgktRow (Id INTEGER NOT NULL, Name TEXT NULL, Amount INTEGER NOT NULL, CONSTRAINT PK_SgktRow PRIMARY KEY (Id))";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Row>().HasKey(x => x.Id)
    }, ownsConnection: false);

    [Fact]
    public async Task SecondSaveInSameTransaction_DoesNotReInsert()
    {
        await using var cn = Db();
        await using var ctx = Ctx(cn);
        var a = new Row { Name = "a", Amount = 1 };
        ctx.Add(a);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.SaveChangesAsync();   // INSERT (a.Id store-generated, "inserted in tx" flag set)
        await ctx.SaveChangesAsync();   // second save in same tx must not re-insert the still-Added row
        await tx.CommitAsync();

        Assert.Equal(1, await ctx.Query<Row>().CountAsync());   // exactly one row, no duplicate
        Assert.True(a.Id > 0);
    }

    [Fact]
    public async Task ModifyAfterInsertInSameTransaction_UpdateIsPersisted_NotDropped()
    {
        await using var cn = Db();
        await using var ctx = Ctx(cn);
        var a = new Row { Name = "a", Amount = 1 };
        ctx.Add(a);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();   // INSERT Amount=1
            a.Amount = 99;                  // modify the already-inserted, still-Added entity
            await ctx.SaveChangesAsync();   // must persist the change, not silently drop it
            await tx.CommitAsync();
        }

        Assert.Equal(99, (await ctx.Query<Row>().SingleAsync()).Amount);
    }

    [Fact]
    public async Task SavepointPartialRollback_KeepsPreSavepointRow_ReInsertsPostSavepointRow()
    {
        await using var cn = Db();
        await using var ctx = Ctx(cn);
        var a = new Row { Name = "a" };
        var b = new Row { Name = "b" };

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(a);
            await ctx.SaveChangesAsync();            // A inserted (before the savepoint)
            await tx.CreateSavepointAsync("sp");

            ctx.Add(b);
            await ctx.SaveChangesAsync();            // B inserted (after the savepoint)
            await tx.RollbackToSavepointAsync("sp"); // undo B, keep A

            await ctx.SaveChangesAsync();            // B must re-insert; A must not duplicate
            await tx.CommitAsync();
        }

        var rows = await ctx.Query<Row>().OrderBy(r => r.Name).ToListAsync();
        Assert.Equal(new[] { "a", "b" }, rows.Select(r => r.Name).ToArray());   // both present, no duplicate
        Assert.True(a.Id > 0 && b.Id > 0 && a.Id != b.Id);
    }
}
