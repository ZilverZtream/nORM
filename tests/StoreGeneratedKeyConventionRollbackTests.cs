using System.Collections.Generic;
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
/// A store-generated convention key inserted inside a caller-owned transaction that is then rolled back
/// must be re-inserted on the next save, never silently dropped. This is subtle because the rollback's
/// key-value reset (<c>RestoreRolledBackGeneratedKeys</c>) is gated on <c>IsDbGenerated</c> and skips a
/// convention key (which is not <c>IsDbGenerated</c> on the column); re-insertion is instead protected by
/// the <c>InsertedInUncommittedTransaction</c> flag reset, which is not so gated. Guards that a rolled-back
/// convention-key insert is not lost.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StoreGeneratedKeyConventionRollbackTests
{
    [Table("SgkrRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }   // convention key: no [DatabaseGenerated]
        public string Name { get; set; } = "";
    }

    private static SqliteConnection Db()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SgkrRow (Id INTEGER NOT NULL, Name TEXT NULL, CONSTRAINT PK_SgkrRow PRIMARY KEY (Id))";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Row>().HasKey(x => x.Id)
    }, ownsConnection: false);

    [Fact]
    public async Task InsertRolledBackInTransaction_IsReInsertedOnNextSave_NotDropped()
    {
        await using var cn = Db();
        await using var ctx = Ctx(cn);

        var a = new Row { Name = "a" };
        ctx.Add(a);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();   // INSERT runs in the tx; a.Id is store-generated + read back
            await tx.RollbackAsync();       // the row is gone
        }

        await ctx.SaveChangesAsync();       // must re-insert, not skip as "already inserted"

        Assert.Equal(1, await ctx.Query<Row>().CountAsync());
        var row = await ctx.Query<Row>().SingleAsync(r => r.Name == "a");
        Assert.True(a.Id > 0);
        Assert.Equal(a.Id, row.Id);                 // in-memory key matches the re-inserted row
        Assert.NotNull(await ctx.FindAsync<Row>(a.Id));   // identity map resolves to a live row
    }

    [Fact]
    public async Task TwoInsertsRolledBackInTransaction_BothReInsertedWithDistinctKeys()
    {
        await using var cn = Db();
        await using var ctx = Ctx(cn);

        var a = new Row { Name = "a" };
        var b = new Row { Name = "b" };
        ctx.Add(a);
        ctx.Add(b);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        await ctx.SaveChangesAsync();

        Assert.Equal(2, await ctx.Query<Row>().CountAsync());
        Assert.True(a.Id > 0 && b.Id > 0 && a.Id != b.Id);
    }

    // A convention-key parent + children graph: after a rolled-back transaction the whole graph must
    // re-insert with correct FK linkage (the parent key is not reset on rollback for convention keys, and
    // FK fixup must still resolve the child FKs on the re-save).

    [Table("SgkrParent")]
    public sealed class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("SgkrChild")]
    public sealed class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = "";
        public Parent Parent { get; set; } = default!;
    }

    private static SqliteConnection GraphDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE SgkrParent (Id INTEGER NOT NULL, Name TEXT NULL, CONSTRAINT PK_SgkrParent PRIMARY KEY (Id));" +
            "CREATE TABLE SgkrChild (Id INTEGER NOT NULL, ParentId INTEGER NOT NULL, Tag TEXT NULL, CONSTRAINT PK_SgkrChild PRIMARY KEY (Id));";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext GraphCtx(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Parent>().HasKey(x => x.Id);
            mb.Entity<Child>().HasKey(x => x.Id);
            mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId, p => p.Id);
        }
    }, ownsConnection: false);

    [Fact]
    public async Task GraphRolledBackInTransaction_ReInsertsWithCorrectForeignKeyLinkage()
    {
        await using var cn = GraphDb();
        await using var ctx = GraphCtx(cn);

        var p = new Parent { Name = "p" };
        p.Children.Add(new Child { Tag = "c1" });
        p.Children.Add(new Child { Tag = "c2" });
        ctx.Add(p);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        await ctx.SaveChangesAsync();   // re-insert the whole graph

        Assert.Single(await ctx.Query<Parent>().ToListAsync());
        var children = await ctx.Query<Child>().ToListAsync();
        Assert.Equal(2, children.Count);
        Assert.True(p.Id > 0);
        Assert.All(children, c => Assert.Equal(p.Id, c.ParentId));
    }
}
