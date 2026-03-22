using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// Entity that has writable public properties but NO public parameterless constructor.
// This is a real-world DDD pattern: force valid initial state via constructor.
// Before the fix, nORM silently skipped tracking for such entities because
// QueryExecutor checked GetConstructor(Type.EmptyTypes) != null before adding to the ChangeTracker.
// The materializer itself already supported parameterized constructors via GetCachedConstructor.

[Table("CtorBoundItem")]
file class CtorBoundItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;

    // Only parameterized constructor — no public parameterless ctor.
    // C# does NOT auto-generate a parameterless ctor when an explicit ctor is defined.
    public CtorBoundItem(int id, string name)
    {
        Id = id;
        Name = name;
    }
}

/// <summary>
/// Tests that entities with only a parameterized constructor are tracked by the ChangeTracker
/// after a query, just like entities with a public parameterless constructor.
///
/// Root cause: <c>QueryExecutor</c> had a <c>GetConstructor(Type.EmptyTypes) != null</c> guard
/// in all six trackability checks. The materializer already supports parameterized-constructor
/// entities via <c>GetCachedConstructor</c>, so the guard was incorrect.
/// </summary>
public class ConstructorBoundEntityTrackingTests
{
    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CtorBoundItem (
                Id   INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT    NOT NULL
            )";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task QueryResult_IsTracked()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CtorBoundItem (Name) VALUES ('Alice')";
            cmd.ExecuteNonQuery();
        }

        var entity = await ctx.Query<CtorBoundItem>().FirstAsync();

        var entry = ctx.ChangeTracker.Entries.FirstOrDefault(e => ReferenceEquals(e.Entity, entity));
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Unchanged, entry!.State);
    }

    [Fact]
    public async Task Query_ReturnsEntityWithCorrectValues()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CtorBoundItem (Name) VALUES ('Bob')";
            cmd.ExecuteNonQuery();
        }

        var entity = await ctx.Query<CtorBoundItem>().FirstAsync();
        Assert.Equal("Bob", entity.Name);
        Assert.True(entity.Id > 0);
    }

    [Fact]
    public async Task MultipleEntities_AllTracked()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CtorBoundItem (Name) VALUES ('A'),('B'),('C')";
            cmd.ExecuteNonQuery();
        }

        var list = await ctx.Query<CtorBoundItem>().ToListAsync();
        Assert.Equal(3, list.Count);
        foreach (var e in list)
        {
            var entry = ctx.ChangeTracker.Entries.FirstOrDefault(x => ReferenceEquals(x.Entity, e));
            Assert.NotNull(entry);
            Assert.Equal(EntityState.Unchanged, entry!.State);
        }
    }

    [Fact]
    public async Task AsNoTracking_NotInChangeTracker()
    {
        // AsNoTracking must bypass tracking even for constructor-bound entities.
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CtorBoundItem (Name) VALUES ('X')";
            cmd.ExecuteNonQuery();
        }

        var entity = await ((INormQueryable<CtorBoundItem>)ctx.Query<CtorBoundItem>())
            .AsNoTracking().FirstAsync();

        var entry = ctx.ChangeTracker.Entries.FirstOrDefault(e => ReferenceEquals(e.Entity, entity));
        Assert.Null(entry);
    }

    [Fact]
    public void TypeCheck_HasNoPublicParameterlessCtor()
    {
        // Confirms the entity actually lacks a parameterless constructor,
        // so we know the pre-fix code would have silently skipped tracking it.
        var ctor = typeof(CtorBoundItem).GetConstructor(Type.EmptyTypes);
        Assert.Null(ctor);
    }
}
