using System;
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
/// Pins <c>EntityEntry.GetDatabaseValues()</c>/<c>GetDatabaseValuesAsync()</c> (EF-parity): reads the
/// entity's current database row as a detached <see cref="PropertyValues"/> snapshot without disturbing the
/// tracked entity or its pending edits. Returns null when the row is gone. The store snapshot feeds the
/// canonical concurrency-conflict resolution (SetValues the winner back).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GetDatabaseValuesContractTests
{
    [Table("GdvWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Qty { get; set; }
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:gdv_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GdvWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Qty INTEGER NOT NULL);
                INSERT INTO GdvWidget VALUES (1, 'original', 10);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static void ExternalUpdate(SqliteConnection keeper, string sql)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void Returns_store_values_not_local_edits()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        using var ctx = make();

        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        w.Name = "local-edit";   // pending, unsaved edit on the tracked entity

        var db = ctx.Entry(w).GetDatabaseValues();

        Assert.NotNull(db);
        Assert.Equal("original", db!["Name"]);   // the DB row, not the local edit
        Assert.Equal(10, Convert.ToInt32(db["Qty"]));
        Assert.Equal("local-edit", w.Name);      // reading the store did not disturb the tracked entity
    }

    [Fact]
    public void Reflects_a_concurrent_external_change()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        using var ctx = make();

        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        ExternalUpdate(keeper, "UPDATE GdvWidget SET Qty = 99, Name = 'theirs' WHERE Id = 1");

        var db = ctx.Entry(w).GetDatabaseValues();

        Assert.NotNull(db);
        Assert.Equal(99, Convert.ToInt32(db!["Qty"]));   // fresh store value
        Assert.Equal("theirs", db["Name"]);
        Assert.Equal(10, w.Qty);                         // tracked entity is still the stale snapshot
        Assert.Equal("original", w.Name);
    }

    [Fact]
    public async Task Async_reflects_a_concurrent_external_change()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        ExternalUpdate(keeper, "UPDATE GdvWidget SET Qty = 42 WHERE Id = 1");

        var db = await ctx.Entry(w).GetDatabaseValuesAsync();

        Assert.NotNull(db);
        Assert.Equal(42, Convert.ToInt32(db!["Qty"]));
        Assert.Equal(10, w.Qty);
    }

    [Fact]
    public void Returns_null_when_the_row_was_deleted()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        using var ctx = make();

        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        ExternalUpdate(keeper, "DELETE FROM GdvWidget WHERE Id = 1");

        Assert.Null(ctx.Entry(w).GetDatabaseValues());
    }

    [Fact]
    public void Store_values_resolve_a_conflict_via_SetValues()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        using var ctx = make();

        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        w.Name = "mine";   // client edit
        ExternalUpdate(keeper, "UPDATE GdvWidget SET Name = 'theirs' WHERE Id = 1");

        // Canonical store-wins resolution: copy the current database values onto the tracked entity.
        var db = ctx.Entry(w).GetDatabaseValues();
        ctx.Entry(w).CurrentValues.SetValues(db!);

        Assert.Equal("theirs", w.Name);

        // The snapshot is a detached copy — mutating it afterwards must not touch the tracked entity.
        db!["Name"] = "scratch";
        Assert.Equal("theirs", w.Name);
    }
}
