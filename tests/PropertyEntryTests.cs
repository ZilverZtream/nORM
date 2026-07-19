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
/// <see cref="EntityEntry.Property(string)"/> → <see cref="PropertyEntry"/> (EF Core parity): reads a single
/// property's current and original value and its modified state, and — now that SaveChanges issues
/// partial-column UPDATEs — lets you force a column into the UPDATE (IsModified = true, even when unchanged) or
/// exclude it (IsModified = false, even when changed).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyEntryTests
{
    [Table("PeWidget")]
    private class Widget
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
    }, ownsConnection: false);

    private static SqliteConnection Open()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PeWidget (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        return cn;
    }

    private static void RawUpdate(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery();
    }

    private static (int A, int B, int C) ReadRow(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT A, B, C FROM PeWidget WHERE Id = 1";
        using var r = cmd.ExecuteReader(); r.Read();
        return (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2));
    }

    private static async Task SeedAsync(SqliteConnection cn)
    {
        await using var ctx = Ctx(cn);
        ctx.Add(new Widget { Id = 1, A = 10, B = 20, C = 30 });
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Property_reads_current_original_and_modified_state()
    {
        using var cn = Open(); await SeedAsync(cn);
        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        w.A = 11;

        var entry = ctx.Entry(w);
        Assert.Equal("A", entry.Property("A").Name);
        Assert.Equal(11, entry.Property("A").CurrentValue);
        Assert.Equal(10, entry.Property("A").OriginalValue);
        Assert.True(entry.Property("A").IsModified);
        Assert.False(entry.Property("B").IsModified);   // unchanged
    }

    [Fact]
    public async Task Setting_current_value_via_property_writes_it()
    {
        using var cn = Open(); await SeedAsync(cn);
        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();

        ctx.Entry(w).Property("A").CurrentValue = 11;
        Assert.Equal(11, w.A);
        await ctx.SaveChangesAsync();
        Assert.Equal(11, ReadRow(cn).A);
    }

    [Fact]
    public async Task IsModified_true_forces_an_unchanged_column_into_the_update()
    {
        using var cn = Open(); await SeedAsync(cn);
        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();

        // B is not changed on the entity, but we force it modified — it must be written (with the tracked
        // value 20), so a concurrent B = 99 gets overwritten. Without the force, a partial update omits B.
        ctx.Entry(w).Property("B").IsModified = true;
        RawUpdate(cn, "UPDATE PeWidget SET B = 99 WHERE Id = 1");

        await ctx.SaveChangesAsync();

        Assert.Equal(20, ReadRow(cn).B);   // forced write of the tracked value clobbered the concurrent 99
    }

    [Fact]
    public async Task IsModified_false_excludes_a_changed_column_from_the_update()
    {
        using var cn = Open(); await SeedAsync(cn);
        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        w.A = 11;   // changed
        w.C = 33;   // changed

        // Exclude A from the update even though it changed; C still writes.
        ctx.Entry(w).Property("A").IsModified = false;
        RawUpdate(cn, "UPDATE PeWidget SET A = 44 WHERE Id = 1");

        await ctx.SaveChangesAsync();

        var (a, _, c) = ReadRow(cn);
        Assert.Equal(44, a);   // A excluded → concurrent value survived, my 11 discarded
        Assert.Equal(33, c);   // C still written
    }

    [Fact]
    public async Task IsModified_cannot_be_set_on_a_key_column()
    {
        using var cn = Open(); await SeedAsync(cn);
        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(w).Property("Id").IsModified = true);
    }

    [Fact]
    public async Task Property_with_unknown_name_throws()
    {
        using var cn = Open(); await SeedAsync(cn);
        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        Assert.ThrowsAny<Exception>(() => ctx.Entry(w).Property("Nonexistent"));
    }
}
