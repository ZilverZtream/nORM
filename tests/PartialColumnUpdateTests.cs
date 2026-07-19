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
/// SaveChanges now issues partial-column UPDATEs — only the columns whose values actually changed are written
/// (EF Core parity + a write-amplification win). The strongest proof is behavioral: a column NOT changed on
/// the tracked entity is absent from the SET clause, so a concurrent write to that column survives; a full-row
/// UPDATE would have clobbered it. A forced update (Entry.State = Modified / ctx.Update) still writes every
/// column. A Modified entity with no actual column change falls back to a full update rather than emitting
/// invalid empty-SET SQL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PartialColumnUpdateTests
{
    [Table("PcuWidget")]
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
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PcuWidget (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void RawUpdate(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery();
    }

    private static (int A, int B, int C) ReadRow(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT A, B, C FROM PcuWidget WHERE Id = 1";
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
    public async Task Partial_update_leaves_unchanged_columns_untouched()
    {
        using var cn = Open();
        await SeedAsync(cn);

        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        w.A = 11;   // only A changed; B and C untouched on the tracked entity

        // A concurrent writer changes B (an UNCHANGED column). A partial UPDATE won't include B in its SET,
        // so this value must survive the save; a full-row UPDATE would reset B to the tracked original (20).
        RawUpdate(cn, "UPDATE PcuWidget SET B = 99 WHERE Id = 1");

        await ctx.SaveChangesAsync();

        var (a, b, c) = ReadRow(cn);
        Assert.Equal(11, a);   // my change applied
        Assert.Equal(99, b);   // concurrent change to the unchanged column SURVIVED (proves B not in SET)
        Assert.Equal(30, c);   // untouched
    }

    [Fact]
    public async Task Forced_update_writes_every_column()
    {
        using var cn = Open();
        await SeedAsync(cn);

        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        w.A = 11;
        ctx.Entry(w).State = EntityState.Modified;   // force: EF-parity ctx.Update marks ALL columns modified

        RawUpdate(cn, "UPDATE PcuWidget SET B = 99 WHERE Id = 1");

        await ctx.SaveChangesAsync();

        var (a, b, c) = ReadRow(cn);
        Assert.Equal(11, a);
        Assert.Equal(20, b);   // full update wrote the tracked original, clobbering the concurrent 99
        Assert.Equal(30, c);
    }

    [Fact]
    public async Task Multiple_entities_in_one_batch_each_write_only_their_changed_columns()
    {
        using var cn = Open();
        await using (var seed = Ctx(cn))
        {
            seed.Add(new Widget { Id = 1, A = 10, B = 20, C = 30 });
            seed.Add(new Widget { Id = 2, A = 40, B = 50, C = 60 });
            await seed.SaveChangesAsync();
        }

        await using var ctx = Ctx(cn);
        var rows = (await ctx.Query<Widget>().ToListAsync()).OrderBy(w => w.Id).ToList();
        rows[0].A = 11;   // widget 1: only A changed
        rows[1].C = 66;   // widget 2: only C changed  (different changed-set → different partial SQL)

        // Concurrent writes to each widget's UNCHANGED columns must survive both partial updates.
        RawUpdate(cn, "UPDATE PcuWidget SET B = 99 WHERE Id = 1");
        RawUpdate(cn, "UPDATE PcuWidget SET A = 44 WHERE Id = 2");

        await ctx.SaveChangesAsync();

        using var read = cn.CreateCommand();
        read.CommandText = "SELECT Id, A, B, C FROM PcuWidget ORDER BY Id";
        using var r = read.ExecuteReader();
        r.Read(); Assert.Equal((1, 11, 99, 30), (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2), r.GetInt32(3)));
        r.Read(); Assert.Equal((2, 44, 50, 66), (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2), r.GetInt32(3)));
    }

    [Fact]
    public async Task Modified_entity_with_no_actual_change_falls_back_to_full_update_without_crashing()
    {
        using var cn = Open();
        await SeedAsync(cn);

        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
        ctx.Entry(w).State = EntityState.Modified;   // Modified but no column value changed

        var affected = await ctx.SaveChangesAsync();   // must not emit invalid empty-SET SQL

        Assert.Equal(1, affected);
        var (a, b, c) = ReadRow(cn);
        Assert.Equal((10, 20, 30), (a, b, c));
    }
}
