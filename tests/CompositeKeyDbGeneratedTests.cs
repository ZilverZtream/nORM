using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//Verifies that a composite PK where one column is DB-generated (AUTOINCREMENT)
//correctly avoids identity-map aliasing when two entities have the same default key value
//before SaveChangesAsync assigns the real DB-generated id.
//</summary>
public class CompositeKeyDbGeneratedTests
{
    private class CompositeOrder2
    {
        public int Id { get; set; }
        public int LineNumber { get; set; }
        public string Description { get; set; } = string.Empty;
    }

    [Table("CompositeOrder")]
    private class CompositeOrder
    {
 //<summary>DB-generated; default 0 before insert.</summary>
        [Key, Column(Order = 0)]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

 //<summary>Manually assigned line number; part of composite PK.</summary>
        [Key, Column(Order = 1)]
        public int LineNumber { get; set; }

        public string Description { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
 // SQLite: Id is AUTOINCREMENT (integer primary key), LineNumber is part of PK
 // We model this as a composite key where Id is DB-generated.
            cmd.CommandText =
                "CREATE TABLE CompositeOrder (" +
                "  Id INTEGER NOT NULL," +
                "  LineNumber INTEGER NOT NULL," +
                "  Description TEXT NOT NULL," +
                "  PRIMARY KEY (Id, LineNumber)" +
                ");";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CompositeOrder>()
                .HasKey(o => new { o.Id, o.LineNumber })
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx);
    }

    [Fact]
    public void Add_two_entities_with_default_DbGenerated_key_are_distinct_in_tracker()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

 // Both entities have Id=0 (DB-generated default) and LineNumber=0 (also default).
 // Before the fix they both returned composite key (0,0) and the second entity
 // was aliased to the first entry in the identity map.
        var e1 = new CompositeOrder { Description = "First" };   // Id=0, LineNumber=0
        var e2 = new CompositeOrder { Description = "Second" };  // Id=0, LineNumber=0

        ctx.Add(e1);
        ctx.Add(e2);

 // Both must be tracked as distinct entries — not aliased to one another.
        Assert.Equal(2, ctx.ChangeTracker.Entries.Count());
    }

    [Fact]
    public void Add_two_entities_with_different_line_numbers_and_default_id_are_distinct()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

 // Id is DB-generated (default 0), LineNumbers differ.
 // The fix returns null for both (Id=0 triggers the guard), so both tracked by reference.
        var e1 = new CompositeOrder { LineNumber = 1, Description = "Line1" };
        var e2 = new CompositeOrder { LineNumber = 2, Description = "Line2" };

        ctx.Add(e1);
        ctx.Add(e2);

        Assert.Equal(2, ctx.ChangeTracker.Entries.Count());
    }

    [Fact]
    public async Task SaveChanges_inserts_two_rows_with_explicit_composite_keys()
    {
 // Use a simpler entity without DB-generated column to verify SaveChanges works
 // after the fix doesn't break the persisting path for explicit composite keys.
        using var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using (var cmd = cn2.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CompositeOrder2 (" +
                "  Id INTEGER NOT NULL," +
                "  LineNumber INTEGER NOT NULL," +
                "  Description TEXT NOT NULL," +
                "  PRIMARY KEY (Id, LineNumber)" +
                ");";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CompositeOrder2>()
                .HasKey(o => new { o.Id, o.LineNumber })
                .ToTable("CompositeOrder2")
        };
        using var ctx2 = new DbContext(cn2, new SqliteProvider(), opts);

        var e1 = new CompositeOrder2 { Id = 1, LineNumber = 1, Description = "Alpha" };
        var e2 = new CompositeOrder2 { Id = 2, LineNumber = 1, Description = "Beta" };

        ctx2.Add(e1);
        ctx2.Add(e2);

        Assert.Equal(2, ctx2.ChangeTracker.Entries.Count());

        await ctx2.SaveChangesAsync();

        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "SELECT COUNT(*) FROM CompositeOrder2";
        var count = (long)cmd2.ExecuteScalar()!;
        Assert.Equal(2, count);
    }

    [Fact]
    public void Explicitly_set_composite_keys_still_tracked_by_pk()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

 // Non-default (non-zero) Id — should still use PK-based identity map
        var e1 = new CompositeOrder { Id = 10, LineNumber = 1, Description = "A" };
        var e2 = new CompositeOrder { Id = 10, LineNumber = 1, Description = "B" };

        var entry1 = ctx.Attach(e1);
        var entry2 = ctx.Attach(e2); // same PK → should return the same entry

 // Second attach with same explicit PK must return the first entity (identity resolution)
        Assert.Same(e1, entry2.Entity);
        Assert.Single(ctx.ChangeTracker.Entries);
    }
}
