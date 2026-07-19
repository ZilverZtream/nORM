using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Partial-column UPDATE with a COMPOSITE primary key: the SET carries only the changed column while the WHERE
/// carries every key column, so the positional @pN parameters must line up across (partial SET → all key
/// columns). A misalignment would target the wrong row or write the wrong column — this pins that the composite
/// WHERE and the partial SET stay in lockstep, and a concurrent write to an unchanged column survives.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompositeKeyPartialColumnUpdateTests
{
    [Table("CkpWidget")]
    private class Widget
    {
        public int K1 { get; set; }
        public int K2 { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => new { w.K1, w.K2 })
    }, ownsConnection: false);

    [Fact]
    public async Task Partial_update_with_a_composite_key_targets_the_right_row_and_keeps_unchanged_columns()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CkpWidget (K1 INTEGER NOT NULL, K2 INTEGER NOT NULL, A INTEGER NOT NULL, B INTEGER NOT NULL, PRIMARY KEY (K1, K2));
                INSERT INTO CkpWidget VALUES (1, 2, 10, 20), (1, 3, 100, 200);
                """;
            cmd.ExecuteNonQuery();
        }

        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<Widget>().Where(x => x.K1 == 1 && x.K2 == 2).ToListAsync()).Single();
        w.A = 11;   // only A changed on row (1,2)

        // Concurrent write to the unchanged column B on the SAME row must survive the partial update.
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "UPDATE CkpWidget SET B = 99 WHERE K1 = 1 AND K2 = 2"; cmd.ExecuteNonQuery(); }

        await ctx.SaveChangesAsync();

        using var read = cn.CreateCommand();
        read.CommandText = "SELECT K1, K2, A, B FROM CkpWidget ORDER BY K2";
        using var r = read.ExecuteReader();
        r.Read();   // row (1,2)
        Assert.Equal((1, 2, 11, 99), (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2), r.GetInt32(3)));
        r.Read();   // row (1,3) — untouched, proves the composite WHERE targeted only (1,2)
        Assert.Equal((1, 3, 100, 200), (r.GetInt32(0), r.GetInt32(1), r.GetInt32(2), r.GetInt32(3)));
    }
}
