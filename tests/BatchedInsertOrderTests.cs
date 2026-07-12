using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Batched SaveChanges must insert entities in Add order, so autoincrement keys are assigned
/// predictably. The change tracker's backing stores enumerate in identity-hash order, which
/// varies run to run — batch order must come from the attach sequence instead.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BatchedInsertOrderTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BioRow")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int Seq { get; set; }
    }

    [Fact]
    public async Task Batched_inserts_preserve_add_order()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BioRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Seq INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 0; i < 25; i++)
            ctx.Add(new Row { Seq = i });
        await ctx.SaveChangesAsync();

        // Ids ordered ascending must yield Seq 0..24 — i.e. inserts happened in Add order.
        var seqs = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Seq).ToList();
        Assert.Equal(Enumerable.Range(0, 25).ToList(), seqs);
    }
}
