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

/// <summary>
/// Contract for nullable-column null fidelity through the BULK insert path (bulk-path matrix cell).
///
/// Nulls bulk-insert as SQL NULL for every type family (asserted with a raw-SQL IS NULL dump) and
/// read back as null, while non-null values in the same columns round-trip exactly; WHERE
/// <c>col == null</c> / <c>!= null</c> partition the bulk-written rows like C#.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkNullableFidelityContractTests
{
    private enum Color { Red = 1, Blue = 2 }

    [Table("BulkNullableFidelity")]
    private sealed class N
    {
        [Key] public int Id { get; set; }
        public int? I { get; set; }
        public ulong? U { get; set; }
        public decimal? M { get; set; }
        public DateTime? Dt { get; set; }
        public DateTimeOffset? Dto { get; set; }
        public Guid? G { get; set; }
        public Color? C { get; set; }
        public string? S { get; set; }
        public byte[]? B { get; set; }
    }

    [Fact]
    public async Task Bulk_nulls_store_sql_null_and_partition_like_csharp()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE BulkNullableFidelity (Id INTEGER PRIMARY KEY, I INTEGER, U INTEGER, M TEXT, Dt TEXT, Dto TEXT, G TEXT, C INTEGER, S TEXT, B BLOB);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var theGuid = Guid.NewGuid();
        var theDto = new DateTimeOffset(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(5));
        await ctx.BulkInsertAsync(new[]
        {
            new N { Id = 1 },   // all nullable columns null
            new N
            {
                Id = 2, I = 42, U = 5_000_000_000UL, M = 1.5m,
                Dt = new DateTime(2020, 6, 15), Dto = theDto, G = theGuid,
                C = Color.Blue, S = "x", B = new byte[] { 1, 2 },
            },
        });

        // Storage: row 1 is SQL NULL in every nullable column.
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM BulkNullableFidelity WHERE Id = 1 AND I IS NULL AND U IS NULL AND M IS NULL AND Dt IS NULL AND Dto IS NULL AND G IS NULL AND C IS NULL AND S IS NULL AND B IS NULL;";
            Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));
        }

        var back = ((INormQueryable<N>)ctx.Query<N>()).AsNoTracking().OrderBy(n => n.Id).ToList();
        Assert.Null(back[0].I); Assert.Null(back[0].U); Assert.Null(back[0].M);
        Assert.Null(back[0].Dt); Assert.Null(back[0].Dto); Assert.Null(back[0].G);
        Assert.Null(back[0].C); Assert.Null(back[0].S); Assert.Null(back[0].B);

        Assert.Equal(42, back[1].I);
        Assert.Equal(5_000_000_000UL, back[1].U);
        Assert.Equal(1.5m, back[1].M);
        Assert.Equal(theDto, back[1].Dto);
        Assert.Equal(theGuid, back[1].G);
        Assert.Equal(Color.Blue, back[1].C);
        Assert.Equal("x", back[1].S);
        Assert.True(back[1].B!.AsSpan().SequenceEqual(new byte[] { 1, 2 }));

        var q = ((INormQueryable<N>)ctx.Query<N>()).AsNoTracking();
        Assert.Equal(new[] { 1 }, q.Where(n => n.M == null).Select(n => n.Id).ToList());
        Assert.Equal(new[] { 2 }, q.Where(n => n.G != null).Select(n => n.Id).ToList());
        Assert.Equal(new[] { 1 }, q.Where(n => n.U == null).Select(n => n.Id).ToList());
    }
}
