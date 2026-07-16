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
/// Contract for nullable-column null round-trip fidelity (write-path matrix cell: nullable-of-any).
///
/// A null written through nORM stores as SQL NULL and reads back as null for EVERY probed type
/// family - int?, ulong?, decimal?, double?, DateTime?, DateTimeOffset?, Guid?, enum?, string?, and
/// byte[]? - while a non-null value in the same column round-trips exactly. WHERE <c>col == null</c>
/// matches only the null row and <c>col != null</c> matches only the value row, like C#.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NullableColumnRoundTripContractTests
{
    private enum Color { Red = 1, Blue = 2 }

    [Table("NullableRtContract")]
    private sealed class N
    {
        [Key] public int Id { get; set; }
        public int? I { get; set; }
        public ulong? U { get; set; }
        public decimal? M { get; set; }
        public double? D { get; set; }
        public DateTime? Dt { get; set; }
        public DateTimeOffset? Dto { get; set; }
        public Guid? G { get; set; }
        public Color? C { get; set; }
        public string? S { get; set; }
        public byte[]? B { get; set; }
    }

    private static readonly Guid TheGuid = Guid.Parse("a1b2c3d4-e5f6-4708-9a0b-c1d2e3f4a5b6");
    private static readonly DateTime TheDt = new(2020, 6, 15, 10, 30, 45);
    private static readonly DateTimeOffset TheDto = new(2020, 6, 15, 12, 0, 0, TimeSpan.FromHours(5));

    private static async Task<(SqliteConnection cn, DbContext ctx)> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE NullableRtContract (Id INTEGER PRIMARY KEY, I INTEGER, U INTEGER, M TEXT, D REAL, Dt TEXT, Dto TEXT, G TEXT, C INTEGER, S TEXT, B BLOB);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        await ctx.InsertAsync(new N { Id = 1 });   // all nullable columns null
        await ctx.InsertAsync(new N
        {
            Id = 2,
            I = 42,
            U = 5_000_000_000UL,
            M = 1.00000000000000005m,
            D = 0.1 + 0.2,
            Dt = TheDt,
            Dto = TheDto,
            G = TheGuid,
            C = Color.Blue,
            S = "x",
            B = new byte[] { 1, 2, 3 },
        });
        return (cn, ctx);
    }

    [Fact]
    public async Task Null_stores_as_sql_null_and_reads_back_null_for_every_family()
    {
        var (cn, ctx) = await SeedAsync();
        using (ctx)
        {
            // Storage: every nullable column of row 1 is SQL NULL.
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "SELECT COUNT(*) FROM NullableRtContract WHERE Id = 1 AND I IS NULL AND U IS NULL AND M IS NULL AND D IS NULL AND Dt IS NULL AND Dto IS NULL AND G IS NULL AND C IS NULL AND S IS NULL AND B IS NULL;";
                Assert.Equal(1, Convert.ToInt32(c.ExecuteScalar()));
            }

            var back = ((INormQueryable<N>)ctx.Query<N>()).AsNoTracking().OrderBy(n => n.Id).ToList();

            var nullRow = back[0];
            Assert.Null(nullRow.I);
            Assert.Null(nullRow.U);
            Assert.Null(nullRow.M);
            Assert.Null(nullRow.D);
            Assert.Null(nullRow.Dt);
            Assert.Null(nullRow.Dto);
            Assert.Null(nullRow.G);
            Assert.Null(nullRow.C);
            Assert.Null(nullRow.S);
            Assert.Null(nullRow.B);

            var valRow = back[1];
            Assert.Equal(42, valRow.I);
            Assert.Equal(5_000_000_000UL, valRow.U);
            Assert.Equal(1.00000000000000005m, valRow.M);
            Assert.Equal(0.1 + 0.2, valRow.D);
            Assert.Equal(TheDt.Ticks, valRow.Dt!.Value.Ticks);
            Assert.Equal(TheDto, valRow.Dto);
            Assert.Equal(TheGuid, valRow.G);
            Assert.Equal(Color.Blue, valRow.C);
            Assert.Equal("x", valRow.S);
            Assert.True(valRow.B!.AsSpan().SequenceEqual(new byte[] { 1, 2, 3 }));
        }
    }

    [Fact]
    public async Task Where_null_and_not_null_partition_the_rows_like_csharp()
    {
        var (_, ctx) = await SeedAsync();
        using (ctx)
        {
            var q = ((INormQueryable<N>)ctx.Query<N>()).AsNoTracking();

            Assert.Equal(new[] { 1 }, q.Where(n => n.I == null).Select(n => n.Id).ToList());
            Assert.Equal(new[] { 2 }, q.Where(n => n.I != null).Select(n => n.Id).ToList());
            Assert.Equal(new[] { 1 }, q.Where(n => n.M == null).Select(n => n.Id).ToList());
            Assert.Equal(new[] { 1 }, q.Where(n => n.G == null).Select(n => n.Id).ToList());
            Assert.Equal(new[] { 2 }, q.Where(n => n.S != null).Select(n => n.Id).ToList());
            Assert.Equal(new[] { 1 }, q.Where(n => n.Dto == null).Select(n => n.Id).ToList());
            Assert.Equal(new[] { 2 }, q.Where(n => n.C != null).Select(n => n.Id).ToList());
        }
    }
}
