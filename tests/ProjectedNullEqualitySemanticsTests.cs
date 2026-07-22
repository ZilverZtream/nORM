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

namespace nORM.Tests
{
    /// <summary>
    /// A projected <c>==</c>/<c>!=</c> between two potentially-null operands must materialize the
    /// same boolean as C#'s lifted equality: <c>null == null</c> is <c>true</c>, <c>value == null</c>
    /// is <c>false</c>, and <c>value != null</c> / <c>null != value</c> are <c>true</c>. Emitting raw
    /// <c>A = B</c> / <c>A &lt;&gt; B</c> would let SQL three-valued logic collapse those to FALSE — a
    /// silently-wrong computed value. The projection path must apply the provider's null-safe
    /// (in)equality, mirroring the WHERE-path expansion.
    /// </summary>
    [Xunit.Trait("Category", "Fast")]
    public sealed class ProjectedNullEqualitySemanticsTests
    {
        [Table("NullEqRow")]
        public sealed class Row
        {
            [Key] public int Id { get; set; }
            public int? A { get; set; }
            public int? B { get; set; }
            public string? S1 { get; set; }
            public string? S2 { get; set; }
            public decimal? D1 { get; set; }
            public decimal? D2 { get; set; }
        }

        private static readonly List<Row> Data = new()
        {
            new Row { Id = 1, A = 1,    B = 1,    S1 = "x",  S2 = "x",  D1 = 10.50m, D2 = 10.5m  },  // equal (decimal scale differs)
            new Row { Id = 2, A = 2,    B = null, S1 = "y",  S2 = null, D1 = 2m,     D2 = null   },  // right null
            new Row { Id = 3, A = null, B = 2,    S1 = null, S2 = "z",  D1 = null,   D2 = 2m     },  // left null
            new Row { Id = 4, A = null, B = null, S1 = null, S2 = null, D1 = null,   D2 = null   },  // both null
            new Row { Id = 5, A = 3,    B = 2,    S1 = "p",  S2 = "q",  D1 = 3m,     D2 = 2m     },  // both non-null, differ
        };

        private static async Task<DbContext> NewContextAsync()
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE NullEqRow (Id INTEGER PRIMARY KEY, A INTEGER NULL, B INTEGER NULL, S1 TEXT NULL, S2 TEXT NULL, D1 TEXT NULL, D2 TEXT NULL)";
                cmd.ExecuteNonQuery();
            }
            var ctx = new DbContext(cn, new SqliteProvider(),
                new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(x => x.Id) });
            foreach (var r in Data) ctx.Add(r);
            await ctx.SaveChangesAsync();
            return ctx;
        }

        [Fact]
        public async Task ProjectedEqualityOfTwoNullableIntColumns_MatchesLiftedCSharpEquality()
        {
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Eq = r.A == r.B }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            var expected = Data.Select(r => new { r.Id, Eq = r.A == r.B })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            Assert.Equal(expected, actual);          // row 4 (null == null) must be true
        }

        [Fact]
        public async Task ProjectedInequalityOfTwoNullableIntColumns_MatchesLiftedCSharpInequality()
        {
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Ne = r.A != r.B }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            var expected = Data.Select(r => new { r.Id, Ne = r.A != r.B })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            Assert.Equal(expected, actual);          // rows 2,3 (value != null) must be true; row 4 false
        }

        [Fact]
        public async Task ProjectedEqualityOfTwoNullableStringColumns_MatchesLiftedCSharpEquality()
        {
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Eq = r.S1 == r.S2 }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            var expected = Data.Select(r => new { r.Id, Eq = r.S1 == r.S2 })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            Assert.Equal(expected, actual);          // row 4 (null == null) must be true
        }

        [Fact]
        public async Task ProjectedInequalityOfTwoNullableStringColumns_MatchesLiftedCSharpInequality()
        {
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Ne = r.S1 != r.S2 }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            var expected = Data.Select(r => new { r.Id, Ne = r.S1 != r.S2 })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            Assert.Equal(expected, actual);          // rows 2,3 (value != null) must be true; row 4 false
        }

        [Fact]
        public async Task ProjectedEqualityOfTwoNullableDecimalColumns_IsScaleInsensitiveAndNullSafe()
        {
            // C# decimal equality is scale-insensitive (10.50m == 10.5m) and lifted (null == null → true).
            // nORM stores decimals as canonical TEXT so the null-safe `=` between two columns preserves both.
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Eq = r.D1 == r.D2 }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            var expected = Data.Select(r => new { r.Id, Eq = r.D1 == r.D2 })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            Assert.Equal(expected, actual);          // row 1 (10.50 == 10.5) and row 4 (null == null) must be true
        }

        [Fact]
        public async Task ProjectedInequalityOfTwoNullableDecimalColumns_IsScaleInsensitiveAndNullSafe()
        {
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Ne = r.D1 != r.D2 }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            var expected = Data.Select(r => new { r.Id, Ne = r.D1 != r.D2 })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            Assert.Equal(expected, actual);          // rows 2,3 (value != null) true; rows 1,4 false
        }

        [Fact]
        public async Task ProjectedEqualityOfNullableColumnAgainstNonNullConstant_StaysCorrect()
        {
            // Guard against over-expansion: `col == 2` where col is null is FALSE in both C# and SQL,
            // so the null-safe form must not change this (col == null-const collapse must not leak in).
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Eq = r.A == 2 }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            var expected = Data.Select(r => new { r.Id, Eq = r.A == 2 })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Eq)).ToList();
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task ProjectedInequalityOfNullableColumnAgainstNonNullConstant_TreatsNullAsUnequal()
        {
            // `col != 2` where col is null is TRUE in C#; the WHERE/projection expansion must keep it true.
            await using var ctx = await NewContextAsync();
            var actual = (await ctx.Query<Row>().Select(r => new { r.Id, Ne = r.A != 2 }).ToListAsync())
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            var expected = Data.Select(r => new { r.Id, Ne = r.A != 2 })
                .OrderBy(x => x.Id).Select(x => (x.Id, x.Ne)).ToList();
            Assert.Equal(expected, actual);          // rows 3,4 (null != 2) must be true
        }
    }
}
