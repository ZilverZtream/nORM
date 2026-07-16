using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for GroupBy key-equality semantics per key type (Query/LINQ matrix cell).
///
/// Group keys compare like .NET's default equality comparer for every probed key type:
///
///  * string keys group case-SENSITIVELY (ordinal) - "a" and "A" are distinct groups, matching
///    <c>EqualityComparer&lt;string&gt;.Default</c>.
///  * decimal keys group at FULL precision: two keys that agree to 16 significant digits and differ
///    at the 17th form SEPARATE groups with their own aggregates (the exact-equality canonical-text
///    key, not the approximate REAL coercion).
///  * nullable keys produce a null-key group, exactly like <c>Enumerable.GroupBy</c>.
///  * value-converter keys group by the stored provider value; for the (by definition injective)
///    converter the groups are identical to grouping by the CLR key, and the key materializes back
///    through <c>ConvertFromProvider</c>.
///  * composite anonymous keys compare component-wise, including null components.
///
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GroupByKeySemanticsContractTests
{
    private enum Cat { A = 1, B = 2 }

    [Table("GrpKeyContract")]
    private sealed class G
    {
        [Key] public int Id { get; set; }
        public string SVal { get; set; } = "";
        public decimal DVal { get; set; }
        public int? NVal { get; set; }
        public Cat CVal { get; set; }
        public int XVal { get; set; }
    }

    private sealed class CatToNameConverter : ValueConverter<Cat, string>
    {
        public override object? ConvertToProvider(Cat v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Cat>(v);
    }

    private static readonly (int Id, string S, decimal D, int? N, Cat C, int X)[] Rows =
    {
        (1, "a",  1.00000000000000005m, 1,    Cat.A, 10),
        (2, "A",  1.00000000000000006m, null, Cat.B, 20),
        (3, "a",  1.00000000000000005m, 1,    Cat.A, 30),
        (4, "b",  2m,                   null, Cat.B, 40),
        (5, "A",  1.00000000000000006m, 2,    Cat.A, 50),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE GrpKeyContract (Id INTEGER PRIMARY KEY, SVal TEXT NOT NULL, DVal TEXT NOT NULL, NVal INTEGER, CVal TEXT NOT NULL, XVal INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<G>().Property<Cat>(p => p.CVal).HasConversion(new CatToNameConverter())
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in Rows) await ctx.InsertAsync(new G { Id = r.Id, SVal = r.S, DVal = r.D, NVal = r.N, CVal = r.C, XVal = r.X });
        return ctx;
    }

    [Fact]
    public async Task GroupBy_string_key_groups_case_sensitively_like_dotnet()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking()
            .GroupBy(g => g.SVal).Select(g => new { g.Key, N = g.Count() })
            .ToList().OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
        var oracle = Rows.GroupBy(r => r.S).Select(g => new { g.Key, N = g.Count() })
            .OrderBy(x => x.Key, StringComparer.Ordinal).ToList();

        Assert.Equal(oracle.Select(x => (x.Key, x.N)), norm.Select(x => (x.Key, x.N)));
        Assert.Equal(3, norm.Count);   // "A", "a", "b" - case-distinct
    }

    [Fact]
    public async Task GroupBy_decimal_key_keeps_17th_digit_keys_distinct()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking()
            .GroupBy(g => g.DVal).Select(g => new { g.Key, N = g.Count(), Sum = g.Sum(x => x.XVal) })
            .ToList().OrderBy(x => x.Key).ToList();
        var oracle = Rows.GroupBy(r => r.D).Select(g => new { g.Key, N = g.Count(), Sum = g.Sum(x => x.X) })
            .OrderBy(x => x.Key).ToList();

        Assert.Equal(oracle.Select(x => (x.Key, x.N, x.Sum)), norm.Select(x => (x.Key, x.N, x.Sum)));
        Assert.Equal(3, norm.Count);   // ...05, ...06, and 2 - the 17th-digit pair does NOT merge
    }

    [Fact]
    public async Task GroupBy_nullable_key_produces_a_null_group_like_dotnet()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking()
            .GroupBy(g => g.NVal).Select(g => new { g.Key, N = g.Count() })
            .ToList().OrderBy(x => x.Key ?? int.MinValue).ToList();
        var oracle = Rows.GroupBy(r => r.N).Select(g => new { g.Key, N = g.Count() })
            .OrderBy(x => x.Key ?? int.MinValue).ToList();

        Assert.Equal(oracle.Select(x => (x.Key, x.N)), norm.Select(x => (x.Key, x.N)));
        Assert.Contains(norm, x => x.Key is null && x.N == 2);   // the null group exists
    }

    [Fact]
    public async Task GroupBy_converter_key_groups_by_provider_value_and_materializes_clr_key()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking()
            .GroupBy(g => g.CVal).Select(g => new { g.Key, N = g.Count() })
            .ToList().OrderBy(x => x.Key).ToList();
        var oracle = Rows.GroupBy(r => r.C).Select(g => new { g.Key, N = g.Count() })
            .OrderBy(x => x.Key).ToList();

        Assert.Equal(oracle.Select(x => (x.Key, x.N)), norm.Select(x => (x.Key, x.N)));
    }

    [Fact]
    public async Task GroupBy_composite_anonymous_key_compares_component_wise_including_null()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking()
            .GroupBy(g => new { g.SVal, g.NVal }).Select(g => new { g.Key.SVal, g.Key.NVal, N = g.Count() })
            .ToList().OrderBy(x => x.SVal, StringComparer.Ordinal).ThenBy(x => x.NVal ?? int.MinValue).ToList();
        var oracle = Rows.GroupBy(r => new { SVal = r.S, NVal = r.N }).Select(g => new { g.Key.SVal, g.Key.NVal, N = g.Count() })
            .OrderBy(x => x.SVal, StringComparer.Ordinal).ThenBy(x => x.NVal ?? int.MinValue).ToList();

        Assert.Equal(oracle.Select(x => (x.SVal, x.NVal, x.N)), norm.Select(x => (x.SVal, x.NVal, x.N)));
        Assert.Equal(4, norm.Count);   // (A,null) (A,2) (a,1) (b,null)
    }
}
