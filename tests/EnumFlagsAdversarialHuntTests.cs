using System;
using System.Collections.Generic;
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
/// Adversarial hunt: [Flags] enums, HasFlag, bitwise ops, and enums in GroupBy/OrderBy/aggregate/Distinct,
/// covering NEW angles not already swept — string-stored flags bitwise, non-contiguous enum values,
/// nullable-enum bitwise 3VL, computed-mask grouping, enum aggregates. Each asserts against a
/// LINQ-to-Objects oracle (same lambda over in-memory objects).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EnumFlagsAdversarialHuntTests
{
    [Flags]
    public enum Perm { None = 0, Read = 1, Write = 2, Delete = 4, Admin = 8, All = 15 }

    // Deliberately non-contiguous, not-from-zero underlying values.
    public enum Prio { Low = 1, Med = 4, High = 16 }

    // ---- int-stored table ----
    [Table("EfahRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public Perm Access { get; set; }
        public Perm? NAccess { get; set; }
        public Prio Priority { get; set; }
    }

    // (Id, Access, NAccess(-1 => NULL), Priority)
    private static readonly (int Id, int Access, int NAccess, int Prio)[] Data =
    {
        (1, 0,  -1, 1),   // None,        NULL,        Low
        (2, 1,  1,  4),   // Read,        Read,        Med
        (3, 3,  2,  16),  // Read|Write,  Write,       High
        (4, 15, -1, 1),   // All,         NULL,        Low
        (5, 4,  6,  4),   // Delete,      Write|Delete,Med
        (6, 6,  15, 16),  // Write|Delete,All,         High
    };

    private static DbContext MakeIntStored()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EfahRow (Id INTEGER PRIMARY KEY, Access INTEGER NOT NULL, NAccess INTEGER NULL, Priority INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, a, na, p) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = na < 0
                    ? $"INSERT INTO EfahRow VALUES ({id}, {a}, NULL, {p})"
                    : $"INSERT INTO EfahRow VALUES ({id}, {a}, {na}, {p})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row
    {
        Id = d.Id,
        Access = (Perm)d.Access,
        NAccess = d.NAccess < 0 ? (Perm?)null : (Perm)d.NAccess,
        Priority = (Prio)d.Prio,
    });

    // ============ int-stored: aggregate / grouping / ordering ============

    [Fact]
    public async Task Max_over_noncontiguous_enum_returns_max_ordinal()
    {
        await using var ctx = MakeIntStored();
        var got = await ctx.Query<Row>().MaxAsync(x => x.Priority);
        var oracle = Oracle().Max(x => x.Priority);
        Assert.Equal(oracle, got);   // High (16)
    }

    [Fact]
    public async Task Min_over_noncontiguous_enum_returns_min_ordinal()
    {
        await using var ctx = MakeIntStored();
        var got = await ctx.Query<Row>().MinAsync(x => x.Priority);
        var oracle = Oracle().Min(x => x.Priority);
        Assert.Equal(oracle, got);   // Low (1)
    }

    [Fact]
    public async Task GroupBy_noncontiguous_enum_key_roundtrips_and_counts()
    {
        await using var ctx = MakeIntStored();
        var got = (await ctx.Query<Row>()
            .GroupBy(x => x.Priority)
            .Select(g => new { g.Key, C = g.Count() })
            .ToListAsync())
            .ToDictionary(x => x.Key, x => x.C);
        var oracle = Oracle().GroupBy(x => x.Priority).ToDictionary(g => g.Key, g => g.Count());
        Assert.Equal(oracle.Count, got.Count);
        foreach (var kv in oracle)
            Assert.Equal(kv.Value, got[kv.Key]);
    }

    [Fact]
    public async Task GroupBy_combined_flags_value_key_roundtrips_and_counts()
    {
        await using var ctx = MakeIntStored();
        var got = (await ctx.Query<Row>()
            .GroupBy(x => x.Access)
            .Select(g => new { g.Key, C = g.Count() })
            .ToListAsync())
            .ToDictionary(x => x.Key, x => x.C);
        var oracle = Oracle().GroupBy(x => x.Access).ToDictionary(g => g.Key, g => g.Count());
        Assert.Equal(oracle.Count, got.Count);
        foreach (var kv in oracle)
        {
            Assert.True(got.ContainsKey(kv.Key), $"missing group {kv.Key}");
            Assert.Equal(kv.Value, got[kv.Key]);
        }
    }

    [Fact]
    public async Task GroupBy_computed_bitwise_mask_key()
    {
        await using var ctx = MakeIntStored();
        var got = (await ctx.Query<Row>()
            .GroupBy(x => x.Access & Perm.Write)
            .Select(g => new { g.Key, C = g.Count() })
            .ToListAsync())
            .ToDictionary(x => x.Key, x => x.C);
        var oracle = Oracle().GroupBy(x => x.Access & Perm.Write).ToDictionary(g => g.Key, g => g.Count());
        Assert.Equal(oracle.Count, got.Count);
        foreach (var kv in oracle)
        {
            Assert.True(got.ContainsKey(kv.Key), $"missing group {kv.Key}");
            Assert.Equal(kv.Value, got[kv.Key]);
        }
    }

    [Fact]
    public async Task OrderBy_nullable_enum_puts_nulls_first_ascending()
    {
        await using var ctx = MakeIntStored();
        var got = (await ctx.Query<Row>()
            .OrderBy(x => x.NAccess).ThenBy(x => x.Id)
            .Select(x => new { x.Id }).ToListAsync())
            .Select(x => x.Id).ToArray();
        var oracle = Oracle().OrderBy(x => x.NAccess).ThenBy(x => x.Id).Select(x => x.Id).ToArray();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Distinct_noncontiguous_enum_projection()
    {
        await using var ctx = MakeIntStored();
        var got = ctx.Query<Row>().Select(x => x.Priority).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Oracle().Select(x => x.Priority).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle, got);
    }

    // ============ int-stored: bitwise / HasFlag corners ============

    [Fact]
    public async Task OnesComplement_projection_matches_dotnet()
    {
        await using var ctx = MakeIntStored();
        var got = ctx.Query<Row>().OrderBy(x => x.Id).Select(x => ~x.Access).ToList();
        var oracle = Oracle().OrderBy(x => x.Id).Select(x => ~x.Access).ToList();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task HasFlag_None_is_always_true()
    {
        await using var ctx = MakeIntStored();
        var got = (await ctx.Query<Row>().Where(x => x.Access.HasFlag(Perm.None))
            .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().Where(x => x.Access.HasFlag(Perm.None)).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);   // all rows
    }

    [Fact]
    public async Task Nullable_enum_bitwise_notequal_keeps_null_rows()
    {
        await using var ctx = MakeIntStored();
        // .NET: (null & Write) != Write  ->  null != Write -> TRUE, so NULL rows are KEPT.
        var got = (await ctx.Query<Row>().Where(x => (x.NAccess & Perm.Write) != Perm.Write)
            .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().Where(x => (x.NAccess & Perm.Write) != Perm.Write).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Noncontiguous_enum_Contains_binds_underlying_values()
    {
        await using var ctx = MakeIntStored();
        var wanted = new[] { Prio.Low, Prio.High };
        var got = (await ctx.Query<Row>().Where(x => wanted.Contains(x.Priority))
            .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().Where(x => wanted.Contains(x.Priority)).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);
    }

    // ============ STRING-stored [Flags]: bitwise silent-wrong hunt ============

    [Flags]
    public enum SPerm { None = 0, Read = 1, Write = 2, Delete = 4 }

    private sealed class SPermConverter : ValueConverter<SPerm, string>
    {
        public override object? ConvertToProvider(SPerm v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<SPerm>(v);
    }

    [Table("EfahSRow")]
    public class SRow
    {
        [Key] public int Id { get; set; }
        public SPerm Perms { get; set; }
    }

    private static DbContext MakeStringStored()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EfahSRow (Id INTEGER PRIMARY KEY, Perms TEXT NOT NULL);" +
                "INSERT INTO EfahSRow (Id, Perms) VALUES " +
                "(1, 'Read, Write'), (2, 'Read'), (3, 'Write'), (4, 'Delete'), (5, 'None');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SRow>().Property(r => r.Perms).HasConversion(new SPermConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<SRow> SOracle() => new[]
    {
        new SRow { Id = 1, Perms = SPerm.Read | SPerm.Write },
        new SRow { Id = 2, Perms = SPerm.Read },
        new SRow { Id = 3, Perms = SPerm.Write },
        new SRow { Id = 4, Perms = SPerm.Delete },
        new SRow { Id = 5, Perms = SPerm.None },
    };

    [Fact]
    public async Task StringStored_flags_bitwise_and_predicate_matches_or_fails_loud()
    {
        await using var ctx = MakeStringStored();
        var oracle = SOracle().Where(x => (x.Perms & SPerm.Write) == SPerm.Write)
            .Select(x => x.Id).OrderBy(x => x).ToArray();   // {1, 3}
        // A bitwise op on a string-stored flags enum can't be evaluated server-side; it must FAIL LOUD
        // (parity with HasFlag and the bitwise projection), NOT silently match nothing/wrong.
        try
        {
            var got = (await ctx.Query<SRow>().Where(x => (x.Perms & SPerm.Write) == SPerm.Write)
                .Select(x => new { x.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
            Assert.Equal(oracle, got);   // if it ever translates, it must be correct — never silently wrong
        }
        catch (nORM.Core.NormUnsupportedFeatureException)
        {
            // Acceptable: fail-loud parity with HasFlag / bitwise-projection on string-stored flags.
        }
    }

    [Fact]
    public void StringStored_flags_bitwise_and_projection_is_guarded_fail_loud()
    {
        // The PROJECTION path IS guarded (GuardComputedConverterProjection) and fails loud.
        // This documents the correct behavior and contrasts with the un-guarded predicate path below.
        using var ctx = MakeStringStored();
        var ex = Record.Exception(() =>
            ctx.Query<SRow>().OrderBy(x => x.Id).Select(x => x.Perms & SPerm.Write).ToList());
        Assert.IsType<NormUnsupportedFeatureException>(ex);
    }

}
