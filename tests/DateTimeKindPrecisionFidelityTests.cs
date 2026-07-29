#nullable enable

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

namespace nORM.Tests;

/// <summary>
/// DateTime / DateTimeOffset KIND, sub-second PRECISION and OFFSET fidelity across the write / bulk / read /
/// predicate paths on SQLite (:memory:, TEXT storage). Each test reads the RAW stored TEXT with a plain
/// SqliteCommand AND re-queries via the ORM, asserting exact Ticks / Kind / Offset. Confirms 100ns precision
/// and DateTimeOffset offsets round-trip losslessly and identically on SaveChanges vs BulkInsert, and that
/// DateTime Kind normalizes to Unspecified without instant/wall-clock corruption (matching EF-on-SQLite).
/// </summary>
[Trait("Category", "Fast")]
public class DateTimeKindPrecisionFidelityTests
{
    private readonly ITestOutputHelper _out;
    public DateTimeKindPrecisionFidelityTests(ITestOutputHelper output) => _out = output;

    [Table("DkpRow")]
    public sealed class DkpRow
    {
        [Key] public int Id { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public DateOnly D { get; set; }
        public TimeOnly T { get; set; }
    }

    [Table("DkcRow")]
    public sealed class DkcRow
    {
        [Key] public int Id { get; set; }
        public DateTime TicksDt { get; set; }
    }

    private sealed class DateTimeTicksConverter : ValueConverter<DateTime, long>
    {
        public override object? ConvertToProvider(DateTime value) => value.Ticks;
        public override object? ConvertFromProvider(long value) => new DateTime(value);
    }

    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE DkpRow (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, Dto TEXT NOT NULL, D TEXT NOT NULL, T TEXT NOT NULL);" +
            "CREATE TABLE DkcRow (Id INTEGER PRIMARY KEY, TicksDt INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(),
        new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<DkpRow>().HasKey(x => x.Id);
                mb.Entity<DkcRow>().HasKey(x => x.Id);
                mb.Entity<DkcRow>().Property<DateTime>(x => x.TicksDt).HasConversion(new DateTimeTicksConverter());
            }
        }, ownsConnection: false);

    private static string RawText(SqliteConnection cn, string col, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {col} FROM DkpRow WHERE Id = {id}";
        return (string)cmd.ExecuteScalar()!;
    }

    // ------------------------------------------------------------------
    // 1. DateTime KIND round-trip (Utc / Local / Unspecified)
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTime_kind_roundtrip_documented()
    {
        using var cn = NewDb();
        var wall = new DateTime(2020, 6, 15, 10, 30, 45, DateTimeKind.Unspecified).AddTicks(1234567);
        var utc = DateTime.SpecifyKind(wall, DateTimeKind.Utc);
        var local = DateTime.SpecifyKind(wall, DateTimeKind.Local);
        var unspec = DateTime.SpecifyKind(wall, DateTimeKind.Unspecified);

        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = utc, Dto = default, D = default, T = default });
            ctx.Add(new DkpRow { Id = 2, Dt = local, Dto = default, D = default, T = default });
            ctx.Add(new DkpRow { Id = 3, Dt = unspec, Dto = default, D = default, T = default });
            await ctx.SaveChangesAsync();
        }

        var t1 = RawText(cn, "Dt", 1);
        var t2 = RawText(cn, "Dt", 2);
        var t3 = RawText(cn, "Dt", 3);
        _out.WriteLine($"Utc  raw='{t1}'");
        _out.WriteLine($"Local raw='{t2}'");
        _out.WriteLine($"Unspec raw='{t3}'");

        // Kind is dropped on write (all three normalized to Unspecified before binding) so the same
        // wall-clock stores identical TEXT regardless of Kind. This is a KNOWN, EF-matching normalization.
        Assert.Equal(t1, t2);
        Assert.Equal(t2, t3);

        using var read = Ctx(cn);
        foreach (var id in new[] { 1, 2, 3 })
        {
            var r = read.Query<DkpRow>().Where(x => x.Id == id).ToList().Single();
            _out.WriteLine($"id={id} read Kind={r.Dt.Kind} Ticks={r.Dt.Ticks}");
            // Instant wall-clock ticks preserved exactly; Kind comes back Unspecified for all.
            Assert.Equal(wall.Ticks, r.Dt.Ticks);
            Assert.Equal(DateTimeKind.Unspecified, r.Dt.Kind);
        }
    }

    // ------------------------------------------------------------------
    // 2. DateTimeOffset OFFSET round-trip (non-zero, negative, zero)
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTimeOffset_offset_roundtrip_exact()
    {
        using var cn = NewDb();
        var wall = new DateTime(2020, 6, 15, 10, 30, 45).AddTicks(1234567);
        var plus = new DateTimeOffset(wall, TimeSpan.FromMinutes(330));   // +05:30
        var minus = new DateTimeOffset(wall, TimeSpan.FromHours(-8));      // -08:00
        var zero = new DateTimeOffset(wall, TimeSpan.Zero);               // +00:00

        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = plus, D = default, T = default });
            ctx.Add(new DkpRow { Id = 2, Dt = default, Dto = minus, D = default, T = default });
            ctx.Add(new DkpRow { Id = 3, Dt = default, Dto = zero, D = default, T = default });
            await ctx.SaveChangesAsync();
        }

        _out.WriteLine($"+05:30 raw='{RawText(cn, "Dto", 1)}'");
        _out.WriteLine($"-08:00 raw='{RawText(cn, "Dto", 2)}'");
        _out.WriteLine($"+00:00 raw='{RawText(cn, "Dto", 3)}'");

        using var read = Ctx(cn);
        var r1 = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        var r2 = read.Query<DkpRow>().Where(x => x.Id == 2).ToList().Single();
        var r3 = read.Query<DkpRow>().Where(x => x.Id == 3).ToList().Single();

        Assert.Equal(plus.Offset, r1.Dto.Offset);
        Assert.Equal(plus.Ticks, r1.Dto.Ticks);
        Assert.Equal(plus.UtcTicks, r1.Dto.UtcTicks);

        Assert.Equal(minus.Offset, r2.Dto.Offset);
        Assert.Equal(minus.Ticks, r2.Dto.Ticks);
        Assert.Equal(minus.UtcTicks, r2.Dto.UtcTicks);

        Assert.Equal(zero.Offset, r3.Dto.Offset);
        Assert.Equal(zero.Ticks, r3.Dto.Ticks);
    }

    // ------------------------------------------------------------------
    // 3. Sub-second precision DateTime — SaveChanges
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTime_subsecond_precision_savechanges()
    {
        using var cn = NewDb();
        var val = new DateTime(2023, 11, 5, 23, 59, 59).AddTicks(1234567); // 7-digit fraction
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = val, Dto = default, D = default, T = default });
            await ctx.SaveChangesAsync();
        }
        _out.WriteLine($"SaveChanges raw Dt='{RawText(cn, "Dt", 1)}'");
        using var read = Ctx(cn);
        var r = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(val.Ticks, r.Dt.Ticks);
    }

    // ------------------------------------------------------------------
    // 4. Sub-second precision DateTime — BulkInsert, and PARITY with SaveChanges
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTime_subsecond_precision_bulk_matches_savechanges()
    {
        var val = new DateTime(2023, 11, 5, 23, 59, 59).AddTicks(1234567);

        // SaveChanges path
        using var cnA = NewDb();
        using (var ctx = Ctx(cnA))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = val, Dto = default, D = default, T = default });
            await ctx.SaveChangesAsync();
        }
        var rawSave = RawText(cnA, "Dt", 1);

        // Bulk path
        using var cnB = NewDb();
        using (var ctx = Ctx(cnB))
        {
            await ctx.BulkInsertAsync(new List<DkpRow>
            {
                new DkpRow { Id = 1, Dt = val, Dto = default, D = default, T = default }
            });
        }
        var rawBulk = RawText(cnB, "Dt", 1);

        _out.WriteLine($"SaveChanges raw='{rawSave}'  Bulk raw='{rawBulk}'");
        Assert.Equal(rawSave, rawBulk);   // same stored representation

        using var read = Ctx(cnB);
        var r = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(val.Ticks, r.Dt.Ticks);
    }

    // ------------------------------------------------------------------
    // 5. DateTimeOffset precision + offset — Bulk vs SaveChanges parity
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTimeOffset_bulk_matches_savechanges()
    {
        var wall = new DateTime(2023, 11, 5, 23, 59, 59).AddTicks(1234567);
        var dto = new DateTimeOffset(wall, TimeSpan.FromMinutes(330)); // +05:30

        using var cnA = NewDb();
        using (var ctx = Ctx(cnA))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = dto, D = default, T = default });
            await ctx.SaveChangesAsync();
        }
        var rawSave = RawText(cnA, "Dto", 1);

        using var cnB = NewDb();
        using (var ctx = Ctx(cnB))
        {
            await ctx.BulkInsertAsync(new List<DkpRow>
            {
                new DkpRow { Id = 1, Dt = default, Dto = dto, D = default, T = default }
            });
        }
        var rawBulk = RawText(cnB, "Dto", 1);

        _out.WriteLine($"DTO SaveChanges raw='{rawSave}'  Bulk raw='{rawBulk}'");
        Assert.Equal(rawSave, rawBulk);

        using var read = Ctx(cnB);
        var r = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(dto.Offset, r.Dto.Offset);
        Assert.Equal(dto.Ticks, r.Dto.Ticks);
        Assert.Equal(dto.UtcTicks, r.Dto.UtcTicks);
    }

    // ------------------------------------------------------------------
    // 6. TimeOnly sub-second round-trip (SaveChanges + Bulk)
    // ------------------------------------------------------------------
    [Fact]
    public async Task TimeOnly_subsecond_roundtrip()
    {
        var t = new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(1234567));

        using var cnA = NewDb();
        using (var ctx = Ctx(cnA))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = default, D = default, T = t });
            await ctx.SaveChangesAsync();
        }
        _out.WriteLine($"TimeOnly SaveChanges raw='{RawText(cnA, "T", 1)}'");

        using var cnB = NewDb();
        using (var ctx = Ctx(cnB))
        {
            await ctx.BulkInsertAsync(new List<DkpRow>
            {
                new DkpRow { Id = 1, Dt = default, Dto = default, D = default, T = t }
            });
        }
        _out.WriteLine($"TimeOnly Bulk raw='{RawText(cnB, "T", 1)}'");
        Assert.Equal(RawText(cnA, "T", 1), RawText(cnB, "T", 1));

        using var readA = Ctx(cnA);
        using var readB = Ctx(cnB);
        var ra = readA.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        var rb = readB.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(t.Ticks, ra.T.Ticks);
        Assert.Equal(t.Ticks, rb.T.Ticks);
    }

    // ------------------------------------------------------------------
    // 7. DateOnly round-trip (SaveChanges + Bulk)
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_roundtrip()
    {
        var d = new DateOnly(2024, 2, 29); // leap day

        using var cnA = NewDb();
        using (var ctx = Ctx(cnA))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = default, D = d, T = default });
            await ctx.SaveChangesAsync();
        }
        _out.WriteLine($"DateOnly SaveChanges raw='{RawText(cnA, "D", 1)}'");

        using var cnB = NewDb();
        using (var ctx = Ctx(cnB))
        {
            await ctx.BulkInsertAsync(new List<DkpRow>
            {
                new DkpRow { Id = 1, Dt = default, Dto = default, D = d, T = default }
            });
        }
        _out.WriteLine($"DateOnly Bulk raw='{RawText(cnB, "D", 1)}'");
        Assert.Equal(RawText(cnA, "D", 1), RawText(cnB, "D", 1));

        using var readA = Ctx(cnA);
        var ra = readA.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(d, ra.D);
    }

    // ------------------------------------------------------------------
    // 8. Predicate matching across Kind and sub-second precision
    // ------------------------------------------------------------------
    [Fact]
    public async Task Predicate_matches_across_kind_and_subsecond()
    {
        using var cn = NewDb();
        // Seed a row whose stored value came from a LOCAL-kind DateTime.
        var wall = new DateTime(2021, 3, 20, 8, 15, 30).AddTicks(7654321);
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = DateTime.SpecifyKind(wall, DateTimeKind.Local), Dto = default, D = default, T = default });
            await ctx.SaveChangesAsync();
        }

        using var read = Ctx(cn);
        // Query with a UTC-kind parameter of the same wall-clock: must still match (Kind ignored in storage).
        var utcParam = DateTime.SpecifyKind(wall, DateTimeKind.Utc);
        var matched = read.Query<DkpRow>().Where(x => x.Dt == utcParam).Select(x => x.Id).ToList();
        _out.WriteLine($"equality match count={matched.Count}");
        Assert.Equal(new[] { 1 }, matched);

        // Range predicate preserving sub-second: cutoff 1 tick below stored must include the row.
        var justBelow = wall.AddTicks(-1);
        var rangeMatched = read.Query<DkpRow>().Where(x => x.Dt >= justBelow).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, rangeMatched);

        // cutoff 1 tick above stored must exclude the row (no silent over-match).
        var justAbove = wall.AddTicks(1);
        var overMatched = read.Query<DkpRow>().Where(x => x.Dt >= justAbove).Select(x => x.Id).ToList();
        Assert.Empty(overMatched);
    }

    // ------------------------------------------------------------------
    // 9. DateTimeOffset predicate: same instant, different offset (canonicalization)
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTimeOffset_predicate_same_instant_different_offset()
    {
        using var cn = NewDb();
        var wall = new DateTime(2021, 3, 20, 8, 15, 30).AddTicks(7654321);
        var stored = new DateTimeOffset(wall, TimeSpan.FromMinutes(330)); // +05:30
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = stored, D = default, T = default });
            await ctx.SaveChangesAsync();
        }

        using var read = Ctx(cn);
        // Same instant expressed with a DIFFERENT offset (UTC). DateTimeOffset == compares by instant.
        var sameInstantUtc = stored.ToUniversalTime();
        var matched = read.Query<DkpRow>().Where(x => x.Dto == sameInstantUtc).Select(x => x.Id).ToList();
        _out.WriteLine($"DTO same-instant-different-offset match count={matched.Count}");
        Assert.Equal(new[] { 1 }, matched);
    }

    // ------------------------------------------------------------------
    // 10. DateTime through a value converter (stored as ticks long)
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTime_converter_ticks_roundtrip()
    {
        using var cn = NewDb();
        var val = new DateTime(2023, 11, 5, 23, 59, 59).AddTicks(1234567);
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkcRow { Id = 1, TicksDt = val });
            await ctx.SaveChangesAsync();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT TicksDt FROM DkcRow WHERE Id = 1";
            var stored = (long)cmd.ExecuteScalar()!;
            _out.WriteLine($"converter stored ticks={stored}");
            Assert.Equal(val.Ticks, stored);
        }
        using var read = Ctx(cn);
        var r = read.Query<DkcRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(val.Ticks, r.TicksDt.Ticks);
    }

    // ------------------------------------------------------------------
    // 11. ExecuteUpdate SetProperty with a captured DateTime (kind/precision)
    // ------------------------------------------------------------------
    [Fact]
    public async Task ExecuteUpdate_setproperty_datetime_precision()
    {
        using var cn = NewDb();
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = new DateTime(2000, 1, 1), Dto = default, D = default, T = default });
            await ctx.SaveChangesAsync();
        }

        var newVal = DateTime.SpecifyKind(new DateTime(2024, 7, 1, 6, 30, 0).AddTicks(9876543), DateTimeKind.Utc);
        using (var ctx = Ctx(cn))
        {
            await ctx.Query<DkpRow>().Where(x => x.Id == 1)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Dt, newVal));
        }
        _out.WriteLine($"ExecuteUpdate raw Dt='{RawText(cn, "Dt", 1)}'");

        using var read = Ctx(cn);
        var r = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(newVal.Ticks, r.Dt.Ticks);
    }

    // ------------------------------------------------------------------
    // 12. DateTimeOffset RANGE predicate with mixed offsets (same-instant boundary)
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTimeOffset_range_predicate_mixed_offset()
    {
        using var cn = NewDb();
        // Stored instant = 04:30Z (10:00 at +05:30).
        var stored = new DateTimeOffset(2021, 3, 20, 10, 0, 0, TimeSpan.FromMinutes(330));
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = stored, D = default, T = default });
            await ctx.SaveChangesAsync();
        }

        using var read = Ctx(cn);
        // A LATER instant (05:00Z) expressed in UTC. stored (04:30Z) >= 05:00Z is FALSE.
        var later = new DateTimeOffset(2021, 3, 20, 5, 0, 0, TimeSpan.Zero);
        Assert.Empty(read.Query<DkpRow>().Where(x => x.Dto >= later).Select(x => x.Id).ToList());
        // stored (04:30Z) < 05:00Z is TRUE.
        Assert.Equal(new[] { 1 }, read.Query<DkpRow>().Where(x => x.Dto < later).Select(x => x.Id).ToList());
        // An EARLIER instant (04:00Z) expressed with a different offset: stored >= 04:00Z TRUE.
        var earlier = new DateTimeOffset(2021, 3, 20, 9, 0, 0, TimeSpan.FromHours(5)); // 04:00Z
        Assert.Equal(new[] { 1 }, read.Query<DkpRow>().Where(x => x.Dto >= earlier).Select(x => x.Id).ToList());
    }

    // ------------------------------------------------------------------
    // 13. DateOnly / TimeOnly equality predicates
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateOnly_and_TimeOnly_equality_predicates()
    {
        using var cn = NewDb();
        var d = new DateOnly(2024, 2, 29);
        var t = new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(1234567));
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = default, Dto = default, D = d, T = t });
            await ctx.SaveChangesAsync();
        }
        using var read = Ctx(cn);
        Assert.Equal(new[] { 1 }, read.Query<DkpRow>().Where(x => x.D == d).Select(x => x.Id).ToList());
        Assert.Equal(new[] { 1 }, read.Query<DkpRow>().Where(x => x.T == t).Select(x => x.Id).ToList());
        // A one-tick-different TimeOnly must NOT match (no silent precision collapse).
        var tOff = new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(1234566));
        Assert.Empty(read.Query<DkpRow>().Where(x => x.T == tOff).Select(x => x.Id).ToList());
    }

    // ------------------------------------------------------------------
    // 14. DateTime whole-second value (fractional trimming edge) + predicate
    // ------------------------------------------------------------------
    [Fact]
    public async Task DateTime_whole_second_roundtrip_and_predicate()
    {
        using var cn = NewDb();
        var whole = new DateTime(2022, 1, 1, 12, 0, 0); // zero fraction
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = whole, Dto = default, D = default, T = default });
            await ctx.SaveChangesAsync();
        }
        _out.WriteLine($"whole-second raw='{RawText(cn, "Dt", 1)}'");
        using var read = Ctx(cn);
        var r = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        Assert.Equal(whole.Ticks, r.Dt.Ticks);
        Assert.Equal(new[] { 1 }, read.Query<DkpRow>().Where(x => x.Dt == whole).Select(x => x.Id).ToList());
        // A value 1 tick above must not equality-match.
        Assert.Empty(read.Query<DkpRow>().Where(x => x.Dt == whole.AddTicks(1)).Select(x => x.Id).ToList());
    }

    // ------------------------------------------------------------------
    // 15. DateTimeOffset / DateTime / TimeOnly Min & Max boundaries
    // ------------------------------------------------------------------
    [Fact]
    public async Task Temporal_min_max_boundaries_roundtrip()
    {
        using var cn = NewDb();
        var dtoMax = DateTimeOffset.MaxValue;   // 9999-12-31 23:59:59.9999999 +00:00
        var dtoMin = DateTimeOffset.MinValue;   // 0001-01-01 00:00:00 +00:00
        var tMax = new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(9_999_999)); // 23:59:59.9999999
        using (var ctx = Ctx(cn))
        {
            ctx.Add(new DkpRow { Id = 1, Dt = DateTime.MaxValue, Dto = dtoMax, D = DateOnly.MaxValue, T = tMax });
            ctx.Add(new DkpRow { Id = 2, Dt = DateTime.MinValue, Dto = dtoMin, D = DateOnly.MinValue, T = TimeOnly.MinValue });
            await ctx.SaveChangesAsync();
        }
        _out.WriteLine($"dtoMax raw='{RawText(cn, "Dto", 1)}'  tMax raw='{RawText(cn, "T", 1)}'");
        _out.WriteLine($"dtoMin raw='{RawText(cn, "Dto", 2)}'");
        using var read = Ctx(cn);
        var r1 = read.Query<DkpRow>().Where(x => x.Id == 1).ToList().Single();
        var r2 = read.Query<DkpRow>().Where(x => x.Id == 2).ToList().Single();
        Assert.Equal(DateTime.MaxValue.Ticks, r1.Dt.Ticks);
        Assert.Equal(dtoMax.Ticks, r1.Dto.Ticks);
        Assert.Equal(dtoMax.Offset, r1.Dto.Offset);
        Assert.Equal(tMax.Ticks, r1.T.Ticks);
        Assert.Equal(DateOnly.MaxValue, r1.D);
        Assert.Equal(DateTime.MinValue.Ticks, r2.Dt.Ticks);
        Assert.Equal(dtoMin.Ticks, r2.Dto.Ticks);
        Assert.Equal(TimeOnly.MinValue.Ticks, r2.T.Ticks);
        Assert.Equal(DateOnly.MinValue, r2.D);
    }

    // ------------------------------------------------------------------
    // 16. Nullable temporal round-trip (null AND non-null)
    // ------------------------------------------------------------------
    [Table("DknRow")]
    public sealed class DknRow
    {
        [Key] public int Id { get; set; }
        public DateTime? Dt { get; set; }
        public DateTimeOffset? Dto { get; set; }
        public TimeOnly? T { get; set; }
    }

    [Fact]
    public async Task Nullable_temporal_roundtrip()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DknRow (Id INTEGER PRIMARY KEY, Dt TEXT NULL, Dto TEXT NULL, T TEXT NULL);";
            cmd.ExecuteNonQuery();
        }
        try
        {
            DbContext Make() => new DbContext(cn, new SqliteProvider(),
                new DbContextOptions { OnModelCreating = mb => mb.Entity<DknRow>().HasKey(x => x.Id) }, ownsConnection: false);

            var wall = new DateTime(2023, 7, 4, 1, 2, 3).AddTicks(1112223);
            using (var ctx = Make())
            {
                ctx.Add(new DknRow { Id = 1, Dt = wall, Dto = new DateTimeOffset(wall, TimeSpan.FromHours(-8)), T = new TimeOnly(1, 2, 3).Add(TimeSpan.FromTicks(1112223)) });
                ctx.Add(new DknRow { Id = 2, Dt = null, Dto = null, T = null });
                await ctx.SaveChangesAsync();
            }
            using var read = Make();
            var r1 = read.Query<DknRow>().Where(x => x.Id == 1).ToList().Single();
            var r2 = read.Query<DknRow>().Where(x => x.Id == 2).ToList().Single();
            Assert.Equal(wall.Ticks, r1.Dt!.Value.Ticks);
            Assert.Equal(TimeSpan.FromHours(-8), r1.Dto!.Value.Offset);
            Assert.Equal(wall.Ticks, r1.Dto!.Value.Ticks);
            Assert.Null(r2.Dt);
            Assert.Null(r2.Dto);
            Assert.Null(r2.T);
        }
        finally
        {
            cn.Close();
            cn.Dispose();
        }
    }
}
