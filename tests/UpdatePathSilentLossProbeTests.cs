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
/// Adversarial probes over the UPDATE write path (SaveChanges partial-column updates,
/// ExecuteUpdate, converter / nullable / temporal / decimal columns) on SQLite. Each test writes,
/// then reads the RAW stored value back with a raw SqliteCommand and diffs it against what .NET/EF
/// would persist. Hunting for silent data loss: a persisted WRONG value, a dropped changed column,
/// or a write to the wrong row without throwing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class UpdatePathSilentLossProbeTests
{
    public enum Status { Active = 1, Inactive = 2, Archived = 3 }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    // ---------- helpers ----------

    private static object? RawScalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static void RawExec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // =====================================================================================
    // Cluster A — converter columns, tracked SaveChanges partial update
    // =====================================================================================

    [Table("A_Row")]
    public class ARow
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }          // NegatingConverter
        public Status Status { get; set; }       // EnumToNameConverter
        public Status? NStatus { get; set; }     // nullable EnumToNameConverter
    }

    private static DbContext CreateA(SqliteConnection cn)
    {
        RawExec(cn, "CREATE TABLE A_Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Score INTEGER NOT NULL, Status TEXT NOT NULL, NStatus TEXT NULL)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ARow>().Property<int>(p => p.Score).HasConversion(new NegatingConverter());
                mb.Entity<ARow>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter());
                mb.Entity<ARow>().Property<Status?>(p => p.NStatus).HasConversion(new EnumToNameConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Tracked_update_converter_columns_write_provider_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateA(cn);
        var r = new ARow { Name = "a", Score = 10, Status = Status.Active, NStatus = Status.Inactive };
        ctx.Add(r); await ctx.SaveChangesAsync();

        r.Score = 42;
        r.Status = Status.Archived;
        await ctx.SaveChangesAsync();

        Assert.Equal(-42L, Convert.ToInt64(RawScalar(cn, $"SELECT Score FROM A_Row WHERE Id={r.Id}")));
        Assert.Equal("Archived", (string)RawScalar(cn, $"SELECT Status FROM A_Row WHERE Id={r.Id}")!);
    }

    [Fact]
    public async Task Tracked_update_nullable_converter_column_to_null_writes_db_null()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateA(cn);
        var r = new ARow { Name = "a", Score = 1, Status = Status.Active, NStatus = Status.Inactive };
        ctx.Add(r); await ctx.SaveChangesAsync();
        Assert.Equal("Inactive", (string)RawScalar(cn, $"SELECT NStatus FROM A_Row WHERE Id={r.Id}")!);

        r.NStatus = null;
        await ctx.SaveChangesAsync();

        Assert.True(RawScalar(cn, $"SELECT NStatus FROM A_Row WHERE Id={r.Id}") is DBNull,
            "nullable converter column set to null must store SQL NULL");
    }

    [Fact]
    public async Task Tracked_update_nullable_converter_column_null_to_value_writes_provider_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateA(cn);
        var r = new ARow { Name = "a", Score = 1, Status = Status.Active, NStatus = null };
        ctx.Add(r); await ctx.SaveChangesAsync();
        Assert.True(RawScalar(cn, $"SELECT NStatus FROM A_Row WHERE Id={r.Id}") is DBNull);

        r.NStatus = Status.Archived;
        await ctx.SaveChangesAsync();

        Assert.Equal("Archived", (string)RawScalar(cn, $"SELECT NStatus FROM A_Row WHERE Id={r.Id}")!);
    }

    // =====================================================================================
    // Cluster C — partial-column selection: no clobber of a concurrently-different column
    // =====================================================================================

    [Table("C_Row")]
    public class CRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
    }

    private static DbContext CreateC(SqliteConnection cn)
    {
        RawExec(cn, "CREATE TABLE C_Row (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL)");
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<CRow>().HasKey(x => x.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Partial_update_does_not_clobber_concurrently_changed_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateC(cn);
        ctx.Add(new CRow { Id = 1, A = 1, B = 2, C = 3 });
        await ctx.SaveChangesAsync();

        // Load into a fresh tracked entity (snapshot A=1,B=2,C=3).
        var e = ctx.Query<CRow>().First(x => x.Id == 1);

        // Simulate a concurrent writer changing a DIFFERENT column.
        RawExec(cn, "UPDATE C_Row SET B = 999 WHERE Id = 1");

        // Change only A on the tracked entity.
        e.A = 10;
        await ctx.SaveChangesAsync();

        Assert.Equal(10L, Convert.ToInt64(RawScalar(cn, "SELECT A FROM C_Row WHERE Id=1")));   // written
        Assert.Equal(999L, Convert.ToInt64(RawScalar(cn, "SELECT B FROM C_Row WHERE Id=1")));  // NOT clobbered
        Assert.Equal(3L, Convert.ToInt64(RawScalar(cn, "SELECT C FROM C_Row WHERE Id=1")));    // untouched
    }

    [Fact]
    public async Task Revert_to_original_emits_no_update()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateC(cn);
        ctx.Add(new CRow { Id = 1, A = 1, B = 2, C = 3 });
        await ctx.SaveChangesAsync();

        var e = ctx.Query<CRow>().First(x => x.Id == 1);
        RawExec(cn, "UPDATE C_Row SET B = 999 WHERE Id = 1");

        e.A = 10;
        e.A = 1;   // reverted -> net no-op
        await ctx.SaveChangesAsync();

        Assert.Equal(1L, Convert.ToInt64(RawScalar(cn, "SELECT A FROM C_Row WHERE Id=1")));
        Assert.Equal(999L, Convert.ToInt64(RawScalar(cn, "SELECT B FROM C_Row WHERE Id=1"))); // no full update
    }

    // =====================================================================================
    // Cluster B — nullable plain columns
    // =====================================================================================

    [Table("B_Row")]
    public class BRow
    {
        [Key] public int Id { get; set; }
        public int? N { get; set; }
        public string? S { get; set; }
    }

    private static DbContext CreateB(SqliteConnection cn)
    {
        RawExec(cn, "CREATE TABLE B_Row (Id INTEGER PRIMARY KEY, N INTEGER NULL, S TEXT NULL)");
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<BRow>().HasKey(x => x.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Tracked_update_value_to_null_and_null_to_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateB(cn);
        ctx.Add(new BRow { Id = 1, N = 5, S = "hi" });
        await ctx.SaveChangesAsync();

        var e = ctx.Query<BRow>().First(x => x.Id == 1);
        e.N = null; e.S = null;
        await ctx.SaveChangesAsync();
        Assert.True(RawScalar(cn, "SELECT N FROM B_Row WHERE Id=1") is DBNull);
        Assert.True(RawScalar(cn, "SELECT S FROM B_Row WHERE Id=1") is DBNull);

        e.N = 7; e.S = "yo";
        await ctx.SaveChangesAsync();
        Assert.Equal(7L, Convert.ToInt64(RawScalar(cn, "SELECT N FROM B_Row WHERE Id=1")));
        Assert.Equal("yo", (string)RawScalar(cn, "SELECT S FROM B_Row WHERE Id=1")!);
    }

    // =====================================================================================
    // Cluster D — decimal / temporal precision on the tracked UPDATE path
    // =====================================================================================

    [Table("D_Row")]
    public class DRow
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
        public DateTime Dt { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Ts { get; set; }
        public TimeOnly To { get; set; }
        public DateOnly Do { get; set; }
    }

    private static DbContext CreateD(SqliteConnection cn)
    {
        RawExec(cn, "CREATE TABLE D_Row (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Dt TEXT NOT NULL, Dto TEXT NOT NULL, Ts TEXT NOT NULL, [To] TEXT NOT NULL, [Do] TEXT NOT NULL)");
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<DRow>().HasKey(x => x.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Tracked_update_preserves_decimal_and_temporal_precision()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateD(cn);
        ctx.Add(new DRow
        {
            Id = 1,
            Amount = 1m,
            Dt = new DateTime(2000, 1, 1),
            Dto = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero),
            Ts = TimeSpan.Zero,
            To = new TimeOnly(0, 0),
            Do = new DateOnly(2000, 1, 1)
        });
        await ctx.SaveChangesAsync();

        var e = ctx.Query<DRow>().First(x => x.Id == 1);
        var amount = 0.1234567890123456789m;
        var dt = new DateTime(2026, 3, 4, 5, 6, 7).AddTicks(1234567);
        var dto = new DateTimeOffset(2026, 3, 4, 5, 6, 7, 89, TimeSpan.FromHours(5.5)).AddTicks(1234);
        var ts = new TimeSpan(1, 2, 3, 4, 5).Add(TimeSpan.FromTicks(6789));
        var to = new TimeOnly(13, 14, 15, 16).Add(TimeSpan.FromTicks(7890));
        var dateOnly = new DateOnly(2026, 12, 31);

        e.Amount = amount; e.Dt = dt; e.Dto = dto; e.Ts = ts; e.To = to; e.Do = dateOnly;
        await ctx.SaveChangesAsync();

        // Read back through the ORM (round-trip) AND raw text for visibility.
        var reread = ctx.Query<DRow>().AsNoTracking().First(x => x.Id == 1);
        Assert.Equal(amount, reread.Amount);
        Assert.Equal(dt, reread.Dt);
        Assert.Equal(dto, reread.Dto);
        Assert.Equal(dto.Offset, reread.Dto.Offset);   // offset preserved, not normalized to UTC
        Assert.Equal(ts, reread.Ts);
        Assert.Equal(to, reread.To);
        Assert.Equal(dateOnly, reread.Do);
    }

    // =====================================================================================
    // Cluster E — ExecuteUpdate (set-based)
    // =====================================================================================

    [Fact]
    public async Task ExecuteUpdate_where_on_converter_column_targets_correct_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateA(cn);
        ctx.Add(new ARow { Name = "a", Score = 1, Status = Status.Active });
        ctx.Add(new ARow { Name = "b", Score = 1, Status = Status.Archived });
        ctx.Add(new ARow { Name = "c", Score = 1, Status = Status.Archived });
        await ctx.SaveChangesAsync();

        var affected = await ctx.Query<ARow>().Where(r => r.Status == Status.Archived)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Name, "X"));

        Assert.Equal(2, affected); // only the two Archived rows
        Assert.Equal(2L, Convert.ToInt64(RawScalar(cn, "SELECT COUNT(*) FROM A_Row WHERE Name='X'")));
        Assert.Equal(1L, Convert.ToInt64(RawScalar(cn, "SELECT COUNT(*) FROM A_Row WHERE Name='a'")));
    }

    [Fact]
    public async Task ExecuteUpdate_set_nullable_to_null_literal_and_captured()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateB(cn);
        ctx.Add(new BRow { Id = 1, N = 5, S = "hi" });
        ctx.Add(new BRow { Id = 2, N = 7, S = "yo" });
        await ctx.SaveChangesAsync();

        // literal null
        await ctx.Query<BRow>().Where(x => x.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.N, (int?)null).SetProperty(x => x.S, (string?)null));
        Assert.True(RawScalar(cn, "SELECT N FROM B_Row WHERE Id=1") is DBNull);
        Assert.True(RawScalar(cn, "SELECT S FROM B_Row WHERE Id=1") is DBNull);

        // captured null
        int? capturedN = null; string? capturedS = null;
        await ctx.Query<BRow>().Where(x => x.Id == 2)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.N, capturedN).SetProperty(x => x.S, capturedS));
        Assert.True(RawScalar(cn, "SELECT N FROM B_Row WHERE Id=2") is DBNull);
        Assert.True(RawScalar(cn, "SELECT S FROM B_Row WHERE Id=2") is DBNull);
    }

    [Fact]
    public async Task ExecuteUpdate_where_on_nullable_is_null()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateB(cn);
        ctx.Add(new BRow { Id = 1, N = null, S = "x" });
        ctx.Add(new BRow { Id = 2, N = 5, S = "y" });
        await ctx.SaveChangesAsync();

        var affected = await ctx.Query<BRow>().Where(x => x.N == null)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.S, "hit"));
        Assert.Equal(1, affected);
        Assert.Equal("hit", (string)RawScalar(cn, "SELECT S FROM B_Row WHERE Id=1")!);
        Assert.Equal("y", (string)RawScalar(cn, "SELECT S FROM B_Row WHERE Id=2")!);
    }

    [Fact]
    public async Task ExecuteUpdate_decimal_literal_precision()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateD(cn);
        ctx.Add(new DRow
        {
            Id = 1, Amount = 1m, Dt = new DateTime(2000,1,1),
            Dto = default, Ts = TimeSpan.Zero, To = new TimeOnly(0,0), Do = new DateOnly(2000,1,1)
        });
        await ctx.SaveChangesAsync();

        var amount = 12345.678901234567m;
        await ctx.Query<DRow>().Where(x => x.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Amount, amount));

        var reread = ctx.Query<DRow>().AsNoTracking().First(x => x.Id == 1);
        Assert.Equal(amount, reread.Amount);
    }

    // =====================================================================================
    // Cluster G — DateTimeOffset offset-only change (same instant, different offset)
    // DateTimeOffset.Equals compares ONLY the instant and ignores the offset. If dirty
    // detection uses .Equals, changing just the offset (a meaningful data change, since the
    // offset is stored) is silently dropped.
    // =====================================================================================

    [Table("G_Row")]
    public class GRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset When { get; set; }
    }

    private static DbContext CreateG(SqliteConnection cn)
    {
        RawExec(cn, "CREATE TABLE G_Row (Id INTEGER PRIMARY KEY, [When] TEXT NOT NULL)");
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<GRow>().HasKey(x => x.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Tracked_update_offset_only_change_is_persisted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateG(cn);
        // 12:00 +05:00  ==  07:00 +00:00 as an INSTANT, but the stored offset differs.
        ctx.Add(new GRow { Id = 1, When = new DateTimeOffset(2000, 1, 1, 12, 0, 0, TimeSpan.FromHours(5)) });
        await ctx.SaveChangesAsync();

        var e = ctx.Query<GRow>().First(x => x.Id == 1);
        // Same instant, different offset. The user wants the stored offset to become +00:00.
        var wanted = new DateTimeOffset(2000, 1, 1, 7, 0, 0, TimeSpan.Zero);
        Assert.True(e.When.Equals(wanted));            // same instant per .Equals
        Assert.NotEqual(e.When.Offset, wanted.Offset); // different offset (data really changed)

        e.When = wanted;
        await ctx.SaveChangesAsync();

        var reread = ctx.Query<GRow>().AsNoTracking().First(x => x.Id == 1);
        Assert.Equal(TimeSpan.Zero, reread.When.Offset); // BUG if still +05:00: offset change silently dropped
    }

    [Fact]
    public async Task Forced_update_offset_only_change_is_persisted_contrast()
    {
        // Contrast: ctx.Update() forces a full-column write (bypasses change detection),
        // proving the write/binding path CAN persist the offset — the loss above is purely
        // a dirty-detection blind spot (DateTimeOffset.Equals ignores offset).
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateG(cn);
        ctx.Add(new GRow { Id = 1, When = new DateTimeOffset(2000, 1, 1, 12, 0, 0, TimeSpan.FromHours(5)) });
        await ctx.SaveChangesAsync();

        var e = ctx.Query<GRow>().First(x => x.Id == 1);
        e.When = new DateTimeOffset(2000, 1, 1, 7, 0, 0, TimeSpan.Zero);
        ctx.Update(e);                       // force full update
        await ctx.SaveChangesAsync();

        var reread = ctx.Query<GRow>().AsNoTracking().First(x => x.Id == 1);
        Assert.Equal(TimeSpan.Zero, reread.When.Offset);
    }

    // =====================================================================================
    // Cluster F — client-managed OCC token + partial update
    // =====================================================================================

    [Table("F_Row")]
    public class FRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        [Timestamp] public int Version { get; set; }
    }

    private static DbContext CreateF(SqliteConnection cn)
    {
        RawExec(cn, "CREATE TABLE F_Row (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Version INTEGER NOT NULL)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<FRow>().HasKey(x => x.Id);
                mb.Entity<FRow>().Property<int>(x => x.Version).IsRowVersion();
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Occ_partial_update_advances_token_and_preserves_other_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = CreateF(cn);
        ctx.Add(new FRow { Id = 1, A = 1, B = 2, Version = 1 });
        await ctx.SaveChangesAsync();

        var e = ctx.Query<FRow>().First(x => x.Id == 1);
        var v0 = e.Version;

        // concurrent writer changes B underneath (but NOT the token — nORM manages the token)
        RawExec(cn, "UPDATE F_Row SET B = 888 WHERE Id = 1");

        e.A = 10;
        await ctx.SaveChangesAsync();

        Assert.Equal(10L, Convert.ToInt64(RawScalar(cn, "SELECT A FROM F_Row WHERE Id=1")));
        Assert.Equal(888L, Convert.ToInt64(RawScalar(cn, "SELECT B FROM F_Row WHERE Id=1"))); // preserved
        var v1 = Convert.ToInt64(RawScalar(cn, "SELECT Version FROM F_Row WHERE Id=1"));
        Assert.NotEqual(v0, (int)v1); // token advanced
    }
}
