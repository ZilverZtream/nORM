using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// MySQL BulkUpdateAsync stages entity values in a temporary table and then writes
/// them into the destination with UPDATE ... JOIN, so any value the staging column
/// type cannot represent exactly is silently written back degraded. The staging
/// types must therefore preserve every value the destination can hold: DATETIME
/// needs microsecond precision (bare DATETIME rounds fractional seconds away),
/// TIME needs (6), floating point and integral types need real numeric columns
/// (an unmapped fallback coerces through TEXT), and strings/blobs must not be
/// capped at TEXT/BLOB's 64KB. Round-trips each value class through a bulk update
/// against a full-fidelity destination table and compares with the C# value.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class MySqlBulkStagingFidelityLiveTests
{
    [Table("BulkStageFidelity_Test")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public DateTimeOffset Moment { get; set; }
        public TimeSpan Span { get; set; }
        public double Amount { get; set; }
        public float Ratio { get; set; }
        public short Small { get; set; }
        public byte Tiny { get; set; }
        public string Body { get; set; } = string.Empty;
        public byte[] Payload { get; set; } = Array.Empty<byte>();
        public DateTime Expires { get; set; }
    }

    /// <summary>
    /// Stores a DateTime as raw ticks in a BIGINT column. The staging table must be
    /// typed for the PROVIDER value (BIGINT) — typing it by the CLR property
    /// (DATETIME) makes MySQL coerce the ticks integer into a date, which fails.
    /// </summary>
    private sealed class TicksConverter : ValueConverter<DateTime, long>
    {
        public override object? ConvertToProvider(DateTime v) => v.Ticks;
        public override object? ConvertFromProvider(long v) => new DateTime(v);
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenMySql()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
        if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
        var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
        return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
    }

    private static DbConnection Open(Type connectionType, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(connectionType, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Bulk_update_preserves_every_column_value_exactly_on_live_mysql()
    {
        var (factory, provider, skip) = OpenMySql();
        if (skip != null) return;

        Exec(factory!, "DROP TABLE IF EXISTS BulkStageFidelity_Test");
        Exec(factory!, @"CREATE TABLE BulkStageFidelity_Test (
            Id INT PRIMARY KEY,
            Stamp DATETIME(6) NOT NULL,
            Moment DATETIME(6) NOT NULL,
            Span TIME(6) NOT NULL,
            Amount DOUBLE NOT NULL,
            Ratio FLOAT NOT NULL,
            Small SMALLINT NOT NULL,
            Tiny TINYINT UNSIGNED NOT NULL,
            Body LONGTEXT NOT NULL,
            Payload LONGBLOB NOT NULL,
            Expires BIGINT NOT NULL)");
        try
        {
            Exec(factory!, "INSERT INTO BulkStageFidelity_Test VALUES " +
                "(1, '2000-01-01', '2000-01-01', '00:00:00', 0, 0, 0, 0, '', x'00', 0)");

            // .700000 fractional seconds: a DATETIME(0) staging column ROUNDS this
            // UP to the next whole second, so corruption changes even the second.
            var updated = new Row
            {
                Id = 1,
                Stamp = new DateTime(2026, 7, 13, 12, 30, 15).AddTicks(7_004_560),
                Moment = new DateTimeOffset(2026, 7, 13, 12, 30, 15, TimeSpan.Zero).AddTicks(7_004_560),
                Span = new TimeSpan(0, 1, 2, 3, 456).Add(TimeSpan.FromTicks(7_890)),
                Amount = 0.1 + 0.2,               // 0.30000000000000004: needs full double round-trip
                Ratio = 0.1f,
                Small = -12345,
                Tiny = 200,                        // overflows signed TINYINT
                Body = new string('x', 70_000),    // over TEXT's 64KB cap
                Payload = MakePayload(70_000),     // over BLOB's 64KB cap
                Expires = new DateTime(2027, 1, 2, 3, 4, 5).AddTicks(1_234_567),
            };

            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Row>().Property<DateTime>(p => p.Expires).HasConversion(new TicksConverter())
            };

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var count = await ctx.BulkUpdateAsync(new[] { updated });
                Assert.Equal(1, count);
            }

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var actual = ctx.Query<Row>().Single(r => r.Id == 1);
                Assert.Equal(updated.Stamp, actual.Stamp);
                // MySQL has no offset type; the driver stores the UTC instant.
                Assert.Equal(updated.Moment.UtcDateTime, actual.Moment.UtcDateTime);
                Assert.Equal(updated.Span, actual.Span);
                Assert.Equal(updated.Amount, actual.Amount);
                Assert.Equal(updated.Ratio, actual.Ratio);
                Assert.Equal(updated.Small, actual.Small);
                Assert.Equal(updated.Tiny, actual.Tiny);
                Assert.Equal(updated.Body.Length, actual.Body.Length);
                Assert.Equal(updated.Body, actual.Body);
                Assert.Equal(updated.Payload.Length, actual.Payload.Length);
                Assert.Equal(updated.Payload, actual.Payload);
                Assert.Equal(updated.Expires, actual.Expires);
            }

            // Bulk INSERT rides MySqlBulkCopy over a DataTable; its columns must be
            // typed for the converted provider values (BIGINT ticks), not the CLR
            // property (DateTime), or the row load rejects the converted value.
            var inserted = new Row
            {
                Id = 2,
                Stamp = new DateTime(2026, 7, 13, 6, 5, 4).AddTicks(3_210_990),
                Moment = new DateTimeOffset(2026, 7, 13, 6, 5, 4, TimeSpan.Zero).AddTicks(3_210_990),
                Span = new TimeSpan(0, 4, 5, 6, 789),
                Amount = 1.0 / 3.0,
                Ratio = 2.5f,
                Small = 321,
                Tiny = 255,
                Body = "insert",
                Payload = new byte[] { 1, 2, 3 },
                Expires = new DateTime(2028, 2, 3, 4, 5, 6).AddTicks(7_654_321),
            };

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var count = await ctx.BulkInsertAsync(new[] { inserted });
                Assert.Equal(1, count);
            }

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var actual = ctx.Query<Row>().Single(r => r.Id == 2);
                Assert.Equal(inserted.Stamp, actual.Stamp);
                Assert.Equal(inserted.Moment.UtcDateTime, actual.Moment.UtcDateTime);
                Assert.Equal(inserted.Span, actual.Span);
                Assert.Equal(inserted.Amount, actual.Amount);
                Assert.Equal(inserted.Ratio, actual.Ratio);
                Assert.Equal(inserted.Small, actual.Small);
                Assert.Equal(inserted.Tiny, actual.Tiny);
                Assert.Equal(inserted.Body, actual.Body);
                Assert.Equal(inserted.Payload, actual.Payload);
                Assert.Equal(inserted.Expires, actual.Expires);
            }
        }
        finally
        {
            Exec(factory!, "DROP TABLE IF EXISTS BulkStageFidelity_Test");
        }
    }

    private static byte[] MakePayload(int length)
    {
        var bytes = new byte[length];
        for (var i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i % 251);
        return bytes;
    }
}
