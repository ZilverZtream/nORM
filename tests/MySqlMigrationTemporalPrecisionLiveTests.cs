using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// MySQL's bare DATETIME and TIME column types hold whole seconds only and ROUND
/// fractional seconds on write, so a migration-created table silently changed
/// every stored sub-second instant (12:30:15.7 became 12:30:16). The migration
/// type map must emit DATETIME(6)/TIME(6) — the precision the query layer already
/// assumes — and TINYINT UNSIGNED for byte (signed TINYINT overflows above 127).
/// Executes the generator's actual DDL on a live server and round-trips values
/// through a regular insert.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class MySqlMigrationTemporalPrecisionLiveTests
{
    [Table("MigTemporalFidelity_Test")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public DateTime At { get; set; }
        public DateTimeOffset Moment { get; set; }
        public TimeSpan Dur { get; set; }
        public TimeOnly Start { get; set; }
        public byte Tiny { get; set; }
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

    private static SchemaDiff MakeTableDiff(string? defaultForAt = null)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(new TableSchema
        {
            Name = "MigTemporalFidelity_Test",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
                new ColumnSchema { Name = "At", ClrType = typeof(DateTime).FullName!, IsNullable = false, DefaultValue = defaultForAt },
                new ColumnSchema { Name = "Moment", ClrType = typeof(DateTimeOffset).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Dur", ClrType = typeof(TimeSpan).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Start", ClrType = typeof(TimeOnly).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Tiny", ClrType = typeof(byte).FullName!, IsNullable = false },
            }
        });
        return diff;
    }

    [Fact]
    public async Task Migrated_table_preserves_fractional_seconds_and_full_byte_range_on_live_mysql()
    {
        var (factory, provider, skip) = OpenMySql();
        if (skip != null) return;

        var statements = new MySqlMigrationSqlGenerator().GenerateSql(MakeTableDiff());

        Exec(factory!, "DROP TABLE IF EXISTS MigTemporalFidelity_Test");
        try
        {
            foreach (var sql in statements.Up)
                Exec(factory!, sql);

            var row = new Row
            {
                Id = 1,
                At = new DateTime(2026, 7, 13, 12, 30, 15).AddTicks(7_004_560),
                Moment = new DateTimeOffset(2026, 7, 13, 12, 30, 15, TimeSpan.Zero).AddTicks(7_004_560),
                Dur = new TimeSpan(0, 1, 2, 3, 456).Add(TimeSpan.FromTicks(7_890)),
                Start = new TimeOnly(23, 58, 59).Add(TimeSpan.FromMilliseconds(987)),
                Tiny = 200,
            };

            await using (var ctx = new DbContext(factory!(), provider!))
            {
                ctx.Add(row);
                await ctx.SaveChangesAsync();
            }

            await using (var ctx = new DbContext(factory!(), provider!))
            {
                var actual = ctx.Query<Row>().Single(r => r.Id == 1);
                Assert.Equal(row.At, actual.At);
                Assert.Equal(row.Moment.UtcDateTime, actual.Moment.UtcDateTime);
                Assert.Equal(row.Dur, actual.Dur);
                Assert.Equal(row.Start, actual.Start);
                Assert.Equal(row.Tiny, actual.Tiny);
            }
        }
        finally
        {
            Exec(factory!, "DROP TABLE IF EXISTS MigTemporalFidelity_Test");
        }
    }

    [Fact]
    public void Temporal_default_still_executes_against_datetime6_column_on_live_mysql()
    {
        var (factory, _, skip) = OpenMySql();
        if (skip != null) return;

        // MySQL requires a temporal default's fractional precision to match the
        // column's, so the generated DDL must remain executable for the common
        // CURRENT_TIMESTAMP default now that DateTime columns are DATETIME(6).
        var statements = new MySqlMigrationSqlGenerator().GenerateSql(MakeTableDiff("CURRENT_TIMESTAMP"));

        Exec(factory!, "DROP TABLE IF EXISTS MigTemporalFidelity_Test");
        try
        {
            foreach (var sql in statements.Up)
                Exec(factory!, sql);
        }
        finally
        {
            Exec(factory!, "DROP TABLE IF EXISTS MigTemporalFidelity_Test");
        }
    }
}
