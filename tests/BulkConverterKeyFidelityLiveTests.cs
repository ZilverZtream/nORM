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
/// Bulk write paths must bind converter columns' PROVIDER values everywhere a
/// value or key parameter is created. The shared batched fallback (used with
/// UseBatchedBulkOps and by the providers' concurrency-token routes) bound raw
/// model values: converter VALUE columns were silently persisted wrong, and
/// converter KEY columns matched no rows, so bulk updates and deletes silently
/// affected zero rows. Postgres' native `= ANY(keys)` bulk delete had the same
/// key gap. A negating int converter (model v stored as -v) makes every miss
/// visible.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class BulkConverterKeyFidelityLiveTests
{
    [Table("BulkConvKey_Test")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
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

    private static DbContextOptions ConverterOptions(bool useBatched) => new()
    {
        UseBatchedBulkOps = useBatched,
        OnModelCreating = mb =>
        {
            var e = mb.Entity<Row>();
            e.Property<int>(p => p.Id).HasConversion(new NegatingConverter());
            e.Property<int>(p => p.Val).HasConversion(new NegatingConverter());
        }
    };

    [Fact]
    public async Task Batched_fallback_updates_and_deletes_converter_keyed_rows_on_live_sqlserver()
    {
        var (factory, provider, skip) = OpenLive("sqlserver");
        if (skip != null) return;

        Exec(factory!, "IF OBJECT_ID('BulkConvKey_Test') IS NOT NULL DROP TABLE BulkConvKey_Test");
        Exec(factory!, "CREATE TABLE BulkConvKey_Test (Id INT PRIMARY KEY, Val INT NOT NULL)");
        try
        {
            // Stored via converter: Id -5, Val -10.
            Exec(factory!, "INSERT INTO BulkConvKey_Test VALUES (-5, -10)");
            var opts = ConverterOptions(useBatched: true);

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var updatedCount = await ctx.BulkUpdateAsync(new[] { new Row { Id = 5, Val = 20 } });
                Assert.Equal(1, updatedCount);
            }

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var actual = ctx.Query<Row>().Single(r => r.Id == 5);
                Assert.Equal(20, actual.Val);
            }

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var deletedCount = await ctx.BulkDeleteAsync(new[] { new Row { Id = 5, Val = 20 } });
                Assert.Equal(1, deletedCount);
                Assert.Empty(ctx.Query<Row>().ToList());
            }
        }
        finally
        {
            Exec(factory!, "IF OBJECT_ID('BulkConvKey_Test') IS NOT NULL DROP TABLE BulkConvKey_Test");
        }
    }

    [Fact]
    public async Task Native_bulk_delete_matches_converter_keys_on_live_postgres()
    {
        var (factory, provider, skip) = OpenLive("postgres");
        if (skip != null) return;

        Exec(factory!, "DROP TABLE IF EXISTS \"BulkConvKey_Test\"");
        Exec(factory!, "CREATE TABLE \"BulkConvKey_Test\" (\"Id\" INT PRIMARY KEY, \"Val\" INT NOT NULL)");
        try
        {
            Exec(factory!, "INSERT INTO \"BulkConvKey_Test\" VALUES (-5, -10), (-6, -12)");
            var opts = ConverterOptions(useBatched: false);

            await using (var ctx = new DbContext(factory!(), provider!, opts))
            {
                var deletedCount = await ctx.BulkDeleteAsync(new[]
                {
                    new Row { Id = 5, Val = 10 },
                    new Row { Id = 6, Val = 12 },
                });
                Assert.Equal(2, deletedCount);
                Assert.Empty(ctx.Query<Row>().ToList());
            }
        }
        finally
        {
            Exec(factory!, "DROP TABLE IF EXISTS \"BulkConvKey_Test\"");
        }
    }
}
