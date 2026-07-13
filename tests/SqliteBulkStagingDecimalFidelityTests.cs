using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
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
/// SQLite BulkUpdateAsync stages rows in a temp table and UPDATE-joins them back,
/// so the staging column affinity must preserve the bound representation exactly.
/// Decimals bind as canonical TEXT (REAL cannot hold every decimal), but the
/// staging table typed them REAL: a 17+ significant digit decimal was collapsed
/// to the nearest double in staging and the degraded value was silently written
/// back into the main table. Converter columns must also stage by the PROVIDER
/// type — a DateTime stored as BIGINT ticks staged under TEXT date affinity.
/// </summary>
[Trait("Category", "Fast")]
public class SqliteBulkStagingDecimalFidelityTests
{
    [Table("SqliteStageDecimal_Test")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
        public DateTime Expires { get; set; }
    }

    private sealed class TicksConverter : ValueConverter<DateTime, long>
    {
        public override object? ConvertToProvider(DateTime v) => v.Ticks;
        public override object? ConvertFromProvider(long v) => new DateTime(v);
    }

    [Fact]
    public async Task Bulk_update_preserves_wide_decimals_and_converter_columns_exactly()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"norm_stage_dec_{Guid.NewGuid():N}.db");
        try
        {
            await RunAsync(dbPath);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    private static async Task RunAsync(string dbPath)
    {
        await using var cn = new SqliteConnection($"Data Source={dbPath}");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // TEXT affinity for the decimal column: SQLite's NUMERIC affinity converts
            // any well-formed real literal to REAL (a double), so TEXT is the only
            // affinity that stores every .NET decimal exactly — the same mapping EF
            // Core uses for SQLite decimals.
            cmd.CommandText = "CREATE TABLE SqliteStageDecimal_Test (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Expires INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property<DateTime>(p => p.Expires).HasConversion(new TicksConverter())
        };

        var original = new Row
        {
            Id = 1,
            Amount = 0.1234567890123456789m,   // 19 significant digits — beyond double
            Expires = new DateTime(2027, 1, 2, 3, 4, 5).AddTicks(1_234_567),
        };

        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(original);
        await ctx.SaveChangesAsync();

        var updated = new Row
        {
            Id = 1,
            Amount = 9.8765432109876543210m,   // another wide decimal
            Expires = new DateTime(2028, 2, 3, 4, 5, 6).AddTicks(7_654_321),
        };

        var count = await ctx.BulkUpdateAsync(new[] { updated });
        Assert.Equal(1, count);

        var actual = ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Single(r => r.Id == 1);
        Assert.Equal(updated.Amount, actual.Amount);
        Assert.Equal(updated.Expires, actual.Expires);
    }

    [Table("SqliteMigratedDecimal_Test")]
    private class PricedRow
    {
        [Key] public int Id { get; set; }
        public decimal Price { get; set; }
    }

    /// <summary>
    /// The migration map used to type decimals NUMERIC, whose affinity converts any
    /// real literal to REAL — so a migrated table silently collapsed every decimal
    /// beyond double precision at INSERT time. The map now emits TEXT.
    /// </summary>
    [Fact]
    public async Task Migrated_decimal_column_round_trips_wide_values_exactly()
    {
        var diff = new nORM.Migration.SchemaDiff();
        diff.AddedTables.Add(new nORM.Migration.TableSchema
        {
            Name = "SqliteMigratedDecimal_Test",
            Columns = new System.Collections.Generic.List<nORM.Migration.ColumnSchema>
            {
                new nORM.Migration.ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
                new nORM.Migration.ColumnSchema { Name = "Price", ClrType = typeof(decimal).FullName!, IsNullable = false },
            }
        });
        var statements = new nORM.Migration.SqliteMigrationSqlGenerator().GenerateSql(diff);

        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        foreach (var sql in statements.Up)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        var row = new PricedRow { Id = 1, Price = 0.1234567890123456789m };
        await using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        var actual = ((INormQueryable<PricedRow>)ctx.Query<PricedRow>()).AsNoTracking().Single(r => r.Id == 1);
        Assert.Equal(row.Price, actual.Price);
    }
}
