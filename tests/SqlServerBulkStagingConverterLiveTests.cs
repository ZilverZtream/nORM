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
/// SQL Server BulkUpdateAsync stages rows into a temp table via SqlBulkCopy, and
/// the EntityDataReader supplies CONVERTED provider values — so the staging
/// column must be typed for the converter's provider representation. Typing it
/// by the CLR property (DATETIME2 for a DateTime stored as BIGINT ticks) makes
/// SqlBulkCopy reject the converted value and the whole bulk update fails.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class SqlServerBulkStagingConverterLiveTests
{
    [Table("BulkStageConverter_Test")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public DateTime Expires { get; set; }
    }

    private sealed class TicksConverter : ValueConverter<DateTime, long>
    {
        public override object? ConvertToProvider(DateTime v) => v.Ticks;
        public override object? ConvertFromProvider(long v) => new DateTime(v);
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenSqlServer()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
        var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
        return (() => Open(t, cs), new SqlServerProvider(), null);
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
    public async Task Bulk_update_stages_converter_columns_by_provider_type_on_live_sqlserver()
    {
        var (factory, provider, skip) = OpenSqlServer();
        if (skip != null) return;

        Exec(factory!, "IF OBJECT_ID('BulkStageConverter_Test') IS NOT NULL DROP TABLE BulkStageConverter_Test");
        Exec(factory!, @"CREATE TABLE BulkStageConverter_Test (
            Id INT PRIMARY KEY,
            Stamp DATETIME2 NOT NULL,
            Expires BIGINT NOT NULL)");
        try
        {
            Exec(factory!, "INSERT INTO BulkStageConverter_Test VALUES (1, '2000-01-01', 0)");

            var updated = new Row
            {
                Id = 1,
                Stamp = new DateTime(2026, 7, 13, 12, 30, 15).AddTicks(7_004_561),
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
                Assert.Equal(updated.Expires, actual.Expires);
            }
        }
        finally
        {
            Exec(factory!, "IF OBJECT_ID('BulkStageConverter_Test') IS NOT NULL DROP TABLE BulkStageConverter_Test");
        }
    }
}
