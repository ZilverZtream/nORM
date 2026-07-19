using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Provider-native (SQL Server system-versioned) reconstruction of an OWNED (OwnsMany) collection shaped into
/// a projection under AsOf, on the TRUE-ASYNC path. This is the one combination SQLite can't cover: SQLite
/// routes ToListAsync through the sync loader and uses nORM-managed history windows, whereas here the async
/// owned loader (<c>LoadOwnedCollectionProjectionAsync</c>) reads through the <c>FOR SYSTEM_TIME AS OF</c>
/// branch of <c>BuildOwnedProjectionFromSource</c> against a real system-versioned owned table. The relation
/// path is covered by <see cref="LiveProviderNativeShapedCollectionTemporalTests"/>. Skips unless
/// NORM_TEST_SQLSERVER is configured.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderNativeOwnedShapedProjectionTemporalTests
{
    [Table("PntOwnOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    // No [Table]: OwnsMany(tableName: ...) must reach the owned type's own mapping for the temporal bootstrap.
    public class Line { [Key] public int Id { get; set; } public int Val { get; set; } }

    public class LineDto { public int Val { get; set; } }

    private static (Func<DbConnection>?, string?) OpenLive()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_SQLSERVER not set");
        var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
        Func<string, DbConnection> open = connectionString =>
        {
            var cn = (DbConnection)Activator.CreateInstance(t, connectionString)!;
            cn.Open();
            return cn;
        };

        // Temporal bootstrap is refused against the provider-owned 'master' DB; run in an application database.
        var builderType = Type.GetType("Microsoft.Data.SqlClient.SqlConnectionStringBuilder, Microsoft.Data.SqlClient")!;
        var builder = Activator.CreateInstance(builderType, cs)!;
        var catalogProp = builderType.GetProperty("InitialCatalog")!;
        var current = catalogProp.GetValue(builder) as string;
        if (string.IsNullOrEmpty(current) || string.Equals(current, "master", StringComparison.OrdinalIgnoreCase))
        {
            using (var master = open(cs))
            using (var cmd = master.CreateCommand())
            {
                cmd.CommandText = "IF DB_ID(N'normtest') IS NULL CREATE DATABASE normtest;";
                cmd.ExecuteNonQuery();
            }
            catalogProp.SetValue(builder, "normtest");
        }
        var appCs = builder.ToString()!;
        return (() => open(appCs), null);
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static DateTime ScalarUtc(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return DateTime.SpecifyKind(Convert.ToDateTime(cmd.ExecuteScalar(), CultureInfo.InvariantCulture), DateTimeKind.Utc);
    }

    private static void DropVersioned(Func<DbConnection> factory, string table)
    {
        Exec(factory, $@"
IF OBJECT_ID(N'{table}') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'{table}') AND temporal_type = 2)
    ALTER TABLE {table} SET (SYSTEM_VERSIONING = OFF);
IF OBJECT_ID(N'{table}') IS NOT NULL DROP TABLE {table};
IF OBJECT_ID(N'{table}_SystemHistory') IS NOT NULL DROP TABLE {table}_SystemHistory;");
    }

    private static void ResetSchema(Func<DbConnection> factory)
    {
        DropVersioned(factory, "PntOwnLine");
        DropVersioned(factory, "PntOwnOrder");
        // Plain base tables; nORM's provider-native bootstrap ALTERs them to add system-versioning.
        Exec(factory, "CREATE TABLE PntOwnOrder (Id INT PRIMARY KEY)");
        Exec(factory, "CREATE TABLE PntOwnLine (Id INT PRIMARY KEY, OrderId INT NOT NULL, Val INT NOT NULL)");
    }

    private static DbContext NewContext(Func<DbConnection> factory)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "PntOwnLine", foreignKey: "OrderId")
        };
        opts.EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
        return new DbContext(factory(), new SqlServerProvider(), opts);
    }

    [Fact]
    public async Task Owned_shaped_projection_reconstructs_system_versioned_era_on_async_path()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;
        ResetSchema(factory!);
        try
        {
            using (var ctx = NewContext(factory!))   // first connection bootstraps system-versioning on both tables
            {
                var order = new Order { Id = 1 };
                order.Lines.Add(new Line { Id = 1, Val = 10 });
                order.Lines.Add(new Line { Id = 2, Val = 20 });
                ctx.Add(order);
                await ctx.SaveChangesAsync();
            }
            await Task.Delay(50);
            var t1 = ScalarUtc(factory!, "SELECT SYSUTCDATETIME()");   // era-1: owned lines were {10, 20}
            await Task.Delay(50);
            // Era-2 via a raw UPDATE: system-versioning captures the pre-image into the history table
            // regardless of the write path, so this targets the AsOf READ reconstruction precisely.
            Exec(factory!, "UPDATE PntOwnLine SET Val = 100 WHERE Id = 1");

            using var readCtx = NewContext(factory!);
            // At t1 the owned lines were {10, 20}; the live Val=100 must reconstruct to its t1 value (10).
            var historic = await ((INormQueryable<Order>)readCtx.Query<Order>())
                .Select(o => new { o.Id, Vals = o.Lines.Select(l => new LineDto { Val = l.Val }).ToList() })
                .AsOf(t1).ToListAsync();
            Assert.Equal(new[] { 10, 20 }, historic.Single().Vals.Select(v => v.Val).OrderBy(v => v).ToArray());

            // A per-element filter applies to the reconstructed values, not the live ones: at t1 only line 2
            // (20) satisfies Val >= 15 (line 1 was 10 then, though it is 100 now).
            var filtered = await ((INormQueryable<Order>)readCtx.Query<Order>())
                .Select(o => new { o.Id, Vals = o.Lines.Where(l => l.Val >= 15).Select(l => l.Val).ToList() })
                .AsOf(t1).ToListAsync();
            Assert.Equal(new[] { 20 }, filtered.Single().Vals.OrderBy(v => v).ToArray());
        }
        finally
        {
            DropVersioned(factory!, "PntOwnLine");
            DropVersioned(factory!, "PntOwnOrder");
        }
    }
}
