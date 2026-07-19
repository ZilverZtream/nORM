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
/// The definitive validation of provider-native (SQL Server system-versioned) reconstruction for a
/// navigation-collection PROJECTION under AsOf: against a REAL system-versioned table, the split-query child
/// load's <c>FOR SYSTEM_TIME AS OF</c> FROM source must return the children as they were at the timestamp —
/// with any element filter applied to those historical values and post-snapshot updates reconstructed away.
/// The deterministic routing/composition contract is proven on SQLite by
/// <see cref="ProviderNativeShapedCollectionTemporalTests"/>; this test proves the engine actually
/// reconstructs the era through the clause nORM emits. Skips unless NORM_TEST_SQLSERVER is configured.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderNativeShapedCollectionTemporalTests
{
    [Table("PntShapedParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("PntShapedChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    public class ChildDto { public int Val { get; set; } }
    public class ParentDto { public int Id { get; set; } public List<ChildDto> Kids { get; set; } = new(); }

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

    // System-versioned tables cannot be dropped while versioning is on, and their history table lingers;
    // turn versioning off (if present) then drop both the base and the history table.
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
        DropVersioned(factory, "PntShapedChild");
        DropVersioned(factory, "PntShapedParent");
        // Plain base tables; nORM's provider-native bootstrap ALTERs them to add system-versioning.
        Exec(factory, "CREATE TABLE PntShapedParent (Id INT PRIMARY KEY)");
        Exec(factory, "CREATE TABLE PntShapedChild (Id INT PRIMARY KEY, ParentId INT NOT NULL, Val INT NOT NULL)");
    }

    private static DbContext NewContext(Func<DbConnection> factory)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        opts.EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
        return new DbContext(factory(), new SqlServerProvider(), opts);
    }

    private static async Task<DateTime> SeedTwoErasAsync(Func<DbConnection> factory)
    {
        using (var ctx = NewContext(factory))   // first connection bootstraps system-versioning on both tables
        {
            ctx.Add(new Parent { Id = 1 });
            ctx.Add(new Child { Id = 1, ParentId = 1, Val = 10 });
            ctx.Add(new Child { Id = 2, ParentId = 1, Val = 20 });
            await ctx.SaveChangesAsync();
        }
        await Task.Delay(50);
        var t1 = ScalarUtc(factory, "SELECT SYSUTCDATETIME()");   // era-1 committed; children were {10, 20}
        await Task.Delay(50);
        using (var ctx = NewContext(factory))
        {
            var c1 = ctx.Find<Child>(1)!;
            c1.Val = 100;                        // era-2: c1 10 -> 100; the old row moves to the history table
            await ctx.SaveChangesAsync();
        }
        return t1;
    }

    [Fact]
    public async Task anon_shaped_collection_projection_reconstructs_system_versioned_era()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;
        ResetSchema(factory!);
        try
        {
            var t1 = await SeedTwoErasAsync(factory!);
            using var ctx = NewContext(factory!);
            // At t1 c1 was 10 (< 15) so only c2 (20) matches; the live c1=100 must NOT leak in — the child
            // load reads FOR SYSTEM_TIME AS OF @t1 and the element filter applies to the reconstructed values.
            var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
                .Select(p => new { p.Id, Vals = p.Children.Where(c => c.Val >= 15).Select(c => c.Val).ToList() })
                .AsOf(t1).ToListAsync();
            Assert.Equal(new[] { 20 }, historic.Single().Vals.OrderBy(v => v).ToArray());
        }
        finally
        {
            DropVersioned(factory!, "PntShapedChild");
            DropVersioned(factory!, "PntShapedParent");
        }
    }

    [Fact]
    public async Task dto_shaped_collection_projection_reconstructs_system_versioned_era()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;
        ResetSchema(factory!);
        try
        {
            var t1 = await SeedTwoErasAsync(factory!);
            using var ctx = NewContext(factory!);
            // No filter: at t1 the children were {10, 20}; the live c1=100 reconstructs to its t1 value (10).
            var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
                .Select(p => new ParentDto { Id = p.Id, Kids = p.Children.Select(c => new ChildDto { Val = c.Val }).ToList() })
                .AsOf(t1).ToListAsync();
            Assert.Equal(new[] { 10, 20 }, historic.Single().Kids.Select(k => k.Val).OrderBy(v => v).ToArray());
        }
        finally
        {
            DropVersioned(factory!, "PntShapedChild");
            DropVersioned(factory!, "PntShapedParent");
        }
    }
}
