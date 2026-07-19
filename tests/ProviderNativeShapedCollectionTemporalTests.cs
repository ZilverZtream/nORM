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
/// Pins that a navigation-collection PROJECTION under AsOf reconstructs the historical era through the
/// PROVIDER-NATIVE temporal path — the split-query child load's FROM source is the provider's
/// <c>FOR SYSTEM_TIME AS OF</c> clause (which keeps the table's own name), not the nORM-managed history
/// window. The reconstruction itself is delegated to the engine's system-versioning; on real SQL Server that
/// is validated by <see cref="LiveProviderNativeShapedCollectionTemporalTests"/>. Here a fake provider
/// substitutes a hand-maintained history table for the native clause so the ROUTING (provider branch is taken)
/// and the filter-over-reconstructed-values contract are proven deterministically on SQLite — if the
/// nORM-managed branch were wrongly taken it would read the (empty) {Table}_History and return nothing, and if
/// no reconstruction happened at all it would read the live era and leak present-day rows through the filter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProviderNativeShapedCollectionTemporalTests
{
    [Table("SptPnParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("SptPnChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    public class ChildDto { public int Val { get; set; } }
    public class ParentDto { public int Id { get; set; } public List<ChildDto> Kids { get; set; } = new(); }

    /// <summary>
    /// Stands in for a system-versioned engine: reports native-temporal support and returns, for the as-of
    /// clause, a reconstruction from a hand-maintained <c>{Table}_PnHist</c> table aliased AS the live table —
    /// exactly the shape of SQL Server's <c>FOR SYSTEM_TIME AS OF</c> (table keeps its own name), so the child
    /// load's FK/global/element filters resolve against it unchanged.
    /// </summary>
    private sealed class NativeTemporalSqliteProvider : SqliteProvider
    {
        public override bool SupportsProviderNativeTemporalTables => true;

        // Tables (and their _PnHist history) are pre-created in Boot, so the temporal bootstrap is a no-op —
        // an empty batch set that ExecuteDdlAsync skips, standing in for "the engine already system-versions".
        public override string GenerateProviderNativeTemporalBootstrapSql(TableMapping mapping) => string.Empty;

        public override string GetProviderNativeTemporalAsOfFromClause(TableMapping mapping, string timestampParameterName)
        {
            // SQLite has no FOR SYSTEM_TIME. This double reconstructs the CHILD from a hand-maintained history
            // table, self-aliased AS the live table — the exact shape the split-query child load consumes
            // (used directly, filters reference the table's own name). The era-invariant root parent is passed
            // through as its live self, since the root aliases the source itself and a self-aliased derived
            // table would collide with that alias. On real SQL Server every table uses one uniform
            // FOR SYSTEM_TIME AS OF clause that satisfies both consumers — see the live test.
            if (mapping.TableName != "SptPnChild")
                return Escape(mapping.TableName);
            var cols = string.Join(", ", mapping.Columns.Select(c => "h." + c.EscCol));
            return $"(SELECT {cols} FROM {Escape(mapping.TableName + "_PnHist")} h " +
                   $"WHERE {timestampParameterName} >= h.PnFrom AND {timestampParameterName} < h.PnTo) AS {Escape(mapping.TableName)}";
        }
    }

    private static DbContext Boot(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            // Live tables hold the CURRENT era; the _PnHist table holds the reconstructable history the fake
            // native clause reads. Only the child is queried temporally here, but both live tables must exist.
            cmd.CommandText = """
                CREATE TABLE SptPnParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE SptPnChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                CREATE TABLE SptPnChild_PnHist (Id INTEGER, ParentId INTEGER, Val INTEGER, PnFrom TEXT, PnTo TEXT);
                -- current era: c1 = 100, c2 = 20
                INSERT INTO SptPnParent (Id) VALUES (1);
                INSERT INTO SptPnChild (Id, ParentId, Val) VALUES (1, 1, 100), (2, 1, 20);
                -- history at t1 (2050): c1 = 10, c2 = 20 (c2 unchanged)
                INSERT INTO SptPnChild_PnHist (Id, ParentId, Val, PnFrom, PnTo) VALUES
                    (1, 1, 10, '2000-01-01 00:00:00', '2100-01-01 00:00:00'),
                    (2, 1, 20, '2000-01-01 00:00:00', '9999-01-01 00:00:00');
                """;
            cmd.ExecuteNonQuery();
        }
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
        return new DbContext(cn, new NativeTemporalSqliteProvider(), opts, ownsConnection: false);
    }

    private static readonly DateTime T1 = new(2050, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task anon_shaped_collection_projection_reconstructs_provider_native_era()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; await using var ctx = Boot(cn);
        // At t1, c1 was 10 (< 15) so only c2 (20) matches; the live c1=100 must NOT leak in — the child load
        // reads the provider-native as-of clause, and the element filter applies to the reconstructed values.
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Select(p => new { p.Id, Vals = p.Children.Where(c => c.Val >= 15).Select(c => c.Val).ToList() })
            .AsOf(T1).ToListAsync();
        Assert.Equal(new[] { 20 }, historic.Single().Vals.OrderBy(v => v).ToArray());
    }

    [Fact]
    public async Task dto_shaped_collection_projection_reconstructs_provider_native_era()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; await using var ctx = Boot(cn);
        // No filter: at t1 the children were {10, 20}; the live c1=100 reconstructs to its t1 value (10).
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Select(p => new ParentDto { Id = p.Id, Kids = p.Children.Select(c => new ChildDto { Val = c.Val }).ToList() })
            .AsOf(T1).ToListAsync();
        Assert.Equal(new[] { 10, 20 }, historic.Single().Kids.Select(k => k.Val).OrderBy(v => v).ToArray());
    }
}
