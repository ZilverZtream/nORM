using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SQL Server generates a fresh ROWVERSION when a row is INSERTED, not just on
/// UPDATE. A context that adds a [Timestamp] entity with an explicit key and
/// saves must be able to update or delete that same instance afterwards: the
/// insert has to read the generated token back into the entity, or the next
/// save's WHERE clause compares against the entity's stale in-memory token and
/// throws a false DbConcurrencyException for a row nobody else touched.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class RowVersionInsertHydrationLiveTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("RvInsHyd_Test")]
    public class VersionedRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
        [System.ComponentModel.DataAnnotations.Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    private static (Func<DbConnection>?, string?) OpenLive()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_SQLSERVER not set");
        var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
        return (() =>
        {
            var cn = (DbConnection)Activator.CreateInstance(t, cs)!;
            cn.Open();
            return cn;
        }, null);
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Inserted_rowversion_entity_can_be_updated_then_deleted_by_the_same_context()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;

        Exec(factory!, "IF OBJECT_ID('RvInsHyd_Test') IS NOT NULL DROP TABLE RvInsHyd_Test");
        Exec(factory!, "CREATE TABLE RvInsHyd_Test (Id INT PRIMARY KEY, Val INT NOT NULL, Token ROWVERSION)");
        try
        {
            using var ctx = new DbContext(factory!(), new SqlServerProvider());

            var row = new VersionedRow { Id = 1, Val = 10 };
            ctx.Add(row);
            await ctx.SaveChangesAsync();
            Assert.True(row.Token.Length > 0,
                "the INSERT must hydrate the generated ROWVERSION into the tracked entity");

            row.Val = 20;
            await ctx.SaveChangesAsync(); // false DbConcurrencyException here = stale insert token

            using (var verifyCtx = new DbContext(factory!(), new SqlServerProvider()))
            {
                var persisted = await verifyCtx.Query<VersionedRow>().FirstAsync(r => r.Id == 1);
                Assert.Equal(20, persisted.Val);
            }

            ctx.Remove(row);
            await ctx.SaveChangesAsync(); // and the delete must match the current token too

            using (var verifyCtx = new DbContext(factory!(), new SqlServerProvider()))
            {
                var remaining = await verifyCtx.Query<VersionedRow>().ToListAsync();
                Assert.Empty(remaining);
            }
        }
        finally
        {
            Exec(factory!, "IF OBJECT_ID('RvInsHyd_Test') IS NOT NULL DROP TABLE RvInsHyd_Test");
        }
    }

    [Fact]
    public async Task Direct_insert_update_update_delete_never_false_conflicts()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;

        Exec(factory!, "IF OBJECT_ID('RvInsHyd_Test') IS NOT NULL DROP TABLE RvInsHyd_Test");
        Exec(factory!, "CREATE TABLE RvInsHyd_Test (Id INT PRIMARY KEY, Val INT NOT NULL, Token ROWVERSION)");
        try
        {
            using var ctx = new DbContext(factory!(), new SqlServerProvider());

            var row = new VersionedRow { Id = 1, Val = 10 };
            await ctx.InsertAsync(row);
            Assert.True(row.Token.Length > 0,
                "direct InsertAsync must hydrate the generated ROWVERSION into the entity");

            row.Val = 20;
            await ctx.UpdateAsync(row);
            row.Val = 30;
            await ctx.UpdateAsync(row); // stale token from the first update false-conflicts here
            await ctx.DeleteAsync(row); // and the delete needs the current token too

            using var verifyCtx = new DbContext(factory!(), new SqlServerProvider());
            var remaining = await verifyCtx.Query<VersionedRow>().ToListAsync();
            Assert.Empty(remaining);
        }
        finally
        {
            Exec(factory!, "IF OBJECT_ID('RvInsHyd_Test') IS NOT NULL DROP TABLE RvInsHyd_Test");
        }
    }

    [Fact]
    public async Task Inserted_rowversion_entities_hydrate_tokens_in_multi_row_batches()
    {
        var (factory, skip) = OpenLive();
        if (skip != null) return;

        Exec(factory!, "IF OBJECT_ID('RvInsHyd_Test') IS NOT NULL DROP TABLE RvInsHyd_Test");
        Exec(factory!, "CREATE TABLE RvInsHyd_Test (Id INT PRIMARY KEY, Val INT NOT NULL, Token ROWVERSION)");
        try
        {
            using var ctx = new DbContext(factory!(), new SqlServerProvider());

            var rows = new[]
            {
                new VersionedRow { Id = 1, Val = 1 },
                new VersionedRow { Id = 2, Val = 2 },
                new VersionedRow { Id = 3, Val = 3 },
            };
            foreach (var row in rows)
                ctx.Add(row);
            await ctx.SaveChangesAsync();

            foreach (var row in rows)
            {
                Assert.True(row.Token.Length > 0,
                    $"row {row.Id}: the batched INSERT must hydrate the generated ROWVERSION");
                row.Val += 100;
            }
            await ctx.SaveChangesAsync();

            using var verifyCtx = new DbContext(factory!(), new SqlServerProvider());
            var persisted = await verifyCtx.Query<VersionedRow>().ToListAsync();
            Assert.Equal(3, persisted.Count);
            Assert.All(persisted, r => Assert.True(r.Val > 100));
        }
        finally
        {
            Exec(factory!, "IF OBJECT_ID('RvInsHyd_Test') IS NOT NULL DROP TABLE RvInsHyd_Test");
        }
    }
}
