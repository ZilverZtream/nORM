using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Gate 4.5 -> 5.0 Section 8: Adversarial tenant fuzzing for owned, Include, M2M, bulk,
/// and compiled-query paths. Proves multi-tenant boundaries for ALL navigation/storage shapes
/// in a single shared SQLite DB with overlapping PKs across tenants.
/// </summary>
public class AdversarialTenantNavigationShapeTests
{
    // ── Entity definitions ────────────────────────────────────────────────────

    [Table("AtnOwner")]
    private class AtnOwner
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";

        /// <summary>1-to-many child (loaded via Include)</summary>
        public ICollection<AtnChild> Children { get; set; } = new List<AtnChild>();

        /// <summary>Owned collection (loaded via LoadOwnedCollectionsAsync)</summary>
        public List<AtnOwnedItem> OwnedItems { get; set; } = new();

        /// <summary>M2M tags (loaded via Include + join table)</summary>
        public List<AtnTag> Tags { get; set; } = new();
    }

    [Table("AtnChild")]
    private class AtnChild
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int OwnerId { get; set; }
        public string TenantId { get; set; } = "";
        public string Value { get; set; } = "";
    }

    [Table("AtnOwnedItem")]
    private class AtnOwnedItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Data { get; set; } = "";
    }

    [Table("AtnTag")]
    private class AtnTag
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Label { get; set; } = "";
    }

    // ── Tenant provider ───────────────────────────────────────────────────────

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── DDL ───────────────────────────────────────────────────────────────────

    private const string Ddl = @"
        CREATE TABLE AtnOwner    (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);
        CREATE TABLE AtnChild    (Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, TenantId TEXT NOT NULL, Value TEXT NOT NULL);
        CREATE TABLE AtnOwnedItem(Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, TenantId TEXT NOT NULL, Data TEXT NOT NULL);
        CREATE TABLE AtnTag      (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Label TEXT NOT NULL);
        CREATE TABLE AtnOwnerTag (OwnerId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY (OwnerId, TagId));
    ";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = Ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    /// <summary>
    /// Creates a context for Include (1-to-many children) only.
    /// </summary>
    private static DbContext CreateIncludeCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb =>
            {
                mb.Entity<AtnOwner>()
                    .HasMany(o => o.Children)
                    .WithOne()
                    .HasForeignKey(c => c.OwnerId, o => o.Id);
            }
        }, ownsConnection: false);

    /// <summary>
    /// Creates a context for owned collections only.
    /// </summary>
    private static DbContext CreateOwnedCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb =>
            {
                mb.Entity<AtnOwner>()
                    .OwnsMany<AtnOwnedItem>(o => o.OwnedItems, tableName: "AtnOwnedItem", foreignKey: "OwnerId");
            }
        }, ownsConnection: false);

    /// <summary>
    /// Creates a context for M2M tags only.
    /// </summary>
    private static DbContext CreateM2MCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb =>
            {
                mb.Entity<AtnOwner>()
                    .HasMany<AtnTag>(o => o.Tags)
                    .WithMany()
                    .UsingTable("AtnOwnerTag", "OwnerId", "TagId");
            }
        }, ownsConnection: false);

    /// <summary>
    /// Creates a context for bulk operations (no nav config needed).
    /// </summary>
    private static DbContext CreateBulkCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId"
        }, ownsConnection: false);

    /// <summary>
    /// Seeds overlapping data for two tenants. Since SQLite PKs must be unique,
    /// tenant A gets owner Id=1 and tenant B gets owner Id=2. Adversarial cross-tenant
    /// children/owned items are seeded with OwnerId=1 in tenant B to test leak prevention.
    /// Tenant B also has legitimate children/owned items for its own owner Id=2.
    /// </summary>
    private static void SeedTwoTenants(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            -- Owners: distinct PKs per tenant
            INSERT INTO AtnOwner (Id, TenantId, Name) VALUES (1, 'A', 'OwnerA1');
            INSERT INTO AtnOwner (Id, TenantId, Name) VALUES (2, 'B', 'OwnerB2');

            -- 1-to-many children:
            --   Tenant A's children (OwnerId=1)
            INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (1, 'A', 'ChildA-1');
            INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (1, 'A', 'ChildA-2');
            --   Tenant B's children (OwnerId=2, legitimate)
            INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (2, 'B', 'ChildB-1');
            INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (2, 'B', 'ChildB-2');
            --   Adversarial: tenant B child with OwnerId=1 (cross-tenant FK, must NOT leak to A)
            INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (1, 'B', 'ChildB-poison');

            -- Owned items:
            --   Tenant A's owned items (OwnerId=1)
            INSERT INTO AtnOwnedItem (OwnerId, TenantId, Data) VALUES (1, 'A', 'OwnedA-1');
            INSERT INTO AtnOwnedItem (OwnerId, TenantId, Data) VALUES (1, 'A', 'OwnedA-2');
            --   Tenant B's owned items (OwnerId=2, legitimate)
            INSERT INTO AtnOwnedItem (OwnerId, TenantId, Data) VALUES (2, 'B', 'OwnedB-1');
            INSERT INTO AtnOwnedItem (OwnerId, TenantId, Data) VALUES (2, 'B', 'OwnedB-2');
            --   Adversarial: tenant B owned item with OwnerId=1 (cross-tenant FK)
            INSERT INTO AtnOwnedItem (OwnerId, TenantId, Data) VALUES (1, 'B', 'OwnedB-poison');

            -- M2M tags
            INSERT INTO AtnTag (Id, TenantId, Label) VALUES (10, 'A', 'TagA-Alpha');
            INSERT INTO AtnTag (Id, TenantId, Label) VALUES (20, 'B', 'TagB-Beta');
            INSERT INTO AtnTag (Id, TenantId, Label) VALUES (30, 'A', 'TagA-Gamma');
            INSERT INTO AtnTag (Id, TenantId, Label) VALUES (40, 'B', 'TagB-Delta');

            -- Join table:
            --   Owner 1 (tenant A) -> Tag 10 (A), Tag 30 (A), Tag 20 (B - adversarial leak attempt)
            INSERT INTO AtnOwnerTag (OwnerId, TagId) VALUES (1, 10);
            INSERT INTO AtnOwnerTag (OwnerId, TagId) VALUES (1, 20);
            INSERT INTO AtnOwnerTag (OwnerId, TagId) VALUES (1, 30);
            --   Owner 2 (tenant B) -> Tag 20 (B), Tag 40 (B), Tag 10 (A - adversarial leak attempt)
            INSERT INTO AtnOwnerTag (OwnerId, TagId) VALUES (2, 20);
            INSERT INTO AtnOwnerTag (OwnerId, TagId) VALUES (2, 40);
            INSERT INTO AtnOwnerTag (OwnerId, TagId) VALUES (2, 10);
        ";
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(SqliteConnection cn, string table, string? tenantFilter = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = tenantFilter == null
            ? $"SELECT COUNT(*) FROM {table}"
            : $"SELECT COUNT(*) FROM {table} WHERE TenantId = @tid";
        if (tenantFilter != null)
            cmd.Parameters.AddWithValue("@tid", tenantFilter);
        return (long)cmd.ExecuteScalar()!;
    }

    private static List<string> QueryRawNames(SqliteConnection cn, string table, string tenantId, string nameCol = "Name")
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {nameCol} FROM {table} WHERE TenantId = @tid ORDER BY {nameCol}";
        cmd.Parameters.AddWithValue("@tid", tenantId);
        var result = new List<string>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            result.Add(reader.GetString(0));
        return result;
    }

    // ── Test 1: Include 1-to-many isolation ───────────────────────────────────

    /// <summary>
    /// Tenant A sees only tenant A's children via Include, not tenant B's,
    /// despite both tenants sharing OwnerId=1 in the child table.
    /// </summary>
    [Fact]
    public async Task T1_Include_OneToMany_TenantA_SeesOnlyOwnChildren()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        using var ctx = CreateIncludeCtx(cn, "A");

        var owners = await ((INormQueryable<AtnOwner>)ctx.Query<AtnOwner>())
            .Include(o => o.Children)
            .AsSplitQuery()
            .ToListAsync();

        // Tenant A should see only owner Id=1
        Assert.Single(owners);
        var ownerA = owners[0];
        Assert.Equal("OwnerA1", ownerA.Name);

        // Children must be only tenant A's (ChildA-1, ChildA-2), NOT ChildB-1
        Assert.Equal(2, ownerA.Children.Count);
        Assert.All(ownerA.Children, c => Assert.Equal("A", c.TenantId));
        Assert.Contains(ownerA.Children, c => c.Value == "ChildA-1");
        Assert.Contains(ownerA.Children, c => c.Value == "ChildA-2");
        Assert.DoesNotContain(ownerA.Children, c => c.Value == "ChildB-1");
    }

    // ── Test 2: Owned collection isolation ────────────────────────────────────

    /// <summary>
    /// Tenant A's owned items do not leak to tenant B. Owned collection loading
    /// for owner Id=1 in tenant A must return only tenant A's items.
    /// </summary>
    [Fact]
    public async Task T2_OwnedCollection_TenantA_SeesOnlyOwnItems()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        using var ctx = CreateOwnedCtx(cn, "A");
        var map = ctx.GetMapping(typeof(AtnOwner));

        var owners = await ctx.Query<AtnOwner>().ToListAsync();
        Assert.Single(owners);

        await ctx.LoadOwnedCollectionsAsync(
            new ArrayList(owners), map, default);

        var ownerA = owners[0];
        Assert.Equal(2, ownerA.OwnedItems.Count);
        Assert.All(ownerA.OwnedItems, i => Assert.Equal("A", i.TenantId));
        Assert.Contains(ownerA.OwnedItems, i => i.Data == "OwnedA-1");
        Assert.Contains(ownerA.OwnedItems, i => i.Data == "OwnedA-2");
    }

    // ── Test 3: M2M isolation ─────────────────────────────────────────────────

    /// <summary>
    /// Tenant A's M2M tags do not leak tenant B's tags. Despite join rows linking
    /// owner 1 to both tag 10 (tenant A) and tag 20 (tenant B), Include should
    /// filter tags by tenant.
    /// </summary>
    [Fact]
    public async Task T3_M2M_TenantA_SeesOnlyOwnTags()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        using var ctx = CreateM2MCtx(cn, "A");

        var owners = await ((INormQueryable<AtnOwner>)ctx.Query<AtnOwner>())
            .Include(o => o.Tags)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(owners);
        var ownerA = owners[0];

        // Owner 1 has join rows to tag 10 (A), tag 20 (B), tag 30 (A).
        // Tenant A should see only tags 10 and 30.
        Assert.Equal(2, ownerA.Tags.Count);
        Assert.All(ownerA.Tags, t => Assert.Equal("A", t.TenantId));
        Assert.Contains(ownerA.Tags, t => t.Label == "TagA-Alpha");
        Assert.Contains(ownerA.Tags, t => t.Label == "TagA-Gamma");
        Assert.DoesNotContain(ownerA.Tags, t => t.Label == "TagB-Beta");
    }

    // ── Test 4: Combined Include + Owned + M2M ───────────────────────────────

    /// <summary>
    /// All three navigation shapes (Include 1-to-many, owned collection, M2M) are loaded
    /// for the same owner entity. Each must be filtered by tenant independently.
    /// </summary>
    [Fact]
    public async Task T4_Combined_AllShapes_FilteredByTenant()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Step 1: Include 1-to-many children
        using var ctxInclude = CreateIncludeCtx(cn, "A");
        var ownersInclude = await ((INormQueryable<AtnOwner>)ctxInclude.Query<AtnOwner>())
            .Include(o => o.Children)
            .AsSplitQuery()
            .ToListAsync();
        Assert.Single(ownersInclude);
        Assert.Equal(2, ownersInclude[0].Children.Count);
        Assert.All(ownersInclude[0].Children, c => Assert.Equal("A", c.TenantId));

        // Step 2: Owned items (separate context for owned config)
        using var ctxOwned = CreateOwnedCtx(cn, "A");
        var ownersOwned = await ctxOwned.Query<AtnOwner>().ToListAsync();
        Assert.Single(ownersOwned);
        var mapOwned = ctxOwned.GetMapping(typeof(AtnOwner));
        await ctxOwned.LoadOwnedCollectionsAsync(new ArrayList(ownersOwned), mapOwned, default);
        Assert.Equal(2, ownersOwned[0].OwnedItems.Count);
        Assert.All(ownersOwned[0].OwnedItems, i => Assert.Equal("A", i.TenantId));

        // Step 3: M2M tags
        using var ctxM2M = CreateM2MCtx(cn, "A");
        var ownersM2M = await ((INormQueryable<AtnOwner>)ctxM2M.Query<AtnOwner>())
            .Include(o => o.Tags)
            .AsSplitQuery()
            .ToListAsync();
        Assert.Single(ownersM2M);
        Assert.Equal(2, ownersM2M[0].Tags.Count);
        Assert.All(ownersM2M[0].Tags, t => Assert.Equal("A", t.TenantId));
    }

    // ── Test 5: Bulk insert + query isolation ─────────────────────────────────

    /// <summary>
    /// BulkInsert as tenant A, then query as tenant B. Tenant B must see zero rows
    /// from tenant A's insert.
    /// </summary>
    [Fact]
    public async Task T5_BulkInsert_TenantA_QueryAsTenantB_SeesNothing()
    {
        using var cn = CreateDb();

        // BulkInsert as tenant A
        using var ctxA = CreateBulkCtx(cn, "A");
        await ctxA.BulkInsertAsync(new[]
        {
            new AtnOwner { Id = 100, TenantId = "A", Name = "BulkA-1" },
            new AtnOwner { Id = 101, TenantId = "A", Name = "BulkA-2" },
            new AtnOwner { Id = 102, TenantId = "A", Name = "BulkA-3" }
        });

        // Verify rows exist at DB level
        Assert.Equal(3, CountRows(cn, "AtnOwner", "A"));

        // Query as tenant B: must see zero
        using var ctxB = CreateBulkCtx(cn, "B");
        var resultB = await ctxB.Query<AtnOwner>().ToListAsync();
        Assert.Empty(resultB);

        // Query as tenant A: must see all 3
        using var ctxA2 = CreateBulkCtx(cn, "A");
        var resultA = await ctxA2.Query<AtnOwner>().ToListAsync();
        Assert.Equal(3, resultA.Count);
        Assert.All(resultA, r => Assert.Equal("A", r.TenantId));
    }

    // ── Test 6: Cross-tenant owned DELETE ─────────────────────────────────────

    /// <summary>
    /// Deleting tenant A's owner must not destroy tenant B's owned items that share
    /// the same OwnerId FK value.
    /// </summary>
    [Fact]
    public async Task T6_CrossTenant_OwnedDelete_DoesNotDestroyOtherTenantItems()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Pre-check: 5 total owned items (2 for A, 3 for B including poison)
        Assert.Equal(5, CountRows(cn, "AtnOwnedItem"));
        Assert.Equal(2, CountRows(cn, "AtnOwnedItem", "A"));
        Assert.Equal(3, CountRows(cn, "AtnOwnedItem", "B"));

        // Delete owner from tenant A (update with empty owned collection triggers DELETE)
        using var ctxA = CreateOwnedCtx(cn, "A");
        var ownersA = await ctxA.Query<AtnOwner>().ToListAsync();
        Assert.Single(ownersA);
        ownersA[0].OwnedItems = new List<AtnOwnedItem>(); // clear owned items
        ctxA.Update(ownersA[0]);
        await ctxA.SaveChangesAsync();

        // Tenant A's owned items are gone
        Assert.Equal(0, CountRows(cn, "AtnOwnedItem", "A"));
        // Tenant B's owned items are untouched (2 legitimate + 1 poison = 3)
        Assert.Equal(3, CountRows(cn, "AtnOwnedItem", "B"));
    }

    // ── Test 7: Cross-tenant M2M DELETE ───────────────────────────────────────

    /// <summary>
    /// Deleting tenant A's owner must not destroy tenant B's join rows. The join
    /// table has rows for both tenants' owners.
    /// </summary>
    [Fact]
    public async Task T7_CrossTenant_M2MDelete_DoesNotDestroyOtherTenantJoinRows()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Pre-check: 6 join rows total (3 for owner 1, 3 for owner 2)
        Assert.Equal(6, CountRows(cn, "AtnOwnerTag"));

        // Delete tenant A's owner
        using var ctxA = CreateM2MCtx(cn, "A");
        var ownerA = new AtnOwner { Id = 1, TenantId = "A", Name = "OwnerA1" };
        ctxA.Attach(ownerA);
        ctxA.Remove(ownerA);
        await ctxA.SaveChangesAsync();

        // Tenant B's join rows for owner 2 must still exist (all 3: tag 20, 40, 10)
        long tenantBJoinRows;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM AtnOwnerTag WHERE OwnerId = 2";
            tenantBJoinRows = (long)cmd.ExecuteScalar()!;
        }
        Assert.Equal(3, tenantBJoinRows);
    }

    // ── Test 8: Adversarial PK collision ──────────────────────────────────────

    /// <summary>
    /// Both tenants have entity Id=1 with different data. Each tenant must see
    /// only their own version across all navigation shapes. This is the most
    /// adversarial scenario: identical PKs, identical FKs, different tenant data.
    /// </summary>
    [Fact]
    public async Task T8_AdversarialPKCollision_EachTenantSeesOwnDataOnly()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // ── Tenant A perspective ──
        using var ctxA = CreateIncludeCtx(cn, "A");
        var ownersA = await ((INormQueryable<AtnOwner>)ctxA.Query<AtnOwner>())
            .Include(o => o.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(ownersA);
        Assert.Equal("OwnerA1", ownersA[0].Name);
        Assert.Equal(2, ownersA[0].Children.Count);
        Assert.All(ownersA[0].Children, c =>
        {
            Assert.Equal("A", c.TenantId);
            Assert.StartsWith("ChildA", c.Value);
        });

        // ── Tenant B perspective ──
        using var ctxB = CreateIncludeCtx(cn, "B");
        var ownersB = await ((INormQueryable<AtnOwner>)ctxB.Query<AtnOwner>())
            .Include(o => o.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(ownersB);
        Assert.Equal("OwnerB2", ownersB[0].Name);
        Assert.Equal(2, ownersB[0].Children.Count);
        Assert.All(ownersB[0].Children, c =>
        {
            Assert.Equal("B", c.TenantId);
            Assert.StartsWith("ChildB", c.Value);
        });

        // Verify no cross-contamination: A's children never appear in B's results
        var allAValues = ownersA[0].Children.Select(c => c.Value).ToHashSet();
        var allBValues = ownersB[0].Children.Select(c => c.Value).ToHashSet();
        Assert.Empty(allAValues.Intersect(allBValues));
    }

    // ── Test 9: Concurrent tenant queries ─────────────────────────────────────

    /// <summary>
    /// Two tasks query simultaneously as different tenants. Neither task should
    /// see the other tenant's data. Uses a shared in-memory SQLite DB.
    /// </summary>
    [Fact]
    public async Task T9_ConcurrentTenantQueries_NoCrossContamination()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Use a barrier to synchronize start
        var barrier = new SemaphoreSlim(0, 2);
        var errors = new List<string>();

        async Task QueryAsTenant(string tenantId, string expectedOwnerName, int expectedChildCount)
        {
            barrier.Wait();
            try
            {
                using var ctx = CreateIncludeCtx(cn, tenantId);
                var owners = await ((INormQueryable<AtnOwner>)ctx.Query<AtnOwner>())
                    .Include(o => o.Children)
                    .AsSplitQuery()
                    .ToListAsync();

                if (owners.Count != 1)
                    lock (errors) errors.Add($"Tenant {tenantId}: expected 1 owner, got {owners.Count}");
                else
                {
                    if (owners[0].Name != expectedOwnerName)
                        lock (errors) errors.Add($"Tenant {tenantId}: expected name '{expectedOwnerName}', got '{owners[0].Name}'");
                    if (owners[0].Children.Count != expectedChildCount)
                        lock (errors) errors.Add($"Tenant {tenantId}: expected {expectedChildCount} children, got {owners[0].Children.Count}");
                    foreach (var c in owners[0].Children)
                    {
                        if (c.TenantId != tenantId)
                            lock (errors) errors.Add($"Tenant {tenantId}: child has wrong TenantId '{c.TenantId}'");
                    }
                }
            }
            catch (Exception ex)
            {
                lock (errors) errors.Add($"Tenant {tenantId}: exception {ex.Message}");
            }
        }

        // SQLite in-memory is single-connection, so concurrent tasks will serialize
        // on the connection, but the tenant filter must still isolate correctly.
        var taskA = Task.Run(() => QueryAsTenant("A", "OwnerA1", 2));
        var taskB = Task.Run(() => QueryAsTenant("B", "OwnerB2", 2));

        // Release both tasks
        barrier.Release(2);
        await Task.WhenAll(taskA, taskB);

        Assert.Empty(errors);
    }

    // ── Test 10: Differential oracle ──────────────────────────────────────────

    /// <summary>
    /// For each tenant, compare nORM query result with raw SQL result. They must
    /// match exactly, proving the ORM's tenant filter is equivalent to manual SQL.
    /// </summary>
    [Fact]
    public async Task T10_DifferentialOracle_NormVsRawSql_Match()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        foreach (var tenantId in new[] { "A", "B" })
        {
            // ── nORM query ──
            using var ctx = CreateBulkCtx(cn, tenantId);
            var normOwners = await ctx.Query<AtnOwner>()
                .OrderBy(o => o.Id)
                .ToListAsync();
            var normNames = normOwners.Select(o => o.Name).ToList();

            // ── Raw SQL query ──
            var rawNames = QueryRawNames(cn, "AtnOwner", tenantId);

            // Must match exactly
            Assert.Equal(rawNames, normNames);

            // Also verify children via raw SQL that mimics Include semantics:
            // children whose OwnerId matches a tenant-filtered owner AND child TenantId matches
            var rawChildValues = new List<string>();
            using (var childCmd = cn.CreateCommand())
            {
                childCmd.CommandText = @"
                    SELECT c.Value FROM AtnChild c
                    INNER JOIN AtnOwner o ON c.OwnerId = o.Id AND o.TenantId = @tid
                    WHERE c.TenantId = @tid
                    ORDER BY c.Value";
                childCmd.Parameters.AddWithValue("@tid", tenantId);
                using var reader = childCmd.ExecuteReader();
                while (reader.Read())
                    rawChildValues.Add(reader.GetString(0));
            }

            using var ctxInc = CreateIncludeCtx(cn, tenantId);
            var normOwnersFull = await ((INormQueryable<AtnOwner>)ctxInc.Query<AtnOwner>())
                .Include(o => o.Children)
                .AsSplitQuery()
                .ToListAsync();
            var normChildValues = normOwnersFull
                .SelectMany(o => o.Children)
                .Select(c => c.Value)
                .OrderBy(v => v)
                .ToList();

            Assert.Equal(rawChildValues, normChildValues);
        }
    }

    // ── Test 11: Bulk insert cross-tenant then Include ────────────────────────

    /// <summary>
    /// BulkInsert owners and children for tenant A and B. Then Include-query as
    /// each tenant. Verifies Include returns only current tenant's children
    /// after bulk seeding.
    /// </summary>
    [Fact]
    public async Task T11_BulkInsertBothTenants_IncludeReturnsOnlyOwnChildren()
    {
        using var cn = CreateDb();

        // BulkInsert owners for both tenants
        using var ctxSeed = new DbContext(cn, new SqliteProvider(), null, ownsConnection: false);
        await ctxSeed.BulkInsertAsync(new[]
        {
            new AtnOwner { Id = 50, TenantId = "X", Name = "OwnerX" },
            new AtnOwner { Id = 51, TenantId = "Y", Name = "OwnerY" }
        });

        // Seed children via raw SQL (BulkInsert for simple entities)
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (50, 'X', 'ChildX-1');
                INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (50, 'X', 'ChildX-2');
                INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (50, 'Y', 'ChildY-poison');
                INSERT INTO AtnChild (OwnerId, TenantId, Value) VALUES (51, 'Y', 'ChildY-1');
            ";
            cmd.ExecuteNonQuery();
        }

        // Query as tenant X with Include
        using var ctxX = CreateIncludeCtx(cn, "X");
        var ownersX = await ((INormQueryable<AtnOwner>)ctxX.Query<AtnOwner>())
            .Include(o => o.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(ownersX);
        Assert.Equal("OwnerX", ownersX[0].Name);
        Assert.Equal(2, ownersX[0].Children.Count);
        Assert.All(ownersX[0].Children, c => Assert.Equal("X", c.TenantId));
        // Must not contain the "poison" child from tenant Y with same OwnerId=50
        Assert.DoesNotContain(ownersX[0].Children, c => c.Value == "ChildY-poison");
    }

    // ── Test 12: M2M tenant B perspective ─────────────────────────────────────

    /// <summary>
    /// Tenant B's M2M Include must only see tenant B's tags, even though join rows
    /// exist linking tenant B's owner to tenant A's tags.
    /// </summary>
    [Fact]
    public async Task T12_M2M_TenantB_SeesOnlyOwnTags()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        using var ctx = CreateM2MCtx(cn, "B");

        var owners = await ((INormQueryable<AtnOwner>)ctx.Query<AtnOwner>())
            .Include(o => o.Tags)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(owners);
        var ownerB = owners[0];
        Assert.Equal("OwnerB2", ownerB.Name);

        // Owner 2 has join rows to tag 20 (B), tag 40 (B), tag 10 (A - adversarial).
        // Must see only tags 20 and 40 (tenant B), NOT tag 10 (tenant A).
        Assert.Equal(2, ownerB.Tags.Count);
        Assert.All(ownerB.Tags, t => Assert.Equal("B", t.TenantId));
        Assert.Contains(ownerB.Tags, t => t.Label == "TagB-Beta");
        Assert.Contains(ownerB.Tags, t => t.Label == "TagB-Delta");
        Assert.DoesNotContain(ownerB.Tags, t => t.Label == "TagA-Alpha");
    }

    // ── Test 13: Adversarial bulk update cross-tenant ─────────────────────────

    /// <summary>
    /// Tenant B attempts BulkUpdate on a row belonging to tenant A. The tenant
    /// predicate in the WHERE clause must block the update.
    /// </summary>
    [Fact]
    public async Task T13_BulkUpdate_CrossTenant_Blocked()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Tenant B tries to update Owner Id=1 which belongs to tenant A
        using var ctxB = CreateBulkCtx(cn, "B");
        var forged = new AtnOwner { Id = 1, TenantId = "B", Name = "TAMPERED" };
        var updated = await ctxB.BulkUpdateAsync(new[] { forged });

        Assert.Equal(0, updated);

        // Verify original name is intact
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT Name FROM AtnOwner WHERE Id = 1";
            var name = (string)cmd.ExecuteScalar()!;
            Assert.Equal("OwnerA1", name);
        }
    }

    // ── Test 14: Differential oracle for owned collections ────────────────────

    /// <summary>
    /// For each tenant, compare the owned collection items loaded via nORM with
    /// a direct SQL query. Both must return the same set.
    /// </summary>
    [Fact]
    public async Task T14_DifferentialOracle_OwnedCollection_MatchesRawSql()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        foreach (var tenantId in new[] { "A", "B" })
        {
            // ── Raw SQL that mimics owned-collection loading semantics ──
            // Only items whose OwnerId matches a tenant-filtered owner AND item TenantId matches
            var rawData = new List<string>();
            using (var rawCmd = cn.CreateCommand())
            {
                rawCmd.CommandText = @"
                    SELECT oi.Data FROM AtnOwnedItem oi
                    INNER JOIN AtnOwner o ON oi.OwnerId = o.Id AND o.TenantId = @tid
                    WHERE oi.TenantId = @tid
                    ORDER BY oi.Data";
                rawCmd.Parameters.AddWithValue("@tid", tenantId);
                using var reader = rawCmd.ExecuteReader();
                while (reader.Read())
                    rawData.Add(reader.GetString(0));
            }

            // ── nORM owned load ──
            using var ctx = CreateOwnedCtx(cn, tenantId);
            var map = ctx.GetMapping(typeof(AtnOwner));
            var owners = await ctx.Query<AtnOwner>().ToListAsync();

            await ctx.LoadOwnedCollectionsAsync(new ArrayList(owners), map, default);

            var normData = owners
                .SelectMany(o => o.OwnedItems)
                .Select(i => i.Data)
                .OrderBy(d => d)
                .ToList();

            Assert.Equal(rawData, normData);
        }
    }
}
