using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// M1/R2: Verifies that <c>MaterializerCacheKey</c> uses actual <see cref="Type"/> references
/// (not hash codes) so that two entity types with identical column layouts but different CLR
/// types produce separate cache entries and separate materializer delegates.
/// MM-1: Verifies that <c>ProjectionHash</c> is a 64-bit value, reducing hash collision risk
/// compared to the previous 32-bit representation.
/// </summary>
public class MaterializerCacheKeyTests
{
    // Two entity types with the same column layout (Id + Name).
    [Table("EntityA")]
    private class EntityA
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("EntityB")]
    private class EntityB
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static DbContext CreateContext() =>
        new DbContext(new SqliteConnection("Data Source=:memory:"), new SqliteProvider());

    [Fact]
    public void DifferentEntityTypes_ProduceSeparateMaterializerDelegates()
    {
        using var ctx = CreateContext();

        // Obtain table mappings via reflection (same pattern used throughout the test suite)
        var getMappingMethod = typeof(DbContext)
            .GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        var mappingA = (TableMapping)getMappingMethod.Invoke(ctx, new object[] { typeof(EntityA) })!;
        var mappingB = (TableMapping)getMappingMethod.Invoke(ctx, new object[] { typeof(EntityB) })!;

        var factory = new MaterializerFactory();

        var delegateA = factory.CreateSyncMaterializer(mappingA, typeof(EntityA));
        var delegateB = factory.CreateSyncMaterializer(mappingB, typeof(EntityB));

        // Must not be the same delegate reference — separate cache entries.
        Assert.False(ReferenceEquals(delegateA, delegateB),
            "Expected distinct materializer delegates for distinct entity types.");
    }

    [Fact]
    public void SameEntityType_ReturnsCachedDelegate()
    {
        using var ctx = CreateContext();

        var getMappingMethod = typeof(DbContext)
            .GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

        var mappingA = (TableMapping)getMappingMethod.Invoke(ctx, new object[] { typeof(EntityA) })!;

        var factory = new MaterializerFactory();

        var delegate1 = factory.CreateSyncMaterializer(mappingA, typeof(EntityA));
        var delegate2 = factory.CreateSyncMaterializer(mappingA, typeof(EntityA));

        // Same type must reuse the cached delegate (same reference).
        Assert.True(ReferenceEquals(delegate1, delegate2),
            "Expected the same cached materializer delegate for the same entity type.");
    }

    [Fact]
    public void MaterializerCacheKey_ProjectionHash_Is64Bit()
    {
        // MM-1: Verify that the ProjectionHash field on MaterializerCacheKey is 64-bit (long),
        // not 32-bit (int), to reduce hash collision probability.
        var keyType = typeof(MaterializerFactory).Assembly
            .GetType("nORM.Query.MaterializerFactory+MaterializerCacheKey", throwOnError: true)!;

        var field = keyType.GetField("ProjectionHash",
            BindingFlags.Public | BindingFlags.Instance);

        Assert.NotNull(field);
        Assert.Equal(typeof(long), field!.FieldType);
    }

    [Fact]
    public void MaterializerCacheKey_TwoKeys_WithDifferent64BitHash_AreDistinct()
    {
        // MM-1: Two keys with the same 32-bit low word but different 64-bit values must
        // not be equal, demonstrating that the 64-bit field prevents 32-bit collisions.
        var keyType = typeof(MaterializerFactory).Assembly
            .GetType("nORM.Query.MaterializerFactory+MaterializerCacheKey", throwOnError: true)!;

        // Construct two keys that share the same low 32-bits but differ in the upper 32-bits.
        // This simulates what would previously be a collision on the old int ProjectionHash.
        var ctor = keyType.GetConstructors(BindingFlags.Public | BindingFlags.Instance).First();

        // Key1: projectionHash = 1 (low=1, high=0)
        var key1 = ctor.Invoke(new object[] { typeof(EntityA), typeof(EntityA), 1L, "EntityA", 0 });
        // Key2: projectionHash = 1 + (1L << 32) — same low 32 bits, different upper 32 bits
        var key2 = ctor.Invoke(new object[] { typeof(EntityA), typeof(EntityA), 1L + (1L << 32), "EntityA", 0 });

        // They must NOT be equal
        Assert.False(key1.Equals(key2),
            "Keys with different 64-bit projection hashes must not be equal.");

        // key1 must equal itself
        Assert.True(key1.Equals(key1),
            "A key must equal itself.");
    }
}
