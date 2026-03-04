using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
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
}
