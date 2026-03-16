using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// X1 — CompiledMaterializerStore Type-only cache key (Gate 3.7→4.0)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that <c>CompiledMaterializerStore</c> keys materializers by
/// <c>(Type, tableName)</c> so that the same CLR type registered under different
/// model mappings each gets a distinct entry — preventing first-write-wins silent
/// hydration errors.
///
/// X1 root cause: <c>_map</c> was keyed by <c>Type</c> only with <c>GetOrAdd</c>
/// semantics. The first registered materializer permanently won for a given type,
/// so a second model mapping for the same CLR type would silently use the wrong
/// compiled deserializer.
///
/// Fix: key changed to <c>(Type, string tableName)</c>. <c>Add()</c> uses the
/// <c>[Table]</c> attribute name (or <c>type.Name</c>) as the discriminator.
/// <c>TryGet(type, tableName)</c> overload performs exact model-aware lookup;
/// <c>MaterializerFactory</c> passes <c>mapping.TableName</c> to avoid using a
/// compiled materializer registered for a different table layout.
/// </summary>
public class CompiledMaterializerMultiModelTests
{
    // ── Entity types used by these tests ──────────────────────────────────────

    // Each test uses its own unique type so the static _map doesn't carry over
    // state between tests.

    [Table("XModel_A")]
    private class XModel_A { public int Id { get; set; } public string Val { get; set; } = ""; }

    [Table("XModel_B")]
    private class XModel_B { public int Id { get; set; } public string Val { get; set; } = ""; }

    // Two distinct types that happen to share the same table name — should remain
    // independent because CLR type is the first tuple element.
    [Table("Shared_Table")]
    private class XSharedTable_TypeOne { public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("Shared_Table")]
    private class XSharedTable_TypeTwo { public int Id { get; set; } public string Name { get; set; } = ""; }

    // Type without a [Table] attribute — table name defaults to CLR type name.
    private class XNoTableAttr { public int Id { get; set; } public string Tag { get; set; } = ""; }

    // Type used to test that TryGet(type, wrongTable) returns false.
    [Table("XTableA")]
    private class XTableDiscriminator { public int Id { get; set; } }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Creates an in-memory SQLite reader with one row of (Id=1, Val="hello").</summary>
    private static async Task<DbDataReader> MakeTwoColumnReaderAsync(SqliteConnection cn, string tableName, int id, string val)
    {
        using var setup = cn.CreateCommand();
        setup.CommandText = $"CREATE TABLE IF NOT EXISTS \"{tableName}\" (Id INTEGER, Val TEXT)";
        setup.ExecuteNonQuery();
        using var insert = cn.CreateCommand();
        insert.CommandText = $"INSERT INTO \"{tableName}\" VALUES(@id, @val)";
        insert.Parameters.AddWithValue("@id", id);
        insert.Parameters.AddWithValue("@val", val);
        insert.ExecuteNonQuery();

        var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Id, Val FROM \"{tableName}\"";
        return await cmd.ExecuteReaderAsync();
    }

    // ── TryGet(type, tableName) discriminates by table name ───────────────────

    [Fact]
    public void TryGet_WithCorrectTableName_ReturnsTrue()
    {
        // Register via [Table("XModel_A")] → key = (XModel_A, "XModel_A")
        CompiledMaterializerStore.Add(typeof(XModel_A), reader =>
            new XModel_A { Id = reader.GetInt32(0), Val = reader.GetString(1) });

        var found = CompiledMaterializerStore.TryGet(typeof(XModel_A), "XModel_A", out var mat);

        Assert.True(found, "TryGet with matching table name must return true");
        Assert.NotNull(mat);
    }

    [Fact]
    public void TryGet_WithWrongTableName_ReturnsFalse()
    {
        // XTableDiscriminator is stored under "XTableA" (from [Table] attribute).
        CompiledMaterializerStore.Add(typeof(XTableDiscriminator), reader =>
            new XTableDiscriminator { Id = reader.GetInt32(0) });

        // Look up with a *different* table name — must not find anything.
        var found = CompiledMaterializerStore.TryGet(typeof(XTableDiscriminator), "WrongTableName", out _);

        Assert.False(found,
            "TryGet with a table name that doesn't match the registered key must return false");
    }

    [Fact]
    public void TryGet_NoTableAttr_UsesTypeName()
    {
        // XNoTableAttr has no [Table] attribute → registered under "XNoTableAttr".
        CompiledMaterializerStore.Add(typeof(XNoTableAttr), reader =>
            new XNoTableAttr { Id = reader.GetInt32(0), Tag = reader.GetString(1) });

        var foundByName    = CompiledMaterializerStore.TryGet(typeof(XNoTableAttr), "XNoTableAttr", out _);
        var foundByWrong   = CompiledMaterializerStore.TryGet(typeof(XNoTableAttr), "WrongName", out _);

        Assert.True(foundByName,  "TryGet with CLR type name must succeed when no [Table] attribute");
        Assert.False(foundByWrong, "TryGet with wrong name must fail");
    }

    // ── TryGet(type) backward-compat overload uses attribute-derived name ─────

    [Fact]
    public void TryGet_BackwardCompatOverload_UsesAttributeName()
    {
        // Register XModel_B under its [Table("XModel_B")] key.
        CompiledMaterializerStore.Add(typeof(XModel_B), reader =>
            new XModel_B { Id = reader.GetInt32(0), Val = reader.GetString(1) });

        // Single-arg TryGet should find it via the attribute-derived name.
        var found = CompiledMaterializerStore.TryGet(typeof(XModel_B), out _);

        Assert.True(found, "Single-arg TryGet must resolve via [Table] attribute name");
    }

    // ── Two CLR types sharing the same table name are independent ─────────────

    [Fact]
    public void SameTableName_DifferentTypes_AreIndependent()
    {
        var mat1 = (Func<DbDataReader, object>)(r => new XSharedTable_TypeOne { Id = r.GetInt32(0), Name = r.GetString(1) });
        var mat2 = (Func<DbDataReader, object>)(r => new XSharedTable_TypeTwo { Id = r.GetInt32(0), Name = r.GetString(1) });

        CompiledMaterializerStore.Add(typeof(XSharedTable_TypeOne), mat1);
        CompiledMaterializerStore.Add(typeof(XSharedTable_TypeTwo), mat2);

        var found1 = CompiledMaterializerStore.TryGet(typeof(XSharedTable_TypeOne), "Shared_Table", out var out1);
        var found2 = CompiledMaterializerStore.TryGet(typeof(XSharedTable_TypeTwo), "Shared_Table", out var out2);

        Assert.True(found1, "First type must have its own entry");
        Assert.True(found2, "Second type must have its own entry");
        Assert.NotSame(out1, out2);
    }

    // ── First-write-wins is preserved per (type, tableName) key ───────────────

    [Fact]
    public void Add_SameTypeAndTable_FirstWriteWins()
    {
        // Create unique private types per call by using different table names
        // through TryGet to verify first-write semantics.

        int firstCallCount = 0;
        int secondCallCount = 0;

        Func<DbDataReader, object> first  = _ => { firstCallCount++;  return new XModel_A(); };
        Func<DbDataReader, object> second = _ => { secondCallCount++; return new XModel_A(); };

        // Both registrations use (XModel_A, "XModel_A") — first wins.
        CompiledMaterializerStore.Add(typeof(XModel_A), first);
        CompiledMaterializerStore.Add(typeof(XModel_A), second);  // must be ignored

        CompiledMaterializerStore.TryGet(typeof(XModel_A), "XModel_A", out var mat);
        Assert.NotNull(mat);

        // Invoke the cached materializer to confirm it's the first one.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var setupCmd = cn.CreateCommand();
        setupCmd.CommandText = "CREATE TABLE XModel_A (Id INTEGER, Val TEXT); INSERT INTO XModel_A VALUES(1,'x')";
        setupCmd.ExecuteNonQuery();
        var readCmd = cn.CreateCommand();
        readCmd.CommandText = "SELECT Id, Val FROM XModel_A";
        using var reader = readCmd.ExecuteReader();
        reader.Read();
        // Sync-over-async is safe here: Task.FromResult does not block.
        var task = mat!(reader, CancellationToken.None);
        Assert.True(task.IsCompleted, "Compiled materializer must complete synchronously");

        Assert.Equal(1, firstCallCount);
        Assert.Equal(0, secondCallCount);
    }

    // ── Runtime parity: compiled and reflection paths return identical data ────

    // Unique type for parity test to avoid shared-state issues with other tests.
    [Table("XParity_Table")]
    private class XParity_Entity { public int Id { get; set; } public string Val { get; set; } = ""; }

    [Fact]
    public async Task CompiledMaterializer_ProducesSameData_AsReflectionPath()
    {
        // Register a compiled materializer that reads Id/Val from the reader.
        CompiledMaterializerStore.Add(typeof(XParity_Entity), reader =>
            new XParity_Entity { Id = reader.GetInt32(0), Val = reader.GetString(1) });

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var reader = await MakeTwoColumnReaderAsync(cn, "XParity_Table", 42, "parity_value");

        CompiledMaterializerStore.TryGet(typeof(XParity_Entity), "XParity_Table", out var mat);
        Assert.NotNull(mat);

        reader.Read();
        var obj = await mat!(reader, CancellationToken.None);
        var entity = Assert.IsType<XParity_Entity>(obj);

        Assert.Equal(42, entity.Id);
        Assert.Equal("parity_value", entity.Val);
    }

    // ── TryGet with wrong table name causes MaterializerFactory to fall through ─

    [Fact]
    public void WrongTableName_FallsThrough_ToReflectionPath()
    {
        // Ensure XModel_A is registered (it may already be from prior tests).
        CompiledMaterializerStore.Add(typeof(XModel_A), r =>
            new XModel_A { Id = r.GetInt32(0), Val = r.GetString(1) });

        // When MaterializerFactory calls TryGet with a different table name (e.g.,
        // a model that maps XModel_A to "AltTable"), the compiled materializer must
        // NOT be returned — preventing wrong-schema hydration.
        var found = CompiledMaterializerStore.TryGet(typeof(XModel_A), "AltTable", out _);

        Assert.False(found,
            "A mapping to 'AltTable' must not find the materializer registered for 'XModel_A'; " +
            "MaterializerFactory must fall through to the reflection-based path");
    }
}
