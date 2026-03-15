using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Source-generated materializer correctness tests.
///
/// Verifies that:
/// 1. Source-generated materializers use name-based ordinal resolution (reader.GetOrdinal)
///    so column ORDER in the result set does not affect mapping correctness.
/// 2. FromSqlRawAsync uses the compiled materializer and inherits the same name-based fix.
/// 3. Owned-type column names (Owner_Prop) are resolved correctly regardless of position.
/// </summary>
public class SourceGenMaterializerCorrectnessTests
{
    // ── reordered columns do not swap property values ─────────────────

    [Fact]
    public async Task SourceGen_ReorderedColumns_ValuesCorrect()
    {
        // Alphabetical property order of Materialized: Created, Guid, Id, IsActive, Name, Price, Status
        // We return columns in REVERSE order to prove ordinal assumptions are not used.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var g = Guid.NewGuid();

        // Deliberately non-alphabetical column order: Id, Name, Price, Status, IsActive, Guid, Created
        cmd.CommandText =
            "SELECT 42 AS Id, 'hello' AS Name, 9.99 AS Price, 1 AS Status, 1 AS IsActive, $g AS Guid, NULL AS Created";
        cmd.Parameters.AddWithValue("$g", g);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var mat = CompiledMaterializerStore.Get<Materialized>();
        var entity = await mat(reader, CancellationToken.None);

        // With the fix these must be correct; without fix they'd be silently swapped
        Assert.Equal(42, entity.Id);
        Assert.Equal("hello", entity.Name);
        Assert.Equal(9.99m, entity.Price);
        Assert.Equal(Status.Active, entity.Status);
        Assert.True(entity.IsActive);
        Assert.Equal(g, entity.Guid);
        Assert.Null(entity.Created);
    }

    [Fact]
    public async Task SourceGen_OwnedType_ReorderedColumns_ValuesCorrect()
    {
        // Owned test with columns in reverse of alphabetical order: Id, Address_Street, Address_City
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 7 AS Id, 'Oak St' AS Address_Street, 'Springfield' AS Address_City";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var mat = CompiledMaterializerStore.Get<MaterializedOwned>();
        var entity = await mat(reader, CancellationToken.None);

        Assert.Equal(7, entity.Id);
        Assert.Equal("Oak St", entity.Address.Street);
        Assert.Equal("Springfield", entity.Address.City);
    }

    [Fact]
    public async Task SourceGen_ExactAlphabeticalOrder_StillCorrect()
    {
        // Regression guard: alphabetical order must still work after the fix
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var g = Guid.NewGuid();
        cmd.CommandText =
            "SELECT NULL AS Created, $g AS Guid, 1 AS Id, 1 AS IsActive, 'foo' AS Name, 12.34 AS Price, 1 AS Status";
        cmd.Parameters.AddWithValue("$g", g);
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var mat = CompiledMaterializerStore.Get<Materialized>();
        var entity = await mat(reader, CancellationToken.None);

        Assert.Null(entity.Created);
        Assert.Equal(g, entity.Guid);
        Assert.Equal(1, entity.Id);
        Assert.True(entity.IsActive);
        Assert.Equal("foo", entity.Name);
        Assert.Equal(12.34m, entity.Price);
        Assert.Equal(Status.Active, entity.Status);
    }

    // ── FromSqlRawAsync uses compiled materializer; reordered columns must be correct ──

    [Fact]
    public async Task FromSqlRawAsync_ReorderedColumns_ValuesCorrect()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Materialized has no [Table] attribute so convention maps it to table "Materialized"
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Materialized (Id INTEGER, Name TEXT, Price REAL, Created TEXT, Guid TEXT, IsActive INTEGER, Status INTEGER)";
            cmd.ExecuteNonQuery();
            cmd.CommandText =
                "INSERT INTO Materialized VALUES (99, 'raw', 3.14, NULL, '00000000-0000-0000-0000-000000000001', 0, 0)";
            cmd.ExecuteNonQuery();
        }

        // Raw SQL with columns in reverse alphabetical order (Status first, Id last)
        var results = await ctx.FromSqlRawAsync<Materialized>(
            "SELECT Status, Price, Name, IsActive, Guid, Created, Id FROM Materialized");

        Assert.Single(results);
        var e = results[0];
        Assert.Equal(99, e.Id);
        Assert.Equal("raw", e.Name);
        Assert.Equal(3.14m, e.Price);
        Assert.Equal(Status.Inactive, e.Status);
        Assert.False(e.IsActive);
        Assert.Null(e.Created);
    }

    // ── compile-time proof — two [GenerateMaterializer] classes with the same simple name
    //    in different namespaces. If the generator uses only class name as hint, the build fails.
    //    If this file compiled successfully, the hint-name collision fix is working. ─────────────

    [Fact]
    public void SameNameDifferentNamespace_BothRegistered()
    {
        // nORM.Tests.Materialized registered by nORM.Tests namespace entity
        Assert.True(CompiledMaterializerStore.TryGet(typeof(Materialized), out _),
            "nORM.Tests.Materialized must be registered");

        // nORM.Tests.Ns2.Materialized is the same class name in a different namespace
        Assert.True(CompiledMaterializerStore.TryGet(typeof(Ns2.Materialized), out _),
            "nORM.Tests.Ns2.Materialized must also be registered (SG2 fix)");
    }
}
