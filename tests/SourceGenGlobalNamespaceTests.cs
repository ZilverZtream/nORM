using System;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.SourceGeneration;
using Xunit;

// SG1: Entity at global namespace — exercises the fix that suppresses "namespace ;" for
// global-namespace types. Before the fix, GenerateMaterializerCode always emitted
// "namespace {ToDisplayString()}" where the global namespace display string is empty,
// producing the invalid declaration "namespace ;" and causing a build failure.

[GenerateMaterializer]
public class SgGlobalNsEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Score { get; set; }
}

namespace nORM.Tests
{
    /// <summary>
    /// SG1: Verifies that [GenerateMaterializer] works on entity types declared in the
    /// global namespace (no namespace declaration). The source generator must emit valid
    /// C# (no "namespace ;" statement) and register the materializer at module init.
    /// </summary>
    public class SourceGenGlobalNamespaceTests
    {
        [Fact]
        public void SgGlobalNsEntity_materializer_is_registered()
        {
            Assert.True(
                CompiledMaterializerStore.TryGet(typeof(SgGlobalNsEntity), out _),
                "Source generator must register a materializer for global-namespace SgGlobalNsEntity.");
        }

        [Fact]
        public async Task SgGlobalNsEntity_materializes_correctly_from_sqlite()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();

            using (var setup = cn.CreateCommand())
            {
                setup.CommandText =
                    "CREATE TABLE SgGlobalNsEntity (Id INTEGER PRIMARY KEY, Name TEXT, Score INTEGER);" +
                    "INSERT INTO SgGlobalNsEntity VALUES (42, 'hello', 99);";
                setup.ExecuteNonQuery();
            }

            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgGlobalNsEntity), out var mat),
                "Materializer must be registered.");

            SgGlobalNsEntity? result = null;
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT Id, Name, Score FROM SgGlobalNsEntity WHERE Id = 42";
            using var reader = cmd.ExecuteReader();
            if (await reader.ReadAsync())
                result = (SgGlobalNsEntity)await mat!(reader, default);

            Assert.NotNull(result);
            Assert.Equal(42, result.Id);
            Assert.Equal("hello", result.Name);
            Assert.Equal(99, result.Score);
        }

        [Fact]
        public async Task SgGlobalNsEntity_round_trips_multiple_rows()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();

            using (var setup = cn.CreateCommand())
            {
                setup.CommandText =
                    "CREATE TABLE SgGlobalNsEntity (Id INTEGER PRIMARY KEY, Name TEXT, Score INTEGER);" +
                    "INSERT INTO SgGlobalNsEntity VALUES (1, 'alpha', 10);" +
                    "INSERT INTO SgGlobalNsEntity VALUES (2, 'beta', 20);";
                setup.ExecuteNonQuery();
            }

            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgGlobalNsEntity), out var mat));

            var results = new System.Collections.Generic.List<SgGlobalNsEntity>();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT Id, Name, Score FROM SgGlobalNsEntity ORDER BY Id";
            using var reader = cmd.ExecuteReader();
            while (await reader.ReadAsync())
                results.Add((SgGlobalNsEntity)await mat!(reader, default));

            Assert.Equal(2, results.Count);
            Assert.Equal(1, results[0].Id);
            Assert.Equal("alpha", results[0].Name);
            Assert.Equal(2, results[1].Id);
            Assert.Equal(20, results[1].Score);
        }
    }
}
