using System;
using Microsoft.Data.Sqlite;
using nORM.Mapping;
using nORM.SourceGeneration;
using Xunit;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Tests
{
    internal enum Status { Inactive = 0, Active = 1 }

    [GenerateMaterializer]
    internal class Materialized
    {
        public DateTime? Created { get; set; }
        public Guid Guid { get; set; }
        public int Id { get; set; }
        public bool? IsActive { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public Status Status { get; set; }
    }

    [Owned]
    internal class MatAddress
    {
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
    }

    [GenerateMaterializer]
    internal class MaterializedOwned
    {
        public int Id { get; set; }
        public MatAddress Address { get; set; } = new();
    }

    // SG2 fix verification: two classes with the same name in different namespaces.
    // If SG2 is broken (hint-name collision), this file will not compile.
}

namespace nORM.Tests.Ns2
{
    [nORM.SourceGeneration.GenerateMaterializer]
    internal class Materialized   // same simple name as nORM.Tests.Materialized — must not collide
    {
        public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}

namespace nORM.Tests
{
    // SG3 verification: nested classes with the same simple name in the same
    // namespace must generate unique helper class names.
    internal static class NestedMaterializerContainerA
    {
        [GenerateMaterializer]
        internal class DuplicateNested
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }

    internal static class NestedMaterializerContainerB
    {
        [GenerateMaterializer]
        internal class DuplicateNested
        {
            public int Id { get; set; }
            public string Tag { get; set; } = string.Empty;
        }
    }

    [Xunit.Trait("Category", "Fast")]
    public class MaterializerGeneratorTests
    {
        [Fact]
        public async Task Generated_materializer_reads_data_correctly()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(Materialized), out _));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var cmd = cn.CreateCommand();
            var g = Guid.NewGuid();
            cmd.CommandText = "SELECT NULL AS Created, $g AS Guid, 1 AS Id, 1 AS IsActive, 'foo' AS Name, 12.34 AS Price, 1 AS Status";
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

        [Fact]
        public async Task Generated_materializer_reads_owned_data_correctly()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(MaterializedOwned), out _));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 'Metro' AS Address_City, 'Main' AS Address_Street, 1 AS Id";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<MaterializedOwned>();
            var entity = await mat(reader, CancellationToken.None);
            Assert.NotNull(entity.Address);
            Assert.Equal("Main", entity.Address.Street);
            Assert.Equal("Metro", entity.Address.City);
            Assert.Equal(1, entity.Id);
        }

        [Fact]
        public async Task Nested_generated_materializers_with_same_simple_name_do_not_collide()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(NestedMaterializerContainerA.DuplicateNested), out _));
            Assert.True(CompiledMaterializerStore.TryGet(typeof(NestedMaterializerContainerB.DuplicateNested), out _));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();

            using var cmdA = cn.CreateCommand();
            cmdA.CommandText = "SELECT 1 AS Id, 'alpha' AS Name";
            using var readerA = cmdA.ExecuteReader();
            Assert.True(readerA.Read());
            var matA = CompiledMaterializerStore.Get<NestedMaterializerContainerA.DuplicateNested>();
            var entityA = await matA(readerA, CancellationToken.None);
            Assert.Equal(1, entityA.Id);
            Assert.Equal("alpha", entityA.Name);

            using var cmdB = cn.CreateCommand();
            cmdB.CommandText = "SELECT 2 AS Id, 'beta' AS Tag";
            using var readerB = cmdB.ExecuteReader();
            Assert.True(readerB.Read());
            var matB = CompiledMaterializerStore.Get<NestedMaterializerContainerB.DuplicateNested>();
            var entityB = await matB(readerB, CancellationToken.None);
            Assert.Equal(2, entityB.Id);
            Assert.Equal("beta", entityB.Tag);
        }
    }
}
