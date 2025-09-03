using System;
using Microsoft.Data.Sqlite;
using nORM.Mapping;
using nORM.SourceGeneration;
using Xunit;

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

    public class MaterializerGeneratorTests
    {
        [Fact]
        public void Generated_materializer_reads_data_correctly()
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
            var entity = mat(reader);
            Assert.Null(entity.Created);
            Assert.Equal(g, entity.Guid);
            Assert.Equal(1, entity.Id);
            Assert.True(entity.IsActive);
            Assert.Equal("foo", entity.Name);
            Assert.Equal(12.34m, entity.Price);
            Assert.Equal(Status.Active, entity.Status);
        }

        [Fact]
        public void Generated_materializer_reads_owned_data_correctly()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(MaterializedOwned), out _));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 'Metro' AS Address_City, 'Main' AS Address_Street, 1 AS Id";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<MaterializedOwned>();
            var entity = mat(reader);
            Assert.NotNull(entity.Address);
            Assert.Equal("Main", entity.Address.Street);
            Assert.Equal("Metro", entity.Address.City);
            Assert.Equal(1, entity.Id);
        }
    }
}
