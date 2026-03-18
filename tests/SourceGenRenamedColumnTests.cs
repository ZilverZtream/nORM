using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Mapping;
using nORM.SourceGeneration;
using Xunit;

// SG1 regression: source generator must use [Column("db_name")] attribute value
// for GetOrdinal calls rather than the C# property name.
// Without the fix, reader.GetOrdinal("Name") throws when the SQL uses "user_name".

namespace nORM.Tests.SgRename
{
    // Entity with [Column] renames on scalar properties.
    [GenerateMaterializer]
    internal class RenamedEntity
    {
        [Column("entity_id")]
        public int Id { get; set; }

        [Column("user_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_at")]
        public DateTime? CreatedAt { get; set; }

        // Property without [Column] — must still work with property name.
        public decimal Price { get; set; }
    }

    // Owned value-object with column renames on the nested properties.
    [Owned]
    internal class RenamedAddr
    {
        [Column("st")]
        public string Street { get; set; } = string.Empty;

        [Column("cty")]
        public string City { get; set; } = string.Empty;
    }

    // Owner entity whose nav property also carries a [Column] prefix rename.
    [GenerateMaterializer]
    internal class RenamedOwner
    {
        public int Id { get; set; }

        // [Column("loc")] on the owner nav → owned columns become loc_st / loc_cty
        [Column("loc")]
        public RenamedAddr Location { get; set; } = new();
    }

    public class SourceGenRenamedColumnTests
    {
        // ── Helper ────────────────────────────────────────────────────────────

        private static SqliteDataReader ExecuteSql(SqliteConnection cn, string sql)
        {
            var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteReader();
        }

        // ── SG1-1: materializer is registered for [GenerateMaterializer] class ──

        [Fact]
        public void RenamedEntity_materializer_is_registered()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(RenamedEntity), out _),
                "Source generator must register a materializer for RenamedEntity.");
        }

        [Fact]
        public void RenamedOwner_materializer_is_registered()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(RenamedOwner), out _),
                "Source generator must register a materializer for RenamedOwner.");
        }

        // ── SG1-2: scalar [Column] rename is used for GetOrdinal ─────────────

        [Fact]
        public async Task Scalar_renamed_columns_materialize_correctly()
        {
            // SQL supplies DB column names — materializer must call GetOrdinal("entity_id")
            // etc., NOT GetOrdinal("Id") which would throw IndexOutOfRange.
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            var d = new DateTime(2025, 6, 1);
            using var reader = ExecuteSql(cn,
                $"SELECT 7 AS entity_id, 'alice' AS user_name, '{d:yyyy-MM-dd}' AS created_at, 9.99 AS Price");

            Assert.True(reader.Read());

            var mat = CompiledMaterializerStore.Get<RenamedEntity>();
            var entity = await mat(reader, CancellationToken.None);

            Assert.Equal(7, entity.Id);
            Assert.Equal("alice", entity.Name);
            Assert.NotNull(entity.CreatedAt);
            Assert.Equal(d.Date, entity.CreatedAt!.Value.Date);
            Assert.Equal(9.99m, entity.Price);
        }

        [Fact]
        public async Task Scalar_Id_without_Column_attribute_still_works()
        {
            // Price has no [Column] attribute — must still map correctly.
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var reader = ExecuteSql(cn,
                "SELECT 1 AS entity_id, 'bob' AS user_name, NULL AS created_at, 3.50 AS Price");

            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<RenamedEntity>();
            var entity = await mat(reader, CancellationToken.None);

            Assert.Equal(3.50m, entity.Price);
            Assert.Null(entity.CreatedAt);
        }

        // ── SG1-3: null value via renamed column is handled correctly ─────────

        [Fact]
        public async Task Null_renamed_nullable_column_maps_to_null()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var reader = ExecuteSql(cn,
                "SELECT 2 AS entity_id, 'charlie' AS user_name, NULL AS created_at, 0.00 AS Price");

            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<RenamedEntity>();
            var entity = await mat(reader, CancellationToken.None);

            Assert.Null(entity.CreatedAt);
        }

        // ── SG1-4: owned type with column renames on owner nav and nested props ─

        [Fact]
        public async Task Owned_type_with_renamed_columns_materializes_correctly()
        {
            // Owner nav [Column("loc")] + nested [Column("st")] / [Column("cty")]
            // → DB columns are loc_st and loc_cty.
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var reader = ExecuteSql(cn,
                "SELECT 5 AS Id, '123 Main St' AS loc_st, 'Springfield' AS loc_cty");

            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<RenamedOwner>();
            var entity = await mat(reader, CancellationToken.None);

            Assert.Equal(5, entity.Id);
            Assert.NotNull(entity.Location);
            Assert.Equal("123 Main St", entity.Location.Street);
            Assert.Equal("Springfield", entity.Location.City);
        }

        [Fact]
        public async Task Owned_type_null_nested_columns_leaves_owner_null_or_default()
        {
            // Null nested columns should result in default (empty) owned object
            // or null — the materializer only sets properties that are non-NULL.
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var reader = ExecuteSql(cn,
                "SELECT 6 AS Id, NULL AS loc_st, NULL AS loc_cty");

            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<RenamedOwner>();
            var entity = await mat(reader, CancellationToken.None);

            Assert.Equal(6, entity.Id);
            // Either null or uninitialized owned object — no crash is the key assertion.
        }

        // ── SG1-5: wrong column name in SQL throws (negative test) ────────────

        [Fact]
        public async Task Wrong_column_name_throws_IndexOutOfRange()
        {
            // If the fix were absent, the materializer would try GetOrdinal("Name")
            // on a result set that only has "user_name" — and that should throw.
            // We verify the *correct* column names work; using property names fails.
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            // Deliberately supply the raw property name — must fail.
            using var reader = ExecuteSql(cn,
                "SELECT 1 AS entity_id, 'dave' AS Name, NULL AS created_at, 1.00 AS Price");

            Assert.True(reader.Read());
            var mat = CompiledMaterializerStore.Get<RenamedEntity>();

            // GetOrdinal("user_name") will fail since the column is called "Name".
            await Assert.ThrowsAnyAsync<Exception>(() => mat(reader, CancellationToken.None));
        }

        // ── SG1-6: multiple renamed entities in same assembly coexist ─────────

        [Fact]
        public async Task Two_renamed_entities_coexist_without_collision()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(RenamedEntity), out _));
            Assert.True(CompiledMaterializerStore.TryGet(typeof(RenamedOwner), out _));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();

            using var r1 = ExecuteSql(cn,
                "SELECT 10 AS entity_id, 'eve' AS user_name, NULL AS created_at, 5.00 AS Price");
            Assert.True(r1.Read());
            var e1 = await CompiledMaterializerStore.Get<RenamedEntity>()(r1, CancellationToken.None);
            Assert.Equal("eve", e1.Name);

            using var r2 = ExecuteSql(cn,
                "SELECT 11 AS Id, 'Road' AS loc_st, 'Metropolis' AS loc_cty");
            Assert.True(r2.Read());
            var e2 = await CompiledMaterializerStore.Get<RenamedOwner>()(r2, CancellationToken.None);
            Assert.Equal("Metropolis", e2.Location.City);
        }
    }
}
