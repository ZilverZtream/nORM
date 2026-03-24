using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.SourceGeneration;
using Xunit;

// SG2: Entities whose [Column] names contain backslash or double-quote characters.
// Before the fix, the generator emitted reader.GetOrdinal("col\z") — where \z is an
// unrecognised C# escape sequence — causing a build failure. After the fix,
// EscapeCSharpLiteral produces reader.GetOrdinal("col\\z"), which is valid C#.

namespace nORM.Tests.SgEscape
{
    // Backslash in column name: col\z — before fix this generated "col\z" (compile error).
    [GenerateMaterializer]
    internal class SgBackslashColEntity
    {
        [Column(@"col\z")]   // verbatim: the actual column name is col\z
        public int Id { get; set; }

        public string Label { get; set; } = string.Empty;
    }

    // Double-quote in column name: col"q — before fix this generated "col"q" (syntax error).
    [GenerateMaterializer]
    internal class SgQuoteColEntity
    {
        [Column("col\"q")]   // the actual column name is col"q
        public int Id { get; set; }

        public string Value { get; set; } = string.Empty;
    }

    // Both backslash and quote combined.
    [GenerateMaterializer]
    internal class SgCombinedEscapeEntity
    {
        [Column("a\\\"b")]   // actual column name: a\"b
        public int Id { get; set; }
    }

    public class SourceGenColumnEscapingTests
    {
        // ── SG2-1: materializers compile and register ────────────────────────

        [Fact]
        public void Backslash_column_materializer_is_registered()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgBackslashColEntity), out _),
                "Materializer for entity with backslash column name must be registered.");
        }

        [Fact]
        public void Quote_column_materializer_is_registered()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgQuoteColEntity), out _),
                "Materializer for entity with double-quote column name must be registered.");
        }

        [Fact]
        public void Combined_escape_materializer_is_registered()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgCombinedEscapeEntity), out _),
                "Materializer for entity with backslash+quote column name must be registered.");
        }

        // ── SG2-2: backslash column name materializes the correct value ──────

        [Fact]
        public async Task Backslash_column_materializes_correctly()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();

            // SQLite double-quote for identifier quoting; C# verbatim string doubles the outer quotes.
            using (var setup = cn.CreateCommand())
            {
                // Column name in SQL is col\z — quoted with double quotes in SQL
                setup.CommandText =
                    @"CREATE TABLE SgBackslashColEntity (""col\z"" INTEGER PRIMARY KEY, Label TEXT);" +
                    @"INSERT INTO SgBackslashColEntity VALUES (7, 'backslash');";
                setup.ExecuteNonQuery();
            }

            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgBackslashColEntity), out var mat));

            SgBackslashColEntity? entity = null;
            using var cmd = cn.CreateCommand();
            cmd.CommandText = @"SELECT ""col\z"", Label FROM SgBackslashColEntity";
            using var reader = cmd.ExecuteReader();
            if (await reader.ReadAsync())
                entity = (SgBackslashColEntity)await mat!(reader, default);

            Assert.NotNull(entity);
            Assert.Equal(7, entity.Id);
            Assert.Equal("backslash", entity.Label);
        }

        // ── SG2-3: double-quote column name materializes the correct value ───

        [Fact]
        public async Task Quote_column_materializes_correctly()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();

            using (var setup = cn.CreateCommand())
            {
                // Column name col"q: use bracket quoting in SQL to avoid nested quote escaping.
                setup.CommandText =
                    @"CREATE TABLE SgQuoteColEntity ([col""q] INTEGER PRIMARY KEY, Value TEXT);" +
                    @"INSERT INTO SgQuoteColEntity VALUES (3, 'quoted');";
                setup.ExecuteNonQuery();
            }

            Assert.True(CompiledMaterializerStore.TryGet(typeof(SgQuoteColEntity), out var mat));

            SgQuoteColEntity? entity = null;
            using var cmd = cn.CreateCommand();
            cmd.CommandText = @"SELECT [col""q], Value FROM SgQuoteColEntity";
            using var reader = cmd.ExecuteReader();
            if (await reader.ReadAsync())
                entity = (SgQuoteColEntity)await mat!(reader, default);

            Assert.NotNull(entity);
            Assert.Equal(3, entity.Id);
            Assert.Equal("quoted", entity.Value);
        }
    }
}
