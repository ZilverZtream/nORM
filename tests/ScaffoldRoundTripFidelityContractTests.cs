using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract: scaffolded output DESCRIBES the schema it came from (scaffolding matrix cell). The
/// compile-check suite proves scaffolded code compiles; a scaffolder can still emit compiling
/// code that silently drops nullability, mistypes a column, or loses a primary key. This
/// round-trip oracle scaffolds a live SQLite schema with boundary column shapes, compiles the
/// output in-proc, rebuilds a schema from the SCAFFOLDED model, re-creates it in a fresh
/// database, and compares the two databases structurally through PRAGMA table_info.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldRoundTripFidelityContractTests
{
    private sealed record ColumnShape(string Name, string Type, bool NotNull, bool Pk);

    private static List<ColumnShape> Introspect(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info(\"{table}\")";
        using var r = cmd.ExecuteReader();
        var cols = new List<ColumnShape>();
        while (r.Read())
        {
            var pk = r.GetInt32(5) != 0;
            cols.Add(new ColumnShape(
                r.GetString(1),
                r.GetString(2).ToUpperInvariant(),
                // INTEGER PRIMARY KEY is implicitly NOT NULL but PRAGMA reports notnull=0;
                // normalize PK columns so the quirk does not mask real nullability drift.
                r.GetInt32(3) != 0 || pk,
                pk));
        }
        return cols.OrderBy(c => c.Name, StringComparer.Ordinal).ToList();
    }

    private static Assembly CompileScaffoldOutput(string dir)
    {
        var parseOptions = new CSharpParseOptions(LanguageVersion.Latest);
        var trees = Directory.GetFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Select(f => CSharpSyntaxTree.ParseText(File.ReadAllText(f), parseOptions, path: f))
            .ToList();
        Assert.True(trees.Count > 0, "scaffolder wrote no source files");

        var tpa = (string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!;
        var references = tpa.Split(Path.PathSeparator)
            .Where(p => p.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(nORM.Core.DbContext).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(SqliteConnection).Assembly.Location));

        var compilation = CSharpCompilation.Create(
            "ScaffoldRoundTrip_" + Guid.NewGuid().ToString("N"),
            trees,
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        using var ms = new MemoryStream();
        var emit = compilation.Emit(ms);
        Assert.True(emit.Success, "scaffolded output failed to compile:\n" +
            string.Join("\n", emit.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).Take(10)));
        return Assembly.Load(ms.ToArray());
    }

    [Fact]
    public async Task Scaffolded_model_recreates_the_source_schema_structurally()
    {
        // Source database: all four storage classes, NOT NULL variety, autoincrement PK,
        // mixed-case identifiers.
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        using var _cn1 = cn1;
        using (var cmd = cn1.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Album (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Title TEXT NOT NULL,
                    Rating REAL NULL,
                    CoverArt BLOB NULL,
                    PlayCount INTEGER NOT NULL
                );
                CREATE TABLE StudioSession (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    StartedAt TEXT NOT NULL,
                    Notes TEXT NULL,
                    DurationSeconds REAL NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "norm_scaffold_rt_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn1, new SqliteProvider(), dir, "RoundTripNs", "RoundTripCtx");
            var assembly = CompileScaffoldOutput(dir);

            // Rebuild the schema FROM THE SCAFFOLDED MODEL and create it in a fresh database.
            var snapshot = SchemaSnapshotBuilder.Build(assembly);
            var tables = snapshot.Tables
                .Where(t => t.Name is "Album" or "StudioSession")
                .ToList();
            Assert.Equal(2, tables.Count);

            var diff = SchemaDiffer.Diff(new SchemaSnapshot(), new SchemaSnapshot { Tables = { tables[0], tables[1] } });
            var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

            var cn2 = new SqliteConnection("Data Source=:memory:");
            cn2.Open();
            using var _cn2 = cn2;
            foreach (var statement in (sql.PreTransactionUp ?? Enumerable.Empty<string>()).Concat(sql.Up).Concat(sql.PostTransactionUp ?? Enumerable.Empty<string>()))
            {
                using var cmd = cn2.CreateCommand();
                cmd.CommandText = statement;
                cmd.ExecuteNonQuery();
            }

            // Structural comparison: the round-tripped schema must match the source.
            foreach (var table in new[] { "Album", "StudioSession" })
            {
                var source = Introspect(cn1, table);
                var roundTripped = Introspect(cn2, table);
                Assert.Equal(source, roundTripped);
            }
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { }
        }
    }
}
