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
/// Contracts for scaffold identifier mapping-back and relationship column fidelity (scaffolding
/// matrix cells). Sanitized C# identifiers are only safe if the generated attributes still map
/// back to the ORIGINAL database names - a renamed identifier without a mapping attribute
/// compiles and then silently targets a nonexistent table or column. And a scaffolded foreign
/// key must carry its dependent column faithfully.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldIdentifierAndRelationshipFidelityTests
{
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
            "ScaffoldFidelity_" + Guid.NewGuid().ToString("N"),
            trees,
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        using var ms = new MemoryStream();
        var emit = compilation.Emit(ms);
        Assert.True(emit.Success, "scaffolded output failed to compile:\n" +
            string.Join("\n", emit.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).Take(10)));
        return Assembly.Load(ms.ToArray());
    }

    private static async Task<Assembly> ScaffoldAndCompileAsync(SqliteConnection cn, string ns)
    {
        var dir = Path.Combine(Path.GetTempPath(), "norm_scaffold_fid_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, ns, ns + "Ctx");
            return CompileScaffoldOutput(dir);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Sanitized_identifiers_map_back_to_the_original_database_names()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            // A table name needing sanitization and columns hitting a C# keyword and a
            // leading digit.
            cmd.CommandText = """
                CREATE TABLE "Order Details" (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    "class" TEXT NOT NULL,
                    "2ndQuantity" INTEGER NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }

        var assembly = await ScaffoldAndCompileAsync(cn, "IdentNs");

        // The rebuilt snapshot must target the ORIGINAL names - that is the mapping-back
        // contract the sanitized C# identifiers depend on.
        var snapshot = SchemaSnapshotBuilder.Build(assembly);
        var table = snapshot.Tables.SingleOrDefault(t => t.Name == "Order Details");
        Assert.True(table is not null, "scaffolded entity lost the original table name; snapshot tables: "
            + string.Join(", ", snapshot.Tables.Select(t => t.Name)));
        Assert.Contains(table!.Columns, c => c.Name == "class");
        Assert.Contains(table.Columns, c => c.Name == "2ndQuantity");
    }

    [Fact]
    public async Task Scaffolded_foreign_key_carries_its_dependent_column_faithfully()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Author (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL
                );
                CREATE TABLE Book (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Title TEXT NOT NULL,
                    AuthorId INTEGER NOT NULL REFERENCES Author(Id)
                );
                """;
            cmd.ExecuteNonQuery();
        }

        var assembly = await ScaffoldAndCompileAsync(cn, "RelNs");

        var book = assembly.GetTypes().Single(t => t.Name == "Book");
        // The FK column survives as a scalar property, and a navigation to the principal exists.
        Assert.NotNull(book.GetProperty("AuthorId"));
        var nav = book.GetProperties().SingleOrDefault(p => p.PropertyType.Name == "Author");
        Assert.True(nav is not null, "scaffolded dependent lost its principal navigation; properties: "
            + string.Join(", ", book.GetProperties().Select(p => $"{p.PropertyType.Name} {p.Name}")));

        var author = assembly.GetTypes().Single(t => t.Name == "Author");
        var inverse = author.GetProperties().SingleOrDefault(p =>
            p.PropertyType.IsGenericType && p.PropertyType.GetGenericArguments()[0].Name == "Book");
        Assert.True(inverse is not null, "scaffolded principal lost its collection navigation; properties: "
            + string.Join(", ", author.GetProperties().Select(p => $"{p.PropertyType.Name} {p.Name}")));
    }
}
