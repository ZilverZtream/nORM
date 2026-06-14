#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Table/schema filtering, output behavior, query artifact, and deterministic scaffold tests.

    [Fact]
    public async Task ScaffoldAsync_WithTableFilter_GeneratesOnlyRequestedTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE KeepMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SkipMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "FilteredCtx",
                new ScaffoldOptions { Tables = new[] { "KeepMe" } });

            Assert.True(File.Exists(Path.Combine(dir, "KeepMe.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SkipMe.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredCtx.cs"));
            Assert.Contains("IQueryable<KeepMe> KeepMes", contextCode);
            Assert.DoesNotContain("SkipMes", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSchemaFilter_GeneratesRequestedSchemasAndExplicitTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE MainKeep (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE MainSkip (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxa"."SchemaKeepOne" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxa"."SchemaKeepTwo" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."SchemaSkip" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "SchemaFilteredCtx",
                new ScaffoldOptions
                {
                    Schemas = new[] { "auxa" },
                    Tables = new[] { "MainKeep" }
                });

            Assert.True(File.Exists(Path.Combine(dir, "MainKeep.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SchemaKeepOne.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SchemaKeepTwo.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "MainSkip.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SchemaSkip.cs")));

            var schemaEntityCode = File.ReadAllText(Path.Combine(dir, "SchemaKeepOne.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaFilteredCtx.cs"));
            Assert.Contains("[Table(\"SchemaKeepOne\", Schema = \"auxa\")]", schemaEntityCode);
            Assert.Contains("IQueryable<MainKeep> MainKeeps", contextCode);
            Assert.Contains("IQueryable<SchemaKeepOne> SchemaKeepOnes", contextCode);
            Assert.Contains("IQueryable<SchemaKeepTwo> SchemaKeepTwos", contextCode);
            Assert.DoesNotContain("MainSkips", contextCode);
            Assert.DoesNotContain("SchemaSkips", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMissingSchemaFilter_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Existing (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "MissingSchemaCtx",
                    new ScaffoldOptions { Schemas = new[] { "missing_schema" } }));

            Assert.Contains("schema filter did not match", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("missing_schema", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTableFilter_SuppressesRelationshipsToUnselectedTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE SkipPrincipal (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE KeepDependent (
                Id INTEGER PRIMARY KEY,
                PrincipalId INTEGER NOT NULL,
                CONSTRAINT FK_KeepDependent_SkipPrincipal
                    FOREIGN KEY (PrincipalId) REFERENCES SkipPrincipal(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "FilteredRelationshipCtx",
                new ScaffoldOptions { Tables = new[] { "KeepDependent" } });

            Assert.True(File.Exists(Path.Combine(dir, "KeepDependent.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SkipPrincipal.cs")));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "KeepDependent.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredRelationshipCtx.cs"));
            Assert.DoesNotContain("[ForeignKey(", dependentCode);
            Assert.DoesNotContain("SkipPrincipal", dependentCode);
            Assert.DoesNotContain("SkipPrincipals", contextCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullOrBlankTableFilter_TreatsFilterAsEmpty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE FilterNullSafe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var nullDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var blankDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                nullDir,
                "TestNs",
                "NullFilterCtx",
                new ScaffoldOptions { Tables = null! });

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                blankDir,
                "TestNs",
                "BlankFilterCtx",
                new ScaffoldOptions { Tables = new[] { " ", "" } });

            Assert.True(File.Exists(Path.Combine(nullDir, "FilterNullSafe.cs")));
            Assert.True(File.Exists(Path.Combine(blankDir, "FilterNullSafe.cs")));
        }
        finally
        {
            if (Directory.Exists(nullDir)) Directory.Delete(nullDir, recursive: true);
            if (Directory.Exists(blankDir)) Directory.Delete(blankDir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithAmbiguousBareTableFilter_RequiresSchemaQualifiedName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE "auxa"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var ambiguousDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var qualifiedDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    ambiguousDir,
                    "TestNs",
                    "AmbiguousFilterCtx",
                    new ScaffoldOptions { Tables = new[] { "DuplicateName" } }));

            Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("auxa.DuplicateName", ex.Message, StringComparison.Ordinal);
            Assert.Contains("auxb.DuplicateName", ex.Message, StringComparison.Ordinal);
            Assert.Contains("schema-qualified", ex.Message, StringComparison.OrdinalIgnoreCase);

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                qualifiedDir,
                "TestNs",
                "QualifiedFilterCtx",
                new ScaffoldOptions { Tables = new[] { "auxa.DuplicateName" } });

            var entityCode = File.ReadAllText(Path.Combine(qualifiedDir, "DuplicateName.cs"));
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxa\")]", entityCode);
        }
        finally
        {
            if (Directory.Exists(ambiguousDir)) Directory.Delete(ambiguousDir, recursive: true);
            if (Directory.Exists(qualifiedDir)) Directory.Delete(qualifiedDir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSameTableNameAcrossSchemas_UsesSchemaQualifiedEntityNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE "auxa"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SchemaDuplicateCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuxaDuplicateName.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "AuxbDuplicateName.cs")));
            var auxaCode = File.ReadAllText(Path.Combine(dir, "AuxaDuplicateName.cs"));
            var auxbCode = File.ReadAllText(Path.Combine(dir, "AuxbDuplicateName.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaDuplicateCtx.cs"));
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxa\")]", auxaCode);
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxb\")]", auxbCode);
            Assert.Contains("IQueryable<AuxaDuplicateName> AuxaDuplicateNames", contextCode);
            Assert.Contains("IQueryable<AuxbDuplicateName> AuxbDuplicateNames", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithLiteralDottedTableFilterCollision_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "DottedFilterCtx",
                    new ScaffoldOptions { Tables = new[] { "aux.orders" } }));

            Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("literal dotted table names", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelectedTableKeyCollision_ThrowsBeforeGeneratingAmbiguousModel()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionCtx"));

            Assert.Contains("display names collide", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("literal dotted table names", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMissingTableFilter_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Existing (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "FilteredCtx",
                    new ScaffoldOptions { Tables = new[] { "Missing" } }));
            Assert.Contains("Missing", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
