#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
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

    [Fact]
    public async Task ScaffoldAsync_WithViewTableFilter_GeneratesQueryArtifact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Existing (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
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
                new ScaffoldOptions { Tables = new[] { "ExistingView" } });

            var viewCode = File.ReadAllText(Path.Combine(dir, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));

            Assert.Contains("[Table(\"ExistingView\")]", viewCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.DoesNotContain("View ExistingView", warnings);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithEmitViewEntities_GeneratesQueryArtifact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Existing (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
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
                "ViewCtx",
                new ScaffoldOptions { Tables = new[] { "ExistingView" }, EmitViewEntities = true });

            var viewCode = File.ReadAllText(Path.Combine(dir, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ViewCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"ExistingView\")]", viewCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("public long Id { get; set; }", viewCode);
            Assert.Contains("public string", viewCode);
            Assert.Contains("Name { get; set; }", viewCode);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.DoesNotContain("View ExistingView", warnings);
            Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "ExistingView");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSchemaFilter_IncludesQueryArtifactsInSchema()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Existing (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
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
                "SchemaViewCtx",
                new ScaffoldOptions { Schemas = new[] { "main" } });

            var tableCode = File.ReadAllText(Path.Combine(dir, "Existing.cs"));
            var viewCode = File.ReadAllText(Path.Combine(dir, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaViewCtx.cs"));

            Assert.Contains("[Table(\"Existing\")]", tableCode);
            Assert.Contains("[Table(\"ExistingView\")]", viewCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("IQueryable<Existing> Existings", contextCode);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoOverwrite_RefusesExistingFiles()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ExistingFile (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "ExistingFile.cs"), "// owned");

            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "NoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));
            Assert.Contains("already exists", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDryRun_DoesNotCreateOrWriteOutputDirectory()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DryRunItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_dry_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "DryRunCtx",
                new ScaffoldOptions { DryRun = true });

            Assert.False(Directory.Exists(dir));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDryRun_DoesNotRemoveStaleWarningReports()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DryRunClean (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_dry_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            File.WriteAllText(warningPath, "# stale");

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "DryRunCtx",
                new ScaffoldOptions { DryRun = true });

            Assert.Equal("# stale", File.ReadAllText(warningPath));
            Assert.False(File.Exists(Path.Combine(dir, "DryRunClean.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "DryRunCtx.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoOverwrite_PreflightsAllFilesBeforeWriting()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AlphaNoOverwrite (Id INTEGER PRIMARY KEY);
            CREATE TABLE BetaNoOverwrite (Id INTEGER PRIMARY KEY);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "BetaNoOverwrite.cs"), "// owned");

            await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "NoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));

            Assert.False(File.Exists(Path.Combine(dir, "AlphaNoOverwrite.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "NoOverwriteCtx.cs")));
            Assert.Equal("// owned", File.ReadAllText(Path.Combine(dir, "BetaNoOverwrite.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_RepeatedRuns_ProduceDeterministicOutput()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE ZetaDeterministic (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL DEFAULT 'z'
            );
            CREATE TABLE AlphaDeterministic (
                Id INTEGER PRIMARY KEY,
                ZetaId INTEGER NOT NULL,
                Value TEXT NOT NULL,
                CONSTRAINT FK_Alpha_Zeta FOREIGN KEY (ZetaId) REFERENCES ZetaDeterministic(Id)
            );
            CREATE INDEX IX_Alpha_Value ON AlphaDeterministic(Value);
            """;
        cmd.ExecuteNonQuery();

        var first = Path.Combine(Path.GetTempPath(), "san_scaffold_det_a_" + Guid.NewGuid().ToString("N"));
        var second = Path.Combine(Path.GetTempPath(), "san_scaffold_det_b_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), first, "TestNs", "DeterministicCtx");
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), second, "TestNs", "DeterministicCtx");

            Assert.Equal(ReadScaffoldSnapshot(first), ReadScaffoldSnapshot(second));
        }
        finally
        {
            if (Directory.Exists(first)) Directory.Delete(first, recursive: true);
            if (Directory.Exists(second)) Directory.Delete(second, recursive: true);
        }
    }

    private static void AssertScaffoldOutputBuildsAsConsumerProject(string outputDirectory)
    {
        var root = FindRepositoryRoot();
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(Path.Combine(outputDirectory, "ScaffoldedConsumer.csproj"), $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <ImplicitUsings>disable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);

        var psi = new ProcessStartInfo("dotnet", "build -c Release --nologo")
        {
            WorkingDirectory = outputDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start dotnet build.");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0,
            $"Scaffolded output failed to build with exit code {process.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{stderr}");
    }

    private static string FindRepositoryRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(dir))
        {
            if (File.Exists(Path.Combine(dir, "nORM.sln")))
                return dir;

            dir = Directory.GetParent(dir)?.FullName;
        }

        throw new InvalidOperationException("Could not locate repository root from " + AppContext.BaseDirectory);
    }

    private static IReadOnlyList<(string RelativePath, string Content)> ReadScaffoldSnapshot(string outputDirectory)
        => Directory.EnumerateFiles(outputDirectory)
            .Select(path => (RelativePath: Path.GetFileName(path), Content: File.ReadAllText(path)))
            .OrderBy(file => file.RelativePath, StringComparer.Ordinal)
            .ToArray();
}
