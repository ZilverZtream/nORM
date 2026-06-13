#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Provider diagnostics and query artifact scaffold integration tests.

    [Fact]
    public async Task ScaffoldAsync_WithProviderOwnedSchemaFeatures_EmitsDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FeatureOwned (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT COLLATE NOCASE NOT NULL DEFAULT 'new',
                NameLength INTEGER GENERATED ALWAYS AS (length(Name)) VIRTUAL,
                NameLengthStored INTEGER GENERATED ALWAYS AS (length(Name) + 1) STORED,
                "Display,Name" TEXT COLLATE NOCASE NOT NULL,
                "Display,Length" INTEGER GENERATED ALWAYS AS (length("Display,Name")) VIRTUAL,
                "Paren)Name" TEXT COLLATE NOCASE NOT NULL,
                "Paren)Length" INTEGER GENERATED ALWAYS AS (length("Paren)Name")) VIRTUAL,
                CONSTRAINT CK_FeatureOwned_Name CHECK (length(Name) > 0)
            );
            CREATE TRIGGER TR_FeatureOwned_Audit AFTER INSERT ON FeatureOwned BEGIN SELECT 1; END;
            CREATE VIEW FeatureOwnedView AS SELECT Id, Name FROM FeatureOwned;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FeatureOwnedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "FeatureOwned.cs"));
            var viewCode = File.ReadAllText(Path.Combine(dir, "FeatureOwnedView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FeatureOwnedCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode);
            Assert.Contains("[ReadOnlyEntity]", entityCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("[Table(\"FeatureOwnedView\")]", viewCode);
            Assert.Contains("IQueryable<FeatureOwnedView> FeatureOwnedViews", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.Name).HasDefaultValueSql(\"'new'\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().HasCheckConstraint(\"CK_FeatureOwned_Name\", \"length(Name) > 0\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.NameLength).HasComputedColumnSql(\"length(Name)\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.NameLengthStored).HasComputedColumnSql(\"length(Name) + 1\", stored: true);", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.Name).HasCollation(\"NOCASE\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.DisplayLength).HasComputedColumnSql(\"length(\\\"Display,Name\\\")\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.DisplayName).HasCollation(\"NOCASE\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.ParenLength).HasComputedColumnSql(\"length(\\\"Paren)Name\\\")\");", contextCode);
            Assert.Contains("mb.Entity<FeatureOwned>().Property(e => e.ParenName).HasCollation(\"NOCASE\");", contextCode);
            Assert.Contains("Provider-Owned Schema Features", warnings);
            Assert.DoesNotContain("Composite Foreign Keys", warnings);
            Assert.DoesNotContain("| SCF100 |", warnings);
            Assert.DoesNotContain("| SCF101 |", warnings);
            Assert.DoesNotContain("NameLength", warnings);
            Assert.DoesNotContain("| SCF103 |", warnings);
            Assert.Contains("Trigger", warnings);
            Assert.Contains("TR_FeatureOwned_Audit", warnings);
            Assert.DoesNotContain("CheckConstraint", warnings);
            Assert.DoesNotContain("Skipped Database Objects", warnings);
            Assert.Contains("FeatureOwnedView", warnings);
            var summary = warningJson.RootElement.GetProperty("summary");
            Assert.Equal(2, summary.GetProperty("totalWarnings").GetInt32());
            Assert.Equal(2, summary.GetProperty("sectionCounts").GetProperty("providerOwnedSchemaFeatures").GetInt32());
            Assert.Equal(0, summary.GetProperty("sectionCounts").GetProperty("skippedDatabaseObjects").GetInt32());
            Assert.False(summary.GetProperty("codes").TryGetProperty("SCF100", out _));
            Assert.Equal(1, summary.GetProperty("codes").GetProperty("SCF116").GetInt32());
            Assert.False(summary.GetProperty("categories").TryGetProperty("schema-feature", out _));
            Assert.Equal(1, summary.GetProperty("categories").GetProperty("database-object").GetInt32());
            Assert.Equal(1, summary.GetProperty("categories").GetProperty("table-shape").GetInt32());
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Default");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Computed");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Collation");
            var triggerDiagnostic = Assert.Single(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Trigger" && item.GetProperty("name").GetString() == "TR_FeatureOwned_Audit");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "CheckConstraint");
            Assert.Equal("SCF110", triggerDiagnostic.GetProperty("code").GetString());
            Assert.Equal("database-object", triggerDiagnostic.GetProperty("category").GetString());
            var triggerMetadata = triggerDiagnostic.GetProperty("metadata");
            Assert.Equal("SQLite", triggerMetadata.GetProperty("provider").GetString());
            Assert.Equal("Trigger", triggerMetadata.GetProperty("providerObjectKind").GetString());
            Assert.Equal("FeatureOwned", triggerMetadata.GetProperty("table").GetString());
            Assert.Equal("TR_FeatureOwned_Audit", triggerMetadata.GetProperty("triggerName").GetString());
            Assert.True(triggerMetadata.GetProperty("providerOwnedDdl").GetBoolean());
            Assert.False(triggerMetadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
            Assert.True(triggerMetadata.GetProperty("readOnlyEntity").GetBoolean());
            Assert.False(triggerMetadata.GetProperty("generatedWritesSupported").GetBoolean());
            Assert.Equal("provider-owned-trigger", triggerMetadata.GetProperty("reason").GetString());
            Assert.True(triggerMetadata.GetProperty("definitionAvailable").GetBoolean());
            Assert.Contains("CREATE TRIGGER TR_FeatureOwned_Audit", triggerMetadata.GetProperty("triggerSql").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "FeatureOwnedView" &&
                item.GetProperty("code").GetString() == "SCF116" &&
                item.GetProperty("category").GetString() == "table-shape");
            Assert.All(providerOwned.EnumerateArray(), item => Assert.Equal("Warning", item.GetProperty("severity").GetString()));
            Assert.All(providerOwned.EnumerateArray(), item => Assert.False(string.IsNullOrWhiteSpace(item.GetProperty("suggestedAction").GetString())));
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Empty(skippedObjects.EnumerateArray());
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteProviderSpecificDeclaredTypes_EmitsDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ProviderTyped (
                Id INTEGER PRIMARY KEY,
                Payload JSON NOT NULL,
                ExternalUuid UUID NOT NULL,
                XmlPayload XML NULL,
                Shape GEOMETRY NULL,
                ShapeId GEOMETRY_UUID NULL,
                PortableText TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ProviderTypedCtx");

            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            var entityCode = File.ReadAllText(Path.Combine(dir, "ProviderTyped.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("ProviderSpecificColumnType", warnings);
            Assert.DoesNotContain("Payload |", warnings);
            Assert.DoesNotContain("ExternalUuid |", warnings);
            Assert.DoesNotContain("XmlPayload |", warnings);
            Assert.Contains("Shape", warnings);
            Assert.Contains("ShapeId", warnings);
            Assert.DoesNotContain("PortableText |", warnings);
            Assert.Contains("using System;", entityCode);
            Assert.Contains("using nORM.Configuration;", entityCode);
            Assert.Contains("[ReadOnlyEntity]", entityCode);
            Assert.Contains("public string Payload { get; set; } = default!;", entityCode);
            Assert.Contains("public Guid ExternalUuid { get; set; }", entityCode);
            Assert.Contains("public string? XmlPayload { get; set; }", entityCode);
            Assert.Contains("public string? ShapeId { get; set; }", entityCode);
            Assert.DoesNotContain("public Guid? ShapeId { get; set; }", entityCode);
            var dynamicType = new DynamicEntityTypeGenerator().GenerateEntityType(cn, "ProviderTyped");
            Assert.Equal(typeof(string), dynamicType.GetProperty("Payload")!.PropertyType);
            Assert.Equal(typeof(Guid), dynamicType.GetProperty("ExternalUuid")!.PropertyType);
            Assert.Equal(typeof(string), dynamicType.GetProperty("XmlPayload")!.PropertyType);
            Assert.Equal(typeof(string), dynamicType.GetProperty("ShapeId")!.PropertyType);
            Assert.NotNull(dynamicType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());

            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("name").GetString() == "Shape" &&
                item.GetProperty("category").GetString() == "schema-feature" &&
                item.GetProperty("metadata").GetProperty("providerType").GetString() == "GEOMETRY" &&
                item.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean() &&
                !item.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean() &&
                item.GetProperty("metadata").GetProperty("reason").GetString() == "provider-specific-column-type");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("name").GetString() == "ShapeId" &&
                item.GetProperty("metadata").GetProperty("providerType").GetString() == "GEOMETRY_UUID" &&
                item.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean() &&
                !item.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSafeSqliteProviderSpecificDeclaredTypes_MapsStaticAndDynamicWritableTypes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SafeProviderTyped (
                Id INTEGER PRIMARY KEY,
                Payload JSON NOT NULL,
                ExternalUuid UUID NOT NULL,
                XmlPayload XML NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SafeProviderTypedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "SafeProviderTyped.cs"));
            Assert.Contains("using System;", entityCode);
            Assert.DoesNotContain("using nORM.Configuration;", entityCode);
            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode);
            Assert.Contains("public string Payload { get; set; } = default!;", entityCode);
            Assert.Contains("public Guid ExternalUuid { get; set; }", entityCode);
            Assert.Contains("public string? XmlPayload { get; set; }", entityCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            var dynamicType = new DynamicEntityTypeGenerator().GenerateEntityType(cn, "SafeProviderTyped");
            Assert.Equal(typeof(string), dynamicType.GetProperty("Payload")!.PropertyType);
            Assert.Equal(typeof(Guid), dynamicType.GetProperty("ExternalUuid")!.PropertyType);
            Assert.Equal(typeof(string), dynamicType.GetProperty("XmlPayload")!.PropertyType);
            Assert.Null(dynamicType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteTemporalDeclaredTypes_MapsStoreTypesInStaticAndDynamicScaffolding()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TemporalStoreTyped (
                Id INTEGER PRIMARY KEY,
                BusinessDate DATE NOT NULL,
                StartsAt TIME NULL,
                CreatedAt DATETIME NOT NULL,
                OffsetAt DATETIMEOFFSET NULL,
                ExternalUuid UUID NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "TemporalStoreTypedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "TemporalStoreTyped.cs"));
            Assert.Contains("public DateOnly BusinessDate { get; set; }", entityCode);
            Assert.Contains("public TimeOnly? StartsAt { get; set; }", entityCode);
            Assert.Contains("public DateTime CreatedAt { get; set; }", entityCode);
            Assert.Contains("public DateTimeOffset? OffsetAt { get; set; }", entityCode);
            Assert.Contains("public Guid ExternalUuid { get; set; }", entityCode);

            var dynamicType = new DynamicEntityTypeGenerator().GenerateEntityType(cn, "TemporalStoreTyped");
            Assert.Equal(typeof(DateOnly), dynamicType.GetProperty("BusinessDate")!.PropertyType);
            Assert.Equal(typeof(TimeOnly?), dynamicType.GetProperty("StartsAt")!.PropertyType);
            Assert.Equal(typeof(DateTime), dynamicType.GetProperty("CreatedAt")!.PropertyType);
            Assert.Equal(typeof(DateTimeOffset?), dynamicType.GetProperty("OffsetAt")!.PropertyType);
            Assert.Equal(typeof(Guid), dynamicType.GetProperty("ExternalUuid")!.PropertyType);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUnmodeledDefault_MarksTypeReadOnly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DefaultOwned (
                Id INTEGER PRIMARY KEY,
                Status TEXT NOT NULL DEFAULT (lower('NEW'))
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DefaultOwnedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "DefaultOwned.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "DefaultOwnedCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[ReadOnlyEntity]", entityCode);
            Assert.DoesNotContain("HasDefaultValueSql", contextCode, StringComparison.Ordinal);
            var defaultDiagnostic = Assert.Single(
                warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(),
                item => item.GetProperty("kind").GetString() == "Default" &&
                        item.GetProperty("name").GetString() == "Status");
            Assert.Equal("SCF100", defaultDiagnostic.GetProperty("code").GetString());
            var metadata = defaultDiagnostic.GetProperty("metadata");
            Assert.Contains("lower('NEW')", metadata.GetProperty("defaultSql").GetString(), StringComparison.Ordinal);
            Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
            Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
            Assert.Equal("provider-specific-default", metadata.GetProperty("reason").GetString());
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteBinaryLiteralDefault_PromotesDefaultAndKeepsTypeWritable()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE BinaryDefaultOwned (
                Id INTEGER PRIMARY KEY,
                Payload BLOB NOT NULL DEFAULT X'DEADBEEF'
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "BinaryDefaultOwnedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "BinaryDefaultOwned.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "BinaryDefaultOwnedCtx.cs"));

            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains("public byte[] Payload { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
            Assert.Contains("mb.Entity<BinaryDefaultOwned>().Property(e => e.Payload).HasDefaultValueSql(\"X'DEADBEEF'\");", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var dynamicType = new DynamicEntityTypeGenerator().GenerateEntityType(cn, "BinaryDefaultOwned");
            Assert.Equal(typeof(byte[]), dynamicType.GetProperty("Payload")!.PropertyType);
            Assert.Null(dynamicType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteIntegerPrimaryKey_EmitsIdentityMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IdentityBacked (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "IdentityCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IdentityBacked.cs"));
            Assert.Contains("[Key]", entityCode);
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Identity)]", entityCode);
            Assert.Contains("public long Id { get; set; }", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteVirtualTable_ReportsSkippedObject()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE VIRTUAL TABLE SearchDocs USING fts5(Body);
            CREATE TABLE SearchDocs_Audit (Id INTEGER PRIMARY KEY, Message TEXT NOT NULL);
            """;
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex)
        {
            if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "VirtualCtx");

            Assert.False(File.Exists(Path.Combine(dir, "SearchDocs.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SearchDocsAudit.cs")));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsData", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsIdx", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsContent", StringComparison.OrdinalIgnoreCase));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("VirtualTable", warnings);
            Assert.Contains("SearchDocs", warnings);
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTable" &&
                item.GetProperty("name").GetString() == "SearchDocs" &&
                item.GetProperty("metadata").GetProperty("provider").GetString() == "SQLite" &&
                item.GetProperty("metadata").GetProperty("targetKind").GetString() == "VirtualTable" &&
                item.GetProperty("metadata").GetProperty("queryArtifactSupported").GetBoolean());
            Assert.Contains(skippedObjects.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                item.GetProperty("name").GetString()!.StartsWith("SearchDocs_", StringComparison.Ordinal) &&
                item.GetProperty("metadata").GetProperty("provider").GetString() == "SQLite" &&
                item.GetProperty("metadata").GetProperty("targetKind").GetString() == "VirtualTableShadow" &&
                !item.GetProperty("metadata").GetProperty("queryArtifactSupported").GetBoolean() &&
                item.GetProperty("metadata").GetProperty("shadowOf").GetString() == "SearchDocs");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithEmitViewEntities_GeneratesSqliteVirtualTableQueryArtifact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE VIRTUAL TABLE SearchDocs USING fts5(Body);
            """;
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex)
        {
            if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "VirtualQueryCtx",
                new ScaffoldOptions { Tables = new[] { "SearchDocs" }, EmitQueryArtifacts = true });

            var entityCode = File.ReadAllText(Path.Combine(dir, "SearchDoc.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "VirtualQueryCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"SearchDocs\")]", entityCode);
            Assert.Contains("[ReadOnlyEntity]", entityCode);
            Assert.Contains("Body { get; set; }", entityCode);
            Assert.Contains("IQueryable<SearchDoc> SearchDocs", contextCode);
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsData", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(Directory.GetFiles(dir, "*.cs"), path => Path.GetFileNameWithoutExtension(path).StartsWith("SearchDocsIdx", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTable" &&
                item.GetProperty("name").GetString() == "SearchDocs");
            Assert.Contains(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                item.GetProperty("name").GetString()!.StartsWith("SearchDocs_", StringComparison.Ordinal) &&
                item.GetProperty("metadata").GetProperty("provider").GetString() == "SQLite" &&
                item.GetProperty("metadata").GetProperty("shadowOf").GetString() == "SearchDocs");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTableFilterAndEmitQueryArtifacts_DoesNotReportUnselectedVirtualTableShadows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE KeepMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIRTUAL TABLE SearchDocs USING fts5(Content);
            """;
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex)
        {
            if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "FilteredVirtualCtx",
                new ScaffoldOptions { Tables = new[] { "KeepMe" }, EmitQueryArtifacts = true });

            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredVirtualCtx.cs"));

            Assert.True(File.Exists(Path.Combine(dir, "KeepMe.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SearchDocs.cs")));
            Assert.Contains("IQueryable<KeepMe> KeepMes", contextCode);
            Assert.DoesNotContain("SearchDocs", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCheckInIdentifierName_DoesNotEmitCheckConstraintDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Checkpoint (
                Id INTEGER PRIMARY KEY,
                CheckName TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CheckpointCtx");

            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            if (File.Exists(warningPath))
            {
                var warnings = File.ReadAllText(warningPath);
                Assert.DoesNotContain("CheckConstraint", warnings);
            }
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
