#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // DatabaseScaffolder public API and basic schema integration tests.

    [Fact]
    public async Task ScaffoldAsync_NullConnection_ThrowsArgumentNullException()
    {
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(null!, provider, Path.GetTempPath(), "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullProvider_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, null!, Path.GetTempPath(), "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullOutputDirectory_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, null!, "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullNamespace_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, Path.GetTempPath(), null!));
    }

    [Fact]
    public async Task ScaffoldAsync_InvalidNamespace_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();

        await Assert.ThrowsAsync<NormConfigurationException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, Path.GetTempPath(), "1.Bad Namespace"));
    }

    [Fact]
    public async Task ScaffoldAsync_UnsafeContextName_WritesEscapedContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "1 Bad/Ctx");

            var contextFile = Path.Combine(dir, "_1BadCtx.cs");
            Assert.True(File.Exists(contextFile));
            var contextCode = File.ReadAllText(contextFile);
            Assert.Contains("public partial class _1BadCtx : DbContext", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_ContextNameCollidingWithEntityName_UsesUniqueContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "Widget");

            Assert.True(File.Exists(Path.Combine(dir, "Widget.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "WidgetContext.cs")));
            var entityCode = File.ReadAllText(Path.Combine(dir, "Widget.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "WidgetContext.cs"));

            Assert.Contains("public partial class Widget", entityCode);
            Assert.Contains("public partial class WidgetContext : DbContext", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullableReferenceTypesDisabled_EmitsNullableDisabledCode()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Parent (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Note TEXT NULL
            );
            CREATE TABLE Child (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                Payload TEXT NOT NULL,
                CONSTRAINT FK_Child_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_nullable_disabled_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "NullableDisabledContext",
                new ScaffoldOptions { UseNullableReferenceTypes = false });

            var parentCode = File.ReadAllText(Path.Combine(dir, "Parent.cs"));
            var childCode = File.ReadAllText(Path.Combine(dir, "Child.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "NullableDisabledContext.cs"));

            Assert.Contains("#nullable disable", parentCode);
            Assert.Contains("#nullable disable", contextCode);
            Assert.Contains("public string Name { get; set; }", parentCode);
            Assert.Contains("public string Note { get; set; }", parentCode);
            Assert.DoesNotContain("string? Note", parentCode);
            Assert.DoesNotContain("= default!;", parentCode);
            Assert.Contains("public Parent Parent { get; set; }", childCode);
            Assert.Contains("DbContextOptions options = null", contextCode);
            Assert.DoesNotContain("DbContextOptions? options", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithContextDirectoryAndNamespace_WritesContextSeparatelyAndImportsEntities()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ContextPlaced (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs.Entities",
                "PlacedContext",
                new ScaffoldOptions
                {
                    ContextDirectory = "Data/Contexts",
                    ContextNamespace = "TestNs.Contexts"
                });

            var entityPath = Path.Combine(dir, "ContextPlaced.cs");
            var contextPath = Path.Combine(dir, "Data", "Contexts", "PlacedContext.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace TestNs.Entities;", entityCode);
            Assert.Contains("namespace TestNs.Contexts;", contextCode);
            Assert.Contains("using TestNs.Entities;", contextCode);
            Assert.Contains("IQueryable<ContextPlaced> ContextPlacedRows", contextCode);

            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithContextOutputDirectory_WritesContextOutsideEntityOutput()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ContextAbsolute (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var root = Path.Combine(Path.GetTempPath(), "san_scaffold_context_output_" + Guid.NewGuid().ToString("N"));
        var entityOutput = Path.Combine(root, "Models");
        var contextOutput = Path.Combine(root, "Data", "Contexts");
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                entityOutput,
                "TestNs.Entities",
                "AbsoluteContext",
                new ScaffoldOptions
                {
                    ContextOutputDirectory = contextOutput,
                    ContextNamespace = "TestNs.Contexts"
                });

            var entityPath = Path.Combine(entityOutput, "ContextAbsolute.cs");
            var contextPath = Path.Combine(contextOutput, "AbsoluteContext.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace TestNs.Contexts;", contextCode);
            Assert.Contains("using TestNs.Entities;", contextCode);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUnsafeContextDirectory_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE UnsafeContextPath (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
            DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "UnsafeContext",
                new ScaffoldOptions { ContextDirectory = "..\\Outside" }));

        Assert.Contains("relative child directory", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TableAttributeSchema_IsIncludedInRuntimeTableMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE "main"."SchemaWidget" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO "main"."SchemaWidget" (Id, Name) VALUES (1, 'schema-ok');
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(SanSchemaWidget));
        Assert.Equal("main.SchemaWidget", mapping.TableName);
        Assert.Equal("\"main\".\"SchemaWidget\"", mapping.EscTable);

        var rows = await ctx.Query<SanSchemaWidget>().ToListAsync();
        var row = Assert.Single(rows);
        Assert.Equal("schema-ok", row.Name);
    }

    [Fact]
    public void DatabaseGeneratedComputed_IsTreatedAsDatabaseGeneratedColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(SanComputedGenerated));
        var total = Assert.Single(mapping.Columns, c => c.PropName == nameof(SanComputedGenerated.Total));
        Assert.True(total.IsDbGenerated);
        Assert.DoesNotContain(mapping.InsertColumns, c => c.PropName == nameof(SanComputedGenerated.Total));
        Assert.DoesNotContain(mapping.UpdateColumns, c => c.PropName == nameof(SanComputedGenerated.Total));
    }

    [Fact]
    public void RowVersionComputed_IsTimestampAndDatabaseGeneratedColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(SanRowVersionGenerated));
        var rowVersion = Assert.Single(mapping.Columns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
        Assert.True(rowVersion.IsTimestamp);
        Assert.True(rowVersion.IsDbGenerated);
        Assert.DoesNotContain(mapping.InsertColumns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
        Assert.DoesNotContain(mapping.UpdateColumns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
    }

    [Fact]
    public async Task ScaffoldAsync_SqliteAttachedDatabase_PreservesSchemaAndDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                PRAGMA foreign_keys=ON;
                ATTACH DATABASE ':memory:' AS aux;
                CREATE TABLE "aux"."SchemaAuthor" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE "aux"."SchemaBook" (
                    Id INTEGER PRIMARY KEY,
                    AuthorId INTEGER NOT NULL,
                    Title TEXT NOT NULL DEFAULT 'untitled',
                    CONSTRAINT FK_SchemaBook_Author FOREIGN KEY (AuthorId) REFERENCES SchemaAuthor(Id)
                );
                CREATE INDEX "aux"."IX_SchemaBook_Title" ON "SchemaBook"(Title);
                CREATE INDEX "aux"."IX_SchemaBook_Title_Filtered" ON "SchemaBook"(Title) WHERE Title <> '';
                CREATE TRIGGER "aux"."TR_SchemaBook_Audit" AFTER INSERT ON "SchemaBook" BEGIN SELECT 1; END;
                CREATE VIEW "aux"."SchemaBookView" AS SELECT Id, Title FROM "SchemaBook";
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "AttachedCtx");

            var authorCode = File.ReadAllText(Path.Combine(dir, "SchemaAuthor.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "SchemaBook.cs"));
            var viewCode = File.ReadAllText(Path.Combine(dir, "SchemaBookView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "AttachedCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"SchemaAuthor\", Schema = \"aux\")]", authorCode);
            Assert.Contains("[Table(\"SchemaBook\", Schema = \"aux\")]", bookCode);
            Assert.Contains("[Table(\"SchemaBookView\", Schema = \"aux\")]", viewCode);
            Assert.Contains("[ReadOnlyEntity]", viewCode);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
            Assert.Contains("HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false)", contextCode);
            Assert.Contains("IQueryable<SchemaBookView> SchemaBookViews", contextCode);
            Assert.Contains("[Index(\"IX_SchemaBook_Title\")]", bookCode);
            Assert.Contains("[Index(\"IX_SchemaBook_Title_Filtered\", FilterSql = \"Title <> ''\")]", bookCode);
            Assert.Contains("aux.SchemaBook", warnings);
            Assert.Contains("TR_SchemaBook_Audit", warnings);
            Assert.Contains("aux.SchemaBookView", warnings);

            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.DoesNotContain(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBook" && item.GetProperty("kind").GetString() == "Default");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBook" && item.GetProperty("kind").GetString() == "Trigger");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBookView" && item.GetProperty("kind").GetString() == "MissingPrimaryKey");
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Empty(skippedObjects.EnumerateArray());
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TableAttributeSchema_IsUsedInNavigationProjectionSubqueries()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                ATTACH DATABASE ':memory:' AS aux;
                CREATE TABLE "aux"."SchemaParent" (Id INTEGER PRIMARY KEY);
                CREATE TABLE "aux"."SchemaChild" (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO "aux"."SchemaParent" (Id) VALUES (1);
                INSERT INTO "aux"."SchemaChild" (Id, ParentId, Amount) VALUES (10, 1, 7), (11, 1, 5);
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SanSchemaParent>()
                .HasMany<SanSchemaChild>(p => p.Children)
                .WithOne()
                .HasForeignKey(c => c.ParentId, p => p.Id)
        });

        var rows = await ctx.Query<SanSchemaParent>()
            .Select(p => new
            {
                p.Id,
                ChildCount = p.Children.Count(),
                Total = p.Children.Sum(c => c.Amount)
            })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal(2, row.ChildCount);
        Assert.Equal(12, row.Total);
    }

}
