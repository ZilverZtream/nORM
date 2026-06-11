#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Many-to-many scaffold integration and join discovery tests.

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));
            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("public List<Author> Authors { get; set; } = new();", bookCode);
            Assert.Contains(".HasMany<Book>(p => p.Books)", contextCode);
            Assert.Contains(".WithMany(p => p.Authors)", contextCode);
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTableReferentialActions_EmitsActionAwareManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id) ON DELETE RESTRICT ON UPDATE NO ACTION
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinActionCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinActionCtx.cs"));

            Assert.Contains(".HasMany<Book>(p => p.Books)", contextCode);
            Assert.Contains(".WithMany(p => p.Authors)", contextCode);
            Assert.Contains(".UsingTable(\"AuthorBook\", new[] { \"AuthorId\" }, new[] { \"BookId\" }, ReferentialAction.Cascade, ReferentialAction.Cascade, ReferentialAction.Restrict, ReferentialAction.NoAction);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTableProviderOwnedFeature_RestoresDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT CK_AuthorBook_Positive CHECK (AuthorId > 0 AND BookId > 0),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinFeatureCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinFeatureCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\");", contextCode);
            Assert.DoesNotContain("HasCheckConstraint(\"CK_AuthorBook_Positive\"", contextCode, StringComparison.Ordinal);
            var summary = warningJson.RootElement.GetProperty("summary");
            Assert.Equal(1, summary.GetProperty("sectionCounts").GetProperty("providerOwnedSchemaFeatures").GetInt32());
            Assert.Equal(0, summary.GetProperty("sectionCounts").GetProperty("possibleManyToManyJoinTables").GetInt32());
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "CheckConstraint" &&
                item.GetProperty("table").GetString() == "AuthorBook" &&
                item.GetProperty("name").GetString() == "CK_AuthorBook_Positive" &&
                item.GetProperty("metadata").GetProperty("checkSql").GetString() == "AuthorId > 0 AND BookId > 0");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithProviderOwnedWriteBlockedBridge_DoesNotEmitManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            CREATE TRIGGER TR_AuthorBook_Audit
            AFTER INSERT ON AuthorBook
            BEGIN
                SELECT 1;
            END;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ProviderOwnedBridgeCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "ProviderOwnedBridgeCtx.cs"));
            var bridgeCode = File.ReadAllText(Path.Combine(dir, "AuthorBook.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"AuthorBook\"", contextCode, StringComparison.Ordinal);
            Assert.Contains("[ReadOnlyEntity]", bridgeCode, StringComparison.Ordinal);
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "Trigger" &&
                item.GetProperty("table").GetString() == "AuthorBook" &&
                item.GetProperty("name").GetString() == "TR_AuthorBook_Audit");

            var join = Assert.Single(warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray());
            Assert.Equal("AuthorBook", join.GetProperty("table").GetString());
            Assert.Contains(join.GetProperty("reasons").EnumerateArray(), item => item.GetString() == "provider-owned-write-blocking-schema");
            Assert.True(join.GetProperty("metadata").GetProperty("providerOwnedWriteBlockingSchema").GetBoolean());
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void BuildManyToManyJoins_WithUnknownReferentialAction_SuppressesUsingTable()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var tableType = scaffolder.GetNestedType("ScaffoldTable", BindingFlags.NonPublic)!;
        var foreignKeyType = scaffolder.GetNestedType("ScaffoldForeignKey", BindingFlags.NonPublic)!;
        var indexType = scaffolder.GetNestedType("ScaffoldIndex", BindingFlags.NonPublic)!;
        var tables = Array.CreateInstance(tableType, 3);
        tables.SetValue(Activator.CreateInstance(tableType, "Author", null)!, 0);
        tables.SetValue(Activator.CreateInstance(tableType, "Book", null)!, 1);
        tables.SetValue(Activator.CreateInstance(tableType, "AuthorBook", null)!, 2);
        var foreignKeys = Array.CreateInstance(foreignKeyType, 2);
        foreignKeys.SetValue(Activator.CreateInstance(
            foreignKeyType,
            null,
            "AuthorBook",
            "AuthorId",
            null,
            "Author",
            "Id",
            "FK_AuthorBook_Author",
            1,
            "PROVIDER CASCADE",
            "NO ACTION",
            false)!, 0);
        foreignKeys.SetValue(Activator.CreateInstance(
            foreignKeyType,
            null,
            "AuthorBook",
            "BookId",
            null,
            "Book",
            "Id",
            "FK_AuthorBook_Book",
            1,
            "NO ACTION",
            "NO ACTION",
            false)!, 1);

        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Author"] = "Author",
            ["Book"] = "Book",
            ["AuthorBook"] = "AuthorBook"
        };
        var columnProperties = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Author"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["Id"] = "Id" },
            ["Book"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["Id"] = "Id" },
            ["AuthorBook"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["AuthorId"] = "AuthorId",
                ["BookId"] = "BookId"
            }
        };
        var primaryKeys = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Author"] = new[] { "Id" },
            ["Book"] = new[] { "Id" },
            ["AuthorBook"] = new[] { "AuthorId", "BookId" }
        };
        var emptySets = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
        var emptyTableKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var indexes = Array.CreateInstance(indexType, 0);
        var nonNullableColumns = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Author"] = new HashSet<string>(new[] { "Id" }, StringComparer.OrdinalIgnoreCase),
            ["Book"] = new HashSet<string>(new[] { "Id" }, StringComparer.OrdinalIgnoreCase),
            ["AuthorBook"] = new HashSet<string>(new[] { "AuthorId", "BookId" }, StringComparer.OrdinalIgnoreCase)
        };
        var memberNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var buildJoins = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "BuildManyToManyJoins");

        var joins = (System.Collections.IEnumerable)buildJoins.Invoke(null, new object?[]
        {
            foreignKeys,
            tables,
            entityByTable,
            columnProperties,
            primaryKeys,
            emptySets,
            emptySets,
            indexes,
            nonNullableColumns,
            emptyTableKeys,
            memberNames
        })!;

        Assert.Empty(joins.Cast<object>());

        var reasonsMethod = scaffolder.GetMethod("BuildPossibleJoinTableReasons", BindingFlags.NonPublic | BindingFlags.Static)!;
        var reasons = (string[])reasonsMethod.Invoke(null, new object?[]
        {
            "AuthorBook",
            foreignKeys,
            primaryKeys,
            columnProperties,
            nonNullableColumns,
            emptySets,
            emptySets,
            indexes,
            emptyTableKeys
        })!;

        Assert.Contains("referential-action-not-scaffoldable", reasons);
    }

    [Fact]
    public async Task ScaffoldAsync_WithSurrogateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                UNIQUE (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SurrogateJoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SurrogateJoinCtx.cs"));
            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("public List<Author> Authors { get; set; } = new();", bookCode);
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\");", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSchemaPureJoinTable_PreservesUsingTableSchema()
    {
        var auxFile = Path.Combine(Path.GetTempPath(), "san_scaffold_aux_" + Guid.NewGuid().ToString("N") + ".db");
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"""
            ATTACH DATABASE '{auxFile.Replace("'", "''")}' AS aux;
            PRAGMA foreign_keys=ON;
            CREATE TABLE aux.Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE aux.Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE aux.AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SchemaJoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SchemaJoinCtx.cs"));
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\", schema: \"aux\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
            cn.Close();
            if (File.Exists(auxFile)) File.Delete(auxFile);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingPureJoinTable_EmitsDistinctManyToManyNavigations()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE PersonRelationship (
                MentorId INTEGER NOT NULL,
                MenteeId INTEGER NOT NULL,
                PRIMARY KEY (MentorId, MenteeId),
                CONSTRAINT FK_PersonRelationship_Mentor FOREIGN KEY (MentorId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonRelationship_Mentee FOREIGN KEY (MenteeId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SelfJoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "PersonRelationship.cs")));
            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SelfJoinCtx.cs"));
            Assert.Contains("public List<Person> PeopleByMenteeId { get; set; } = new();", personCode);
            Assert.Contains("public List<Person> PeopleByMentorId { get; set; } = new();", personCode);
            Assert.Contains(".HasMany<Person>(p => p.PeopleByMenteeId)", contextCode);
            Assert.Contains(".WithMany(p => p.PeopleByMentorId)", contextCode);
            Assert.Contains(".UsingTable(\"PersonRelationship\", \"MenteeId\", \"MentorId\");", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPayloadJoinTable_EmitsExplicitJoinEntityAndDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                CreatedAt TEXT NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var joinCode = File.ReadAllText(Path.Combine(dir, "AuthorBook.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("CreatedAt { get; set; }", joinCode);
            Assert.Contains("public Author Author { get; set; } = default!;", joinCode);
            Assert.Contains("public Book Book { get; set; } = default!;", joinCode);
            Assert.Contains("public List<AuthorBook>", authorCode);
            Assert.Contains("public List<AuthorBook>", bookCode);
            Assert.Contains(".HasMany(p => p.AuthorBooks)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.BookId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            Assert.Contains("AuthorBook", warnings);
            Assert.Contains("Author", warnings);
            Assert.Contains("Book", warnings);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("SCF002", joinTables[0].GetProperty("code").GetString());
            Assert.Equal("Warning", joinTables[0].GetProperty("severity").GetString());
            Assert.Equal("many-to-many", joinTables[0].GetProperty("category").GetString());
            Assert.Equal("AuthorBook", joinTables[0].GetProperty("table").GetString());
            Assert.Contains(joinTables[0].GetProperty("principalTables").EnumerateArray(), item => item.GetString() == "Author");
            Assert.Contains(joinTables[0].GetProperty("principalTables").EnumerateArray(), item => item.GetString() == "Book");
            Assert.Contains(joinTables[0].GetProperty("reasons").EnumerateArray(), item => item.GetString() == "payload-columns");
            Assert.Contains("UsingTable", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("all FK columns are NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("generated primary keys or exact ordered unfiltered unique indexes", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            Assert.Contains("generated surrogate primary key plus an exact unfiltered unique index", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            var metadata = joinTables[0].GetProperty("metadata");
            Assert.Equal(2, metadata.GetProperty("foreignKeyConstraintCount").GetInt32());
            Assert.Equal(new[] { "AuthorId", "BookId" }, metadata.GetProperty("foreignKeyColumns").EnumerateArray().Select(item => item.GetString()).ToArray());
            Assert.Equal(new[] { "AuthorId", "BookId" }, metadata.GetProperty("primaryKeyColumns").EnumerateArray().Select(item => item.GetString()).ToArray());
            Assert.Equal(new[] { "CreatedAt" }, metadata.GetProperty("payloadColumns").EnumerateArray().Select(item => item.GetString()).ToArray());
            Assert.Empty(metadata.GetProperty("databaseGeneratedColumns").EnumerateArray());
            Assert.Empty(metadata.GetProperty("identityColumns").EnumerateArray());
            Assert.Empty(metadata.GetProperty("nullableForeignKeyColumns").EnumerateArray());
            Assert.True(metadata.GetProperty("hasExactBridgePrimaryKey").GetBoolean());
            Assert.False(metadata.GetProperty("hasGeneratedSurrogatePrimaryKey").GetBoolean());
            Assert.False(metadata.GetProperty("hasExactForeignKeyUniqueIndex").GetBoolean());
            Assert.Contains(metadata.GetProperty("foreignKeys").EnumerateArray(), item =>
                item.GetProperty("principalTable").GetString() == "Author" &&
                string.Join(",", item.GetProperty("dependentColumns").EnumerateArray().Select(column => column.GetString())) == "AuthorId" &&
                string.Join(",", item.GetProperty("principalColumns").EnumerateArray().Select(column => column.GetString())) == "Id");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, StudentId)
            );
            CREATE TABLE Course (
                TenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                Title TEXT NOT NULL,
                PRIMARY KEY (TenantId, CourseId)
            );
            CREATE TABLE StudentCourse (
                StudentTenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseTenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                PRIMARY KEY (StudentTenantId, StudentId, CourseTenantId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentTenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseTenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeJoinCtx.cs"));
            var warningsPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.Contains(".UsingTable(\"StudentCourse\", new[] { \"StudentTenantId\", \"StudentId\" }, new[] { \"CourseTenantId\", \"CourseId\" });", contextCode);
            if (File.Exists(warningsPath))
            {
                var warnings = File.ReadAllText(warningsPath);
                Assert.DoesNotContain("possible many-to-many", warnings, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("Composite Foreign Keys", warnings);
            }

            if (File.Exists(warningJsonPath))
            {
                using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
                Assert.Empty(joinTables.EnumerateArray());
            }
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDatabaseGeneratedBridgeColumn_StillEmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE Course (
                Id INTEGER PRIMARY KEY,
                Title TEXT NOT NULL
            );
            CREATE TABLE StudentCourse (
                StudentId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                PairKey TEXT GENERATED ALWAYS AS (StudentId || ':' || CourseId) VIRTUAL,
                PRIMARY KEY (StudentId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentId) REFERENCES Student(Id),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseId) REFERENCES Course(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "GeneratedBridgeJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "GeneratedBridgeJoinCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.Contains(".UsingTable(\"StudentCourse\", \"StudentId\", \"CourseId\");", contextCode);
            if (File.Exists(warningJsonPath))
            {
                using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
                Assert.Empty(joinTables.EnumerateArray());
            }

            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeKeyPayloadJoinTable_ReportsPayloadNotCompositeUnsupported()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, StudentId)
            );
            CREATE TABLE Course (
                TenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                Title TEXT NOT NULL,
                PRIMARY KEY (TenantId, CourseId)
            );
            CREATE TABLE StudentCourse (
                StudentTenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseTenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                EnrolledAt TEXT NOT NULL,
                PRIMARY KEY (StudentTenantId, StudentId, CourseTenantId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentTenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseTenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositePayloadJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositePayloadJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            var reasons = joinTables[0].GetProperty("reasons").EnumerateArray().Select(item => item.GetString()).ToArray();

            Assert.True(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.DoesNotContain(".UsingTable(\"StudentCourse\"", contextCode);
            Assert.Contains("at least one safe `UsingTable` requirement was not met", warnings);
            Assert.DoesNotContain("two single-column foreign key constraints", warnings);
            Assert.Contains("payload-columns", reasons);
            Assert.DoesNotContain("composite-foreign-key", reasons);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
