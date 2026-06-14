#nullable enable

using System;
using System.Collections.Generic;
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

}
