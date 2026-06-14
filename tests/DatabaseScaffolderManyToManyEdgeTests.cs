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
    [Fact]
    public async Task ScaffoldAsync_WithCompositeSurrogateKeyPureJoinTable_EmitsManyToManyMapping()
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
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                StudentTenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseTenantId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                UNIQUE (StudentTenantId, StudentId, CourseTenantId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentTenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseTenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeSurrogateJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeSurrogateJoinCtx.cs"));

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.Contains(".UsingTable(\"StudentCourse\", new[] { \"StudentTenantId\", \"StudentId\" }, new[] { \"CourseTenantId\", \"CourseId\" });", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithFilteredUniqueSurrogateJoinTable_DoesNotEmitUnsafeManyToManyMapping()
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
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                StudentId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentId) REFERENCES Student(Id),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseId) REFERENCES Course(Id)
            );
            CREATE UNIQUE INDEX UX_StudentCourse_Filtered ON StudentCourse(StudentId, CourseId) WHERE StudentId > 0;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FilteredSurrogateJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredSurrogateJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();

            Assert.True(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.DoesNotContain(".UsingTable(\"StudentCourse\"", contextCode, StringComparison.Ordinal);
            Assert.Contains("StudentCourse", warnings);
            var join = Assert.Single(joinTables);
            Assert.Equal("StudentCourse", join.GetProperty("table").GetString());
            Assert.Contains("missing-exact-unique-index", join.GetProperty("reasons").EnumerateArray().Select(reason => reason.GetString()));
            var metadata = join.GetProperty("metadata");
            Assert.False(metadata.GetProperty("hasExactForeignKeyUniqueIndex").GetBoolean());
            var candidate = Assert.Single(metadata.GetProperty("foreignKeyUniqueIndexCandidates").EnumerateArray());
            Assert.Equal("UX_StudentCourse_Filtered", candidate.GetProperty("indexName").GetString());
            Assert.Equal(new[] { "StudentId", "CourseId" }, candidate.GetProperty("columns").EnumerateArray().Select(column => column.GetString()).ToArray());
            Assert.True(candidate.GetProperty("isFiltered").GetBoolean());
            Assert.Equal("StudentId > 0", candidate.GetProperty("filterSql").GetString());
            Assert.False(candidate.GetProperty("isUnfilteredExactForeignKeyUniqueIndex").GetBoolean());
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithAlternateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY,
                Isbn TEXT NOT NULL UNIQUE,
                Title TEXT NOT NULL
            );
            CREATE TABLE AuthorBook (
                AuthorCode TEXT NOT NULL,
                BookIsbn TEXT NOT NULL,
                PRIMARY KEY (AuthorCode, BookIsbn),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorCode) REFERENCES Author(Code),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookIsbn) REFERENCES Book(Isbn)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "AlternateKeyJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "AlternateKeyJoinCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorCode\", \"BookIsbn\", p => p.Code, p => p.Isbn);", contextCode);
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
    public async Task ScaffoldAsync_WithFilteredUniquePrincipalIndex_ReportsPartialIndexAndDoesNotInventRelationship()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=OFF;
            CREATE TABLE Customer (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX UX_Customer_Code_Filtered ON Customer(Code) WHERE Code <> '';
            CREATE TABLE CustomerNote (
                Id INTEGER PRIMARY KEY,
                CustomerCode TEXT NOT NULL,
                Note TEXT NOT NULL,
                CONSTRAINT FK_CustomerNote_Customer FOREIGN KEY (CustomerCode) REFERENCES Customer(Code)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FilteredPrincipalRelationshipCtx");

            var noteCode = File.ReadAllText(Path.Combine(dir, "CustomerNote.cs"));
            var customerCode = File.ReadAllText(Path.Combine(dir, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredPrincipalRelationshipCtx.cs"));

            Assert.Contains("FilterSql = \"Code <> ''\"", customerCode);
            Assert.DoesNotContain("public Customer Customer", noteCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeAlternateKeyPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                Code TEXT NOT NULL,
                Name TEXT NOT NULL,
                UNIQUE (TenantId, Code)
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                Isbn TEXT NOT NULL,
                Title TEXT NOT NULL,
                UNIQUE (TenantId, Isbn)
            );
            CREATE TABLE AuthorBook (
                TenantId INTEGER NOT NULL,
                AuthorCode TEXT NOT NULL,
                BookIsbn TEXT NOT NULL,
                PRIMARY KEY (TenantId, AuthorCode, BookIsbn),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (TenantId, AuthorCode) REFERENCES Author(TenantId, Code),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (TenantId, BookIsbn) REFERENCES Book(TenantId, Isbn)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeAlternateKeyJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeAlternateKeyJoinCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.Contains(".UsingTable(\"AuthorBook\", new[] { \"TenantId\", \"AuthorCode\" }, new[] { \"TenantId\", \"BookIsbn\" }, p => new { p.TenantId, p.Code }, p => new { p.TenantId, p.Isbn });", contextCode);
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
    public async Task ScaffoldAsync_WithSharedTenantCompositeKeyPureJoinTable_EmitsManyToManyMapping()
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
                TenantId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                CourseId INTEGER NOT NULL,
                PRIMARY KEY (TenantId, StudentId, CourseId),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (TenantId, StudentId) REFERENCES Student(TenantId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (TenantId, CourseId) REFERENCES Course(TenantId, CourseId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SharedTenantCompositeJoinCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "SharedTenantCompositeJoinCtx.cs"));
            var warningsPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");

            Assert.False(File.Exists(Path.Combine(dir, "StudentCourse.cs")));
            Assert.Contains(".UsingTable(\"StudentCourse\", new[] { \"TenantId\", \"StudentId\" }, new[] { \"TenantId\", \"CourseId\" });", contextCode);
            if (File.Exists(warningsPath))
            {
                var warnings = File.ReadAllText(warningsPath);
                Assert.DoesNotContain("possible many-to-many", warnings, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("Composite Foreign Keys", warnings);
            }

            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithKeylessJoinTable_DoesNotEmitUnsafeManyToManyMapping()
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
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessJoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "KeylessJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"AuthorBook\"", contextCode);
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            Assert.Contains("MissingPrimaryKey", warnings);
            var summary = warningJson.RootElement.GetProperty("summary");
            Assert.Equal(1, summary.GetProperty("sectionCounts").GetProperty("possibleManyToManyJoinTables").GetInt32());
            var keylessJoin = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables")[0];
            Assert.Contains(keylessJoin.GetProperty("reasons").EnumerateArray(), item => item.GetString() == "missing-primary-key");
            var keylessMetadata = keylessJoin.GetProperty("metadata");
            Assert.Equal(new[] { "AuthorId", "BookId" }, keylessMetadata.GetProperty("foreignKeyColumns").EnumerateArray().Select(item => item.GetString()).ToArray());
            Assert.Empty(keylessMetadata.GetProperty("primaryKeyColumns").EnumerateArray());
            Assert.False(keylessMetadata.GetProperty("hasExactBridgePrimaryKey").GetBoolean());
            Assert.False(keylessMetadata.GetProperty("hasGeneratedSurrogatePrimaryKey").GetBoolean());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "AuthorBook");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullableJoinTableForeignKeys_DoesNotEmitUnsafeManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER,
                BookId INTEGER,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "NullableJoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "NullableJoinCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"AuthorBook\"", contextCode);
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("AuthorBook", joinTables[0].GetProperty("table").GetString());
            Assert.Contains(joinTables[0].GetProperty("reasons").EnumerateArray(), item => item.GetString() == "nullable-foreign-key");
            Assert.Contains("NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUnsafeSelfJoinTable_EmitsManyToManyDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PersonFriend (
                PersonId INTEGER,
                FriendId INTEGER,
                PRIMARY KEY (PersonId, FriendId),
                CONSTRAINT FK_PersonFriend_Person FOREIGN KEY (PersonId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonFriend_Friend FOREIGN KEY (FriendId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "UnsafeSelfJoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "PersonFriend.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "UnsafeSelfJoinCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain(".UsingTable(\"PersonFriend\"", contextCode);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("PersonFriend", joinTables[0].GetProperty("table").GetString());
            Assert.Single(joinTables[0].GetProperty("principalTables").EnumerateArray());
            Assert.Contains(joinTables[0].GetProperty("reasons").EnumerateArray(), item => item.GetString() == "nullable-foreign-key");
            Assert.Contains("NOT NULL", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTable_UsesJoinTableNameForNavigationDirection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Course (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE StudentCourse (
                CourseId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                PRIMARY KEY (CourseId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseId) REFERENCES Course(Id),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentId) REFERENCES Student(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            var studentCode = File.ReadAllText(Path.Combine(dir, "Student.cs"));
            var courseCode = File.ReadAllText(Path.Combine(dir, "Course.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));

            Assert.Contains("public List<Course> Courses { get; set; } = new();", studentCode);
            Assert.Contains("public List<Student> Students { get; set; } = new();", courseCode);
            Assert.Contains(".HasMany<Course>(p => p.Courses)", contextCode);
            Assert.Contains(".WithMany(p => p.Students)", contextCode);
            Assert.Contains(".UsingTable(\"StudentCourse\", \"StudentId\", \"CourseId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingPureJoinTable_GeneratesUniqueInverseCollections()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PersonFriend (
                PersonId INTEGER NOT NULL,
                FriendId INTEGER NOT NULL,
                PRIMARY KEY (PersonId, FriendId),
                CONSTRAINT FK_PersonFriend_Person FOREIGN KEY (PersonId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonFriend_Friend FOREIGN KEY (FriendId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SelfJoinCtx");

            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SelfJoinCtx.cs"));

            Assert.False(File.Exists(Path.Combine(dir, "PersonFriend.cs")));
            Assert.Contains("public List<Person> PeopleByPersonId { get; set; } = new();", personCode);
            Assert.Contains("public List<Person> PeopleByFriendId { get; set; } = new();", personCode);
            Assert.Contains(".HasMany<Person>(p => p.PeopleByPersonId)", contextCode);
            Assert.Contains(".WithMany(p => p.PeopleByFriendId)", contextCode);
            Assert.Contains(".UsingTable(\"PersonFriend\", \"PersonId\", \"FriendId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
