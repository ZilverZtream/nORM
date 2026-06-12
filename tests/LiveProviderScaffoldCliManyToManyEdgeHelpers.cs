#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName)
    {
        CleanupSurrogateKeyManyToMany(connection, provider, authorTable, bookTable, authorBookTable);

        var author = provider.Escape(authorTable);
        var book = provider.Escape(bookTable);
        var authorBook = provider.Escape(authorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {book} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {authorBook} ({IdentityPrimaryKeyColumn(kind, id)}, {authorId} {idType} NOT NULL, {bookId} {idType} NOT NULL, UNIQUE ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");
    }

    private static void CleanupSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(authorBookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(bookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(authorTable)}");
    }

    private static void SetupDatabaseGeneratedBridgeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupDatabaseGeneratedBridgeManyToMany(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var pairSum = provider.Escape("PairSum");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var generatedColumn = kind switch
        {
            ProviderKind.SqlServer => $"{pairSum} AS ({studentId} + {courseId}) PERSISTED",
            ProviderKind.Postgres => $"{pairSum} integer GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.MySql => $"{pairSum} INT GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.Sqlite => $"{pairSum} INTEGER GENERATED ALWAYS AS ({studentId} + {courseId}) VIRTUAL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection,
            $"CREATE TABLE {student} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {course} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {studentCourse} ({studentId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {generatedColumn}, PRIMARY KEY ({studentId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
    }

    private static void CleanupDatabaseGeneratedBridgeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupCompositeSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupCompositeSurrogateKeyManyToMany(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({tenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))",
            $"CREATE TABLE {course} ({tenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {title} {text} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))",
            $"CREATE TABLE {studentCourse} ({IdentityPrimaryKeyColumn(kind, id)}, {studentTenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {courseTenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, " +
            $"UNIQUE ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static void CleanupCompositeSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupCompositePayloadJoin(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupCompositePayloadJoin(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var enrollmentCode = provider.Escape("EnrollmentCode");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({tenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {name} {text80} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))",
            $"CREATE TABLE {course} ({tenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {title} {text80} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))",
            $"CREATE TABLE {studentCourse} ({studentTenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {courseTenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {enrollmentCode} {text40} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static void CleanupCompositePayloadJoin(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupFilteredUniqueSurrogateJoin(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName,
        string uniqueIndexName)
    {
        CleanupFilteredUniqueSurrogateJoin(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {course} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {studentCourse} ({IdentityPrimaryKeyColumn(kind, id)}, {studentId} {idType} NOT NULL, {courseId} {idType} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(uniqueIndexName)} ON {studentCourse} ({studentId}, {courseId}) WHERE {studentId} > 0");
    }

    private static void CleanupFilteredUniqueSurrogateJoin(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupSchemaQualifiedManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName)
    {
        CleanupSchemaQualifiedManyToMany(connection, provider, kind, schemaName, authorTable, bookTable, authorBookTable);

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else if (kind != ProviderKind.Sqlite)
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");

        var author = Qualified(provider, schemaName, authorTable);
        var book = Qualified(provider, schemaName, bookTable);
        var authorBook = Qualified(provider, schemaName, authorBookTable);
        var authorReference = kind == ProviderKind.Sqlite ? provider.Escape(authorTable) : author;
        var bookReference = kind == ProviderKind.Sqlite ? provider.Escape(bookTable) : book;
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var text = kind == ProviderKind.SqlServer ? "nvarchar(80)" : "text";

        Execute(connection,
            $"CREATE TABLE {author} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {book} ({id} int NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {authorBook} ({authorId} int NOT NULL, {bookId} int NOT NULL, PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorId}) REFERENCES {authorReference} ({id}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookId}) REFERENCES {bookReference} ({id}))");
    }

    private static void CleanupSchemaQualifiedManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        var author = kind == ProviderKind.Sqlite ? provider.Escape(authorTable) : Qualified(provider, schemaName, authorTable);
        var book = kind == ProviderKind.Sqlite ? provider.Escape(bookTable) : Qualified(provider, schemaName, bookTable);
        var authorBook = kind == ProviderKind.Sqlite ? provider.Escape(authorBookTable) : Qualified(provider, schemaName, authorBookTable);

        Execute(connection,
            DropTable(kind, schemaName + "." + authorBookTable, authorBook),
            DropTable(kind, schemaName + "." + bookTable, book),
            DropTable(kind, schemaName + "." + authorTable, author));

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else if (kind != ProviderKind.Sqlite)
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }
}
