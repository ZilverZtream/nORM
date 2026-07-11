#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static async Task SetupSurrogateManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SurrogateAuthorBookTable, provider.Escape(SurrogateAuthorBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SurrogateBookTable, provider.Escape(SurrogateBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SurrogateAuthorTable, provider.Escape(SurrogateAuthorTable)));

        var author = provider.Escape(SurrogateAuthorTable);
        var book = provider.Escape(SurrogateBookTable);
        var join = provider.Escape(SurrogateAuthorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection, $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({IdentityPrimaryKeyColumn(kind, id)}, {authorId} {IntType(kind)} NOT NULL, {bookId} {IntType(kind)} NOT NULL, UNIQUE ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(SurrogateAuthorBookAuthorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(SurrogateAuthorBookBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");
    }

    private static async Task SetupGeneratedBridgeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownGeneratedBridgeManyToManyAsync(connection, provider, kind);

        var student = provider.Escape(GeneratedBridgeStudentTable);
        var course = provider.Escape(GeneratedBridgeCourseTable);
        var join = provider.Escape(GeneratedBridgeStudentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var pairSum = provider.Escape("PairSum");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var generatedColumn = kind switch
        {
            ProviderKind.SqlServer => $"{pairSum} AS ({studentId} + {courseId}) PERSISTED",
            ProviderKind.Postgres => $"{pairSum} integer GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.MySql => $"{pairSum} INT GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.Sqlite => $"{pairSum} INTEGER GENERATED ALWAYS AS ({studentId} + {courseId}) VIRTUAL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection, $"CREATE TABLE {student} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE TABLE {course} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({studentId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {generatedColumn}, PRIMARY KEY ({studentId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(GeneratedBridgeStudentCourseStudentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(GeneratedBridgeStudentCourseCourseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
    }

    private static async Task SetupCompositeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeStudentCourseTable, provider.Escape(CompositeStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeCourseTable, provider.Escape(CompositeCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeStudentTable, provider.Escape(CompositeStudentTable)));

        var student = provider.Escape(CompositeStudentTable);
        var course = provider.Escape(CompositeCourseTable);
        var join = provider.Escape(CompositeStudentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({studentTenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseTenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(CompositeStudentCourseStudentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(CompositeStudentCourseCourseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static async Task SetupCompositePayloadJoinAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositePayloadStudentCourseTable, provider.Escape(CompositePayloadStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositePayloadCourseTable, provider.Escape(CompositePayloadCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositePayloadStudentTable, provider.Escape(CompositePayloadStudentTable)));

        var student = provider.Escape(CompositePayloadStudentTable);
        var course = provider.Escape(CompositePayloadCourseTable);
        var join = provider.Escape(CompositePayloadStudentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var enrollmentCode = provider.Escape("EnrollmentCode");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({studentTenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseTenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {enrollmentCode} {TextType(kind, 40)} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(CompositePayloadStudentCourseStudentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(CompositePayloadStudentCourseCourseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static async Task SetupCompositeSurrogateManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateStudentCourseTable, provider.Escape(CompositeSurrogateStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateCourseTable, provider.Escape(CompositeSurrogateCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateStudentTable, provider.Escape(CompositeSurrogateStudentTable)));

        var student = provider.Escape(CompositeSurrogateStudentTable);
        var course = provider.Escape(CompositeSurrogateCourseTable);
        var join = provider.Escape(CompositeSurrogateStudentCourseTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({IdentityPrimaryKeyColumn(kind, id)}, {studentTenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseTenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"UNIQUE ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(CompositeSurrogateStudentCourseStudentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(CompositeSurrogateStudentCourseCourseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static async Task SetupSharedTenantManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SharedTenantStudentCourseTable, provider.Escape(SharedTenantStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedTenantCourseTable, provider.Escape(SharedTenantCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedTenantStudentTable, provider.Escape(SharedTenantStudentTable)));

        var student = provider.Escape(SharedTenantStudentTable);
        var course = provider.Escape(SharedTenantCourseTable);
        var join = provider.Escape(SharedTenantStudentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"PRIMARY KEY ({tenantId}, {studentId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(SharedTenantStudentCourseStudentFkName)} FOREIGN KEY ({tenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(SharedTenantStudentCourseCourseFkName)} FOREIGN KEY ({tenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");

        if (kind == ProviderKind.MySql)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveSharedTenantStudentCourse_Course")} ON {join} ({tenantId}, {courseId})");
    }

    private static async Task SetupSharedAlternateKeyManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SharedAlternateAuthorBookTable, provider.Escape(SharedAlternateAuthorBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedAlternateBookTable, provider.Escape(SharedAlternateBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedAlternateAuthorTable, provider.Escape(SharedAlternateAuthorTable)));

        var author = provider.Escape(SharedAlternateAuthorTable);
        var book = provider.Escape(SharedAlternateBookTable);
        var join = provider.Escape(SharedAlternateAuthorBookTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var code = provider.Escape("Code");
        var isbn = provider.Escape("Isbn");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorCode = provider.Escape("AuthorCode");
        var bookIsbn = provider.Escape("BookIsbn");

        await ExecuteAsync(connection,
            $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {code} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, UNIQUE ({tenantId}, {code}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {isbn} {TextType(kind, 40)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, UNIQUE ({tenantId}, {isbn}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({tenantId} {IntType(kind)} NOT NULL, {authorCode} {TextType(kind, 40)} NOT NULL, {bookIsbn} {TextType(kind, 40)} NOT NULL, " +
            $"PRIMARY KEY ({tenantId}, {authorCode}, {bookIsbn}), " +
            $"CONSTRAINT {provider.Escape(SharedAlternateAuthorBookAuthorFkName)} FOREIGN KEY ({tenantId}, {authorCode}) REFERENCES {author} ({tenantId}, {code}), " +
            $"CONSTRAINT {provider.Escape(SharedAlternateAuthorBookBookFkName)} FOREIGN KEY ({tenantId}, {bookIsbn}) REFERENCES {book} ({tenantId}, {isbn}))");

        if (kind == ProviderKind.MySql)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveSharedAlternateAuthorBook_Book")} ON {join} ({tenantId}, {bookIsbn})");
    }

    private static async Task SetupAlternateKeyManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, AlternateAuthorBookTable, provider.Escape(AlternateAuthorBookTable)));
        await ExecuteAsync(connection, DropTable(kind, AlternateBookTable, provider.Escape(AlternateBookTable)));
        await ExecuteAsync(connection, DropTable(kind, AlternateAuthorTable, provider.Escape(AlternateAuthorTable)));

        var author = provider.Escape(AlternateAuthorTable);
        var book = provider.Escape(AlternateBookTable);
        var join = provider.Escape(AlternateAuthorBookTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var isbn = provider.Escape("Isbn");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorCode = provider.Escape("AuthorCode");
        var bookIsbn = provider.Escape("BookIsbn");

        await ExecuteAsync(connection,
            $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, UNIQUE ({code}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {isbn} {TextType(kind, 40)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, UNIQUE ({isbn}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({authorCode} {TextType(kind, 40)} NOT NULL, {bookIsbn} {TextType(kind, 40)} NOT NULL, PRIMARY KEY ({authorCode}, {bookIsbn}), " +
            $"CONSTRAINT {provider.Escape(AlternateAuthorBookAuthorFkName)} FOREIGN KEY ({authorCode}) REFERENCES {author} ({code}), " +
            $"CONSTRAINT {provider.Escape(AlternateAuthorBookBookFkName)} FOREIGN KEY ({bookIsbn}) REFERENCES {book} ({isbn}))");
    }

    private static async Task SetupSelfReferencingManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SelfPersonRelationshipTable, provider.Escape(SelfPersonRelationshipTable)));
        await ExecuteAsync(connection, DropTable(kind, SelfPersonTable, provider.Escape(SelfPersonTable)));

        var person = provider.Escape(SelfPersonTable);
        var relationship = provider.Escape(SelfPersonRelationshipTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var mentorId = provider.Escape("MentorId");
        var menteeId = provider.Escape("MenteeId");

        await ExecuteAsync(connection,
            $"CREATE TABLE {person} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {relationship} ({mentorId} {IntType(kind)} NOT NULL, {menteeId} {IntType(kind)} NOT NULL, PRIMARY KEY ({mentorId}, {menteeId}), " +
            $"CONSTRAINT {provider.Escape(SelfPersonRelationshipMentorFkName)} FOREIGN KEY ({mentorId}) REFERENCES {person} ({id}), " +
            $"CONSTRAINT {provider.Escape(SelfPersonRelationshipMenteeFkName)} FOREIGN KEY ({menteeId}) REFERENCES {person} ({id}))");
    }

    private static async Task SetupFilteredUniqueSurrogateJoinAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, FilteredStudentCourseTable, provider.Escape(FilteredStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, FilteredCourseTable, provider.Escape(FilteredCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, FilteredStudentTable, provider.Escape(FilteredStudentTable)));

        var student = provider.Escape(FilteredStudentTable);
        var course = provider.Escape(FilteredCourseTable);
        var join = provider.Escape(FilteredStudentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var activePredicate = $"{studentId} > 0";

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {studentId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(FilteredStudentCourseStudentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(FilteredStudentCourseCourseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(FilteredStudentCourseUniqueIndex)} ON {join} ({studentId}, {courseId}) WHERE {activePredicate}");
    }

    private static async Task SetupNullableBridgeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownNullableBridgeManyToManyAsync(connection, provider, kind);

        var student = provider.Escape(NullableBridgeStudentTable);
        var course = provider.Escape(NullableBridgeCourseTable);
        var join = provider.Escape(NullableBridgeStudentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection, $"CREATE TABLE {student} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE TABLE {course} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({IdentityPrimaryKeyColumn(kind, id)}, {studentId} {IntType(kind)} NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(NullableBridgeStudentCourseStudentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(NullableBridgeStudentCourseCourseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(NullableBridgeStudentCourseUniqueIndexName)} ON {join} ({studentId}, {courseId})");
    }

    private static async Task SetupProviderOwnedBridgeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownProviderOwnedBridgeManyToManyAsync(connection, provider, kind);

        var author = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, ProviderOwnedBridgeAuthorTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", ProviderOwnedBridgeAuthorTable)
                : provider.Escape(ProviderOwnedBridgeAuthorTable);
        var book = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, ProviderOwnedBridgeBookTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", ProviderOwnedBridgeBookTable)
                : provider.Escape(ProviderOwnedBridgeBookTable);
        var join = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, ProviderOwnedBridgeAuthorBookTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", ProviderOwnedBridgeAuthorBookTable)
                : provider.Escape(ProviderOwnedBridgeAuthorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection, $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({authorId} {IntType(kind)} NOT NULL, {bookId} {IntType(kind)} NOT NULL, PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(ProviderOwnedBridgeAuthorBookAuthorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(ProviderOwnedBridgeAuthorBookBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, ProviderOwnedBridgeTrigger)}} ON {{join}}
                    AFTER INSERT AS
                    BEGIN
                        SET NOCOUNT ON;
                    END
                    """);
                break;
            case ProviderKind.Postgres:
                var function = Qualified(provider, "public", ProviderOwnedBridgePostgresFunction);
                await ExecuteAsync(connection, $$"""
                    CREATE FUNCTION {{function}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        RETURN NEW;
                    END
                    $$
                    """);
                await ExecuteAsync(connection,
                    $"CREATE TRIGGER {provider.Escape(ProviderOwnedBridgeTrigger)} BEFORE INSERT ON {join} FOR EACH ROW EXECUTE FUNCTION {function}()");
                break;
            case ProviderKind.MySql:
                // No-op self-assignment: MySqlConnector client-side parses @name tokens as
                // command parameters unless AllowUserVariables=true, so the trigger body
                // must avoid session user variables to run on any connection string.
                await ExecuteAsync(connection,
                    $"CREATE TRIGGER {provider.Escape(ProviderOwnedBridgeTrigger)} BEFORE INSERT ON {join} FOR EACH ROW SET NEW.{authorId} = NEW.{authorId}");
                break;
            case ProviderKind.Sqlite:
                await ExecuteAsync(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(ProviderOwnedBridgeTrigger)}} AFTER INSERT ON {{join}}
                    BEGIN
                        SELECT 1;
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static async Task SetupSchemaQualifiedManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSchemaQualifiedManyToManyAsync(connection, provider, kind);

        if (kind == ProviderKind.Sqlite)
        {
            try
            {
                await ExecuteAsync(connection, $"DETACH DATABASE {provider.Escape(SchemaName)}");
            }
            catch
            {
                // Attachment may not exist before setup.
            }

            await ExecuteAsync(connection, $"ATTACH DATABASE ':memory:' AS {provider.Escape(SchemaName)}");
        }
        else if (kind == ProviderKind.SqlServer)
        {
            await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{SchemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(SchemaName)}')");
        }
        else
        {
            await ExecuteAsync(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(SchemaName)}");
        }

        var author = Qualified(provider, SchemaName, SchemaAuthorTable);
        var book = Qualified(provider, SchemaName, SchemaBookTable);
        var join = Qualified(provider, SchemaName, SchemaAuthorBookTable);
        var fkAuthor = kind == ProviderKind.Sqlite ? provider.Escape(SchemaAuthorTable) : author;
        var fkBook = kind == ProviderKind.Sqlite ? provider.Escape(SchemaBookTable) : book;
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");

        await ExecuteAsync(connection,
            $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({authorId} {IntType(kind)} NOT NULL, {bookId} {IntType(kind)} NOT NULL, PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(SchemaAuthorBookAuthorFkName)} FOREIGN KEY ({authorId}) REFERENCES {fkAuthor} ({id}), " +
            $"CONSTRAINT {provider.Escape(SchemaAuthorBookBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {fkBook} ({id}))");
    }
}
