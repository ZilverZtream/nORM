#nullable enable
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static string ExpectedCascadeForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, cascadeDelete: false);"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, \"{constraintName}\", false);";

    private static string ExpectedReferentialForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string onDelete,
        string onUpdate,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate});"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate}, \"{constraintName}\");";

    private static async Task SetupAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, BookLabelTable, provider.Escape(BookLabelTable)));
        await ExecuteAsync(connection, DropTable(kind, BookTable, provider.Escape(BookTable)));
        await ExecuteAsync(connection, DropTable(kind, LabelTable, provider.Escape(LabelTable)));
        await ExecuteAsync(connection, DropTable(kind, AuthorTable, provider.Escape(AuthorTable)));

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("Author_Id");
        var bookId = provider.Escape("BookId");
        var labelId = provider.Escape("LabelId");
        var author = provider.Escape(AuthorTable);
        var book = provider.Escape(BookTable);
        var label = provider.Escape(LabelTable);
        var bookLabel = provider.Escape(BookLabelTable);

        await ExecuteAsync(connection, $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {authorId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(FkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}))");
        await ExecuteAsync(connection, $"CREATE TABLE {label} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {bookLabel} ({bookId} {IntType(kind)} NOT NULL, {labelId} {IntType(kind)} NOT NULL, PRIMARY KEY ({bookId}, {labelId}), " +
            $"CONSTRAINT {provider.Escape(BookLabelBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}), " +
            $"CONSTRAINT {provider.Escape(BookLabelLabelFkName)} FOREIGN KEY ({labelId}) REFERENCES {label} ({id}))");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveBook_Author_Title")} ON {book} ({authorId}, {title})");
    }

    private static async Task SetupDatabaseNamesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownDatabaseNamesAsync(connection, provider, kind);

        var customer = provider.Escape(DatabaseNamesCustomerTable);
        var orderLine = provider.Escape(DatabaseNamesOrderLineTable);
        var customerId = provider.Escape("scaffold_database_names_customer_id");
        var displayName = provider.Escape("display_name");
        var orderLineId = provider.Escape("scaffold_database_names_order_line_id");
        var billingCustomerId = provider.Escape("billing_scaffold_database_names_customer_id");
        var shippingCustomerId = provider.Escape("shipping_scaffold_database_names_customer_id");
        var sku = provider.Escape("SKU");
        var classColumn = provider.Escape("class");
        var hasSpace = provider.Escape("has space");

        await ExecuteAsync(connection,
            $"CREATE TABLE {customer} ({customerId} {IntType(kind)} NOT NULL PRIMARY KEY, {displayName} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {orderLine} ({orderLineId} {IntType(kind)} NOT NULL PRIMARY KEY, {billingCustomerId} {IntType(kind)} NOT NULL, {shippingCustomerId} {IntType(kind)} NULL, {sku} {TextType(kind, 40)} NOT NULL, {classColumn} {TextType(kind, 40)} NULL, {hasSpace} {TextType(kind, 40)} NULL, " +
            $"CONSTRAINT {provider.Escape(DatabaseNamesBillingFkName)} FOREIGN KEY ({billingCustomerId}) REFERENCES {customer} ({customerId}), " +
            $"CONSTRAINT {provider.Escape(DatabaseNamesShippingFkName)} FOREIGN KEY ({shippingCustomerId}) REFERENCES {customer} ({customerId}))");
    }

    private static async Task SetupCompositeAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeChildTable, provider.Escape(CompositeChildTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeParentTable, provider.Escape(CompositeParentTable)));

        var parent = provider.Escape(CompositeParentTable);
        var child = provider.Escape(CompositeChildTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({tenantId} {IntType(kind)} NOT NULL, {orderNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {orderNo}))");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {orderNo} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeFkName)} FOREIGN KEY ({tenantId}, {orderNo}) REFERENCES {parent} ({tenantId}, {orderNo}))");
    }

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

    private static async Task SetupReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ReferentialChildTable, provider.Escape(ReferentialChildTable)));
        await ExecuteAsync(connection, DropTable(kind, ReferentialParentTable, provider.Escape(ReferentialParentTable)));

        var parent = provider.Escape(ReferentialParentTable);
        var child = provider.Escape(ReferentialChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NULL, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(ReferentialFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET NULL ON UPDATE CASCADE)");
    }

    private static async Task SetupRestrictReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ReferentialRestrictChildTable, provider.Escape(ReferentialRestrictChildTable)));
        await ExecuteAsync(connection, DropTable(kind, ReferentialRestrictParentTable, provider.Escape(ReferentialRestrictParentTable)));

        var parent = provider.Escape(ReferentialRestrictParentTable);
        var child = provider.Escape(ReferentialRestrictChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(ReferentialRestrictFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE)");
    }

    private static async Task SetupSetDefaultReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ReferentialDefaultChildTable, provider.Escape(ReferentialDefaultChildTable)));
        await ExecuteAsync(connection, DropTable(kind, ReferentialDefaultParentTable, provider.Escape(ReferentialDefaultParentTable)));

        var parent = provider.Escape(ReferentialDefaultParentTable);
        var child = provider.Escape(ReferentialDefaultChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape("DF_ScaffoldLiveDefaultChild_ParentId")} DEFAULT (0)"
            : "DEFAULT 0";

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL {defaultClause}, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(ReferentialDefaultFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)");
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

    private static async Task SetupCompositeUniqueAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, UniqueChildTable, provider.Escape(UniqueChildTable)));
        await ExecuteAsync(connection, DropTable(kind, UniqueParentTable, provider.Escape(UniqueParentTable)));

        var parent = provider.Escape(UniqueParentTable);
        var child = provider.Escape(UniqueChildTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var externalNo = provider.Escape("ExternalNo");
        var name = provider.Escape("Name");
        var eventName = provider.Escape("EventName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {externalNo} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(UniqueIndexName)} ON {parent} ({tenantId}, {externalNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {externalNo} {TextType(kind, 40)} NOT NULL, {eventName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(UniqueFkName)} FOREIGN KEY ({tenantId}, {externalNo}) REFERENCES {parent} ({tenantId}, {externalNo}))");
    }

    private static async Task SetupCompositeRoleForeignKeysAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownCompositeRoleForeignKeysAsync(connection, provider, kind);

        var account = provider.Escape(CompositeRoleAccountTable);
        var transfer = provider.Escape(CompositeRoleTransferTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var primaryAccountNo = provider.Escape("PrimaryAccountNo");
        var backupAccountNo = provider.Escape("BackupAccountNo");
        var name = provider.Escape("Name");
        var amount = provider.Escape("Amount");

        await ExecuteAsync(connection,
            $"CREATE TABLE {account} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(CompositeRoleAccountIndexName)} ON {account} ({tenantId}, {accountNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {transfer} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {primaryAccountNo} {IntType(kind)} NOT NULL, {backupAccountNo} {IntType(kind)} NOT NULL, {amount} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeRolePrimaryFkName)} FOREIGN KEY ({tenantId}, {primaryAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}), " +
            $"CONSTRAINT {provider.Escape(CompositeRoleBackupFkName)} FOREIGN KEY ({tenantId}, {backupAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}))");
    }

    private static async Task SetupSingleColumnAlternateKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSingleColumnAlternateKeyAsync(connection, provider, kind);

        var parent = provider.Escape(SingleAlternateParentTable);
        var child = provider.Escape(SingleAlternateChildTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(SingleAlternateIndexName)} ON {parent} ({code})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentCode} {TextType(kind, 40)} NOT NULL, {notes} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(SingleAlternateFkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static async Task SetupNullableAlternateKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownNullableAlternateKeyAsync(connection, provider, kind);

        var parent = provider.Escape(NullableAlternateParentTable);
        var child = provider.Escape(NullableAlternateChildTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 40)} NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(NullableAlternateIndexName)} ON {parent} ({code})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentCode} {TextType(kind, 40)} NOT NULL, {notes} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(NullableAlternateFkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static async Task SetupUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(UniqueDependentParentTable);
        var profile = provider.Escape(UniqueDependentProfileTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(UniqueDependentFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(UniqueDependentIndexName)} ON {profile} ({parentId})");
    }

    private static async Task SetupOptionalUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownOptionalUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(OptionalUniqueParentTable);
        var profile = provider.Escape(OptionalUniqueProfileTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(OptionalUniqueFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(OptionalUniqueIndexName)} ON {profile} ({parentId})");
    }

    private static async Task SetupRoleNamedUniqueDependentForeignKeysAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownRoleNamedUniqueDependentForeignKeysAsync(connection, provider, kind);

        var parent = provider.Escape(RoleOneParentTable);
        var profile = provider.Escape(RoleOneProfileTable);
        var id = provider.Escape("Id");
        var primaryAccountId = provider.Escape("PrimaryAccountId");
        var backupAccountId = provider.Escape("BackupAccountId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {primaryAccountId} {IntType(kind)} NOT NULL, {backupAccountId} {IntType(kind)} NOT NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(RoleOnePrimaryFkName)} FOREIGN KEY ({primaryAccountId}) REFERENCES {parent} ({id}), " +
            $"CONSTRAINT {provider.Escape(RoleOneBackupFkName)} FOREIGN KEY ({backupAccountId}) REFERENCES {parent} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(RoleOnePrimaryIndexName)} ON {profile} ({primaryAccountId})");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(RoleOneBackupIndexName)} ON {profile} ({backupAccountId})");
    }

    private static async Task SetupSharedPrimaryKeyForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSharedPrimaryKeyForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(SharedPkParentTable);
        var profile = provider.Escape(SharedPkProfileTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(SharedPkFkName)} FOREIGN KEY ({id}) REFERENCES {parent} ({id}))");
    }

    private static async Task SetupCompositeUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(CompositeUniqueDependentParentTable);
        var profile = provider.Escape(CompositeUniqueDependentProfileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(CompositeUniqueDependentParentIndexName)} ON {parent} ({tenantId}, {accountNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeUniqueDependentFkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(CompositeUniqueDependentProfileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static async Task SetupOptionalCompositeUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownOptionalCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(OptionalCompositeUniqueDependentParentTable);
        var profile = provider.Escape(OptionalCompositeUniqueDependentProfileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(OptionalCompositeUniqueDependentParentIndexName)} ON {parent} ({tenantId}, {accountNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(OptionalCompositeUniqueDependentFkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(OptionalCompositeUniqueDependentProfileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static async Task SetupDecimalPrecisionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, DecimalPrecisionTable, provider.Escape(DecimalPrecisionTable)));

        var table = provider.Escape(DecimalPrecisionTable);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL)");
    }

    private static async Task SetupStringBinaryFacetsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind, bool alternate)
    {
        await ExecuteAsync(connection, DropTable(kind, StringBinaryFacetTable, provider.Escape(StringBinaryFacetTable)));

        var table = provider.Escape(StringBinaryFacetTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var fixedCode = provider.Escape("FixedCode");
        var token = provider.Escape("Token");
        var (codeType, fixedCodeType, tokenType) = kind switch
        {
            ProviderKind.SqlServer when alternate => ("NVARCHAR(40)", "NCHAR(12)", "VARBINARY(16)"),
            ProviderKind.SqlServer => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Postgres when alternate => ("CHAR(40)", "VARCHAR(12)", "BYTEA"),
            ProviderKind.Postgres => ("VARCHAR(40)", "CHAR(12)", "BYTEA"),
            ProviderKind.MySql when alternate => ("CHAR(40)", "VARCHAR(12)", "VARBINARY(16)"),
            ProviderKind.MySql => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Sqlite when alternate => ("CHAR(40)", "VARCHAR(12)", "VARBINARY(16)"),
            ProviderKind.Sqlite => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
    }

    private static async Task SetupTemporalStoreTypeTableAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownTemporalStoreTypeTableAsync(connection, provider, kind);

        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, TemporalStoreTypeTable),
            ProviderKind.Postgres => Qualified(provider, "public", TemporalStoreTypeTable),
            _ => provider.Escape(TemporalStoreTypeTable)
        };
        var id = provider.Escape("Id");
        var businessDate = provider.Escape("BusinessDate");
        var startsAt = provider.Escape("StartsAt");
        var createdAt = provider.Escape("CreatedAt");
        var offsetAt = provider.Escape("OffsetAt");
        var sql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {businessDate} date NOT NULL, {startsAt} time NULL, {createdAt} datetime2 NOT NULL, {offsetAt} datetimeoffset NULL)",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {businessDate} date NOT NULL, {startsAt} time without time zone NULL, {createdAt} timestamp without time zone NOT NULL, {offsetAt} timestamp with time zone NULL)",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {businessDate} DATE NOT NULL, {startsAt} TIME NULL, {createdAt} DATETIME NOT NULL)",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} INTEGER PRIMARY KEY, {businessDate} DATE NOT NULL, {startsAt} TIME NULL, {createdAt} DATETIME NOT NULL, {offsetAt} DATETIMEOFFSET NULL)",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection, sql);
    }

}
