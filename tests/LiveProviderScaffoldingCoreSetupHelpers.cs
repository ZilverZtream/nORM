#nullable enable
using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

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

}
