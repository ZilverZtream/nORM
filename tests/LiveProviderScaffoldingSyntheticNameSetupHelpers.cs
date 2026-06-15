#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string SyntheticCheckTable = "ScaffoldLiveSyntheticCheck";
    private const string NamedUniqueTable = "ScaffoldLiveNamedUnique";
    private const string SyntheticUniqueTable = "ScaffoldLiveSyntheticUnique";
    private const string SyntheticFkParentTable = "ScaffoldLiveSyntheticFkParent";
    private const string SyntheticFkChildTable = "ScaffoldLiveSyntheticFkChild";
    private const string NamedUniqueCodeConstraint = "UQ_ScaffoldLiveNamedUnique_Code";
    private const string NamedUniqueCompositeConstraint = "UQ_ScaffoldLiveNamedUnique_Tenant_External";

    private static async Task SetupSyntheticCheckConstraintAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownSyntheticCheckConstraintAsync(connection, provider, kind);

        var table = provider.Escape(SyntheticCheckTable);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {amount} {IntType(kind)} NOT NULL, CHECK ({amount} > 0))");
    }

    private static async Task TeardownSyntheticCheckConstraintAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SyntheticCheckTable, provider.Escape(SyntheticCheckTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task SetupSyntheticUniqueConstraintIndexAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownSyntheticUniqueConstraintIndexAsync(connection, provider, kind);

        var table = provider.Escape(SyntheticUniqueTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 80)} NOT NULL UNIQUE, {name} {TextType(kind, 80)} NOT NULL)");
    }

    private static async Task TeardownSyntheticUniqueConstraintIndexAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SyntheticUniqueTable, provider.Escape(SyntheticUniqueTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task SetupNamedUniqueConstraintIndexAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownNamedUniqueConstraintIndexAsync(connection, provider, kind);

        var table = provider.Escape(NamedUniqueTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var tenantId = provider.Escape("TenantId");
        var externalNo = provider.Escape("ExternalNo");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"""
            CREATE TABLE {table} (
                {id} {IntType(kind)} NOT NULL PRIMARY KEY,
                {code} {TextType(kind, 80)} NOT NULL,
                {tenantId} {IntType(kind)} NOT NULL,
                {externalNo} {TextType(kind, 80)} NOT NULL,
                {name} {TextType(kind, 80)} NOT NULL,
                CONSTRAINT {provider.Escape(NamedUniqueCodeConstraint)} UNIQUE ({code}),
                CONSTRAINT {provider.Escape(NamedUniqueCompositeConstraint)} UNIQUE ({tenantId}, {externalNo})
            )
            """);
    }

    private static async Task TeardownNamedUniqueConstraintIndexAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, NamedUniqueTable, provider.Escape(NamedUniqueTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task SetupSyntheticForeignKeyRelationshipAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownSyntheticForeignKeyRelationshipAsync(connection, provider, kind);

        var parent = provider.Escape(SyntheticFkParentTable);
        var child = provider.Escape(SyntheticFkChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static async Task TeardownSyntheticForeignKeyRelationshipAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SyntheticFkChildTable, provider.Escape(SyntheticFkChildTable)));
            await ExecuteAsync(connection, DropTable(kind, SyntheticFkParentTable, provider.Escape(SyntheticFkParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
