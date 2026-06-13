#nullable enable

using System;
using System.Collections.Generic;
using nORM.Scaffolding;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    private static string InvokeGetUnqualifiedName(string identifier)
    {
        var idx = identifier.LastIndexOf('.');
        return idx >= 0 ? identifier[(idx + 1)..] : identifier;
    }

    private static string? InvokeGetSchemaNameOrNull(string identifier)
    {
        var idx = identifier.IndexOf('.');
        return idx > 0 ? identifier[..idx] : null;
    }

    private static string InvokeScaffoldContext(string ns, string ctxName, IEnumerable<string> entities)
        => WriteScaffoldContext(ns, ctxName, entities);

    private static string InvokeScaffoldContext(
        string ns,
        string ctxName,
        IEnumerable<string> entities,
        bool pluralizeQueryProperties)
        => WriteScaffoldContext(ns, ctxName, entities, usePluralizer: pluralizeQueryProperties);

    private static string InvokeScaffoldContextWithNamedRelationship()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "Author", "Book" },
            relationships: new[]
            {
                new DatabaseScaffolder.ScaffoldRelationship(
                    "Book",
                    "Author",
                    "Book",
                    "Author",
                    "AuthorId",
                    "Id",
                    "Author",
                    "Books",
                    false,
                    false,
                    "NO ACTION",
                    "NO ACTION",
                    "FK_Book_Author_Custom")
            });

    private static string InvokeScaffoldContextWithPrimaryKeyConstraintName()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            compositePrimaryKeys: new[]
            {
                new DatabaseScaffolder.ScaffoldPrimaryKey(
                    "User",
                    new[] { "Id" },
                    "PK_User_Custom")
            });

    private static string InvokeScaffoldContextWithRoutineStub()
        => InvokeScaffoldContextWithRoutine("dbo", "GetRevenue", "SQL Server stored procedure; parameters=3; outputParameters=2; parameterModes=@tenantId:IN:int,@total:OUT:decimal(18,2),@message:INOUT:nvarchar(32); resultColumns=Id:int:0|Name:nvarchar(40):0");

    private static string InvokeScaffoldContextWithRoutineReturnStub()
        => InvokeScaffoldContextWithRoutine("dbo", "ApplyDiscount", "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@orderId:IN:int,return:RETURN:int");

    private static string InvokeScaffoldContextWithRoutine(string? schema, string name, string detail, bool useDatabaseNames = false)
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            routineStubs: new[]
            {
                new DatabaseScaffolder.ScaffoldSkippedObject(
                    schema,
                    name,
                    "Routine",
                    detail,
                    null)
            },
            useDatabaseNames: useDatabaseNames);

    private static string InvokeScaffoldContextWithSequence(string? schema, string name, string detail, bool useDatabaseNames = false)
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            sequenceStubs: new[]
            {
                new DatabaseScaffolder.ScaffoldSkippedObject(
                    schema,
                    name,
                    "Sequence",
                    detail,
                    null)
            },
            useDatabaseNames: useDatabaseNames);

    private static string InvokeScaffoldContextWithIdentityOptions()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            identityOptionConfigurations: new[]
            {
                new DatabaseScaffolder.ScaffoldIdentityOptionConfiguration(
                    "dbo.Users",
                    "User",
                    "Id",
                    "Id",
                    1000L,
                    25L)
            });

    private static string InvokeScaffoldContextWithPrecision(int? scale = 6)
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "Invoice" },
            precisionConfigurations: new[]
            {
                new DatabaseScaffolder.ScaffoldPrecisionConfiguration(
                    "dbo.Invoices",
                    "Invoice",
                    "Amount",
                    "Amount",
                    28,
                    scale)
            });

    private static string InvokeScaffoldContextWithColumnFacets()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "Customer" },
            columnFacetConfigurations: new[]
            {
                new DatabaseScaffolder.ScaffoldColumnFacetConfiguration(
                    "dbo.Customers",
                    "Customer",
                    "Code",
                    "Code",
                    40,
                    false,
                    true),
                new DatabaseScaffolder.ScaffoldColumnFacetConfiguration(
                    "dbo.Customers",
                    "Customer",
                    "Token",
                    "Token",
                    16,
                    null,
                    true)
            });

    private static string WriteScaffoldContext(
        string namespaceName,
        string contextName,
        IEnumerable<string> entities,
        IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship>? relationships = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin>? manyToManyJoins = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject>? routineStubs = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey>? compositePrimaryKeys = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration>? defaultValueConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration>? checkConstraintConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration>? computedColumnConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration>? expressionIndexConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration>? collationConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject>? sequenceStubs = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration>? precisionConfigurations = null,
        IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration>? columnFacetConfigurations = null,
        bool usePluralizer = true,
        bool useNullableReferenceTypes = true,
        string? entityNamespaceName = null,
        bool useDatabaseNames = false)
        => ScaffoldContextAdapter.Write(
            namespaceName,
            contextName,
            entities,
            relationships ?? Array.Empty<DatabaseScaffolder.ScaffoldRelationship>(),
            manyToManyJoins ?? Array.Empty<DatabaseScaffolder.ScaffoldManyToManyJoin>(),
            routineStubs,
            compositePrimaryKeys,
            defaultValueConfigurations,
            checkConstraintConfigurations,
            computedColumnConfigurations,
            expressionIndexConfigurations,
            collationConfigurations,
            sequenceStubs,
            identityOptionConfigurations,
            precisionConfigurations,
            columnFacetConfigurations,
            usePluralizer,
            useNullableReferenceTypes,
            entityNamespaceName,
            useDatabaseNames);
}
