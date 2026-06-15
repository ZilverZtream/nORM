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
                new ScaffoldRelationship(
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
                new ScaffoldPrimaryKey(
                    "User",
                    new[] { "Id" },
                    "PK_User_Custom")
            });

    private static string InvokeScaffoldContextWithRoutineStub()
        => InvokeScaffoldContextWithRoutine("dbo", "GetRevenue", "SQL Server stored procedure; parameters=3; outputParameters=2; parameterModes=@tenantId:IN:int,@total:OUT:decimal(18,2),@message:INOUT:nvarchar(32); resultColumns=Id:int:0|Name:nvarchar(40):0");

    private static string InvokeScaffoldContextWithRoutineStubOnly()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            Array.Empty<string>(),
            routineStubs: new[]
            {
                new ScaffoldSkippedObject(
                    "dbo",
                    "GetRevenue",
                    "Routine",
                    "SQL Server stored procedure; parameters=1; parameterModes=@tenantId:IN:int",
                    null)
            });

    private static string InvokeScaffoldContextWithRoutineReturnStub()
        => InvokeScaffoldContextWithRoutine("dbo", "ApplyDiscount", "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@orderId:IN:int,return:RETURN:int");

    private static string InvokeScaffoldContextWithRoutine(string? schema, string name, string detail, bool useDatabaseNames = false)
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            routineStubs: new[]
            {
                new ScaffoldSkippedObject(
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
                new ScaffoldSkippedObject(
                    schema,
                    name,
                    "Sequence",
                    detail,
                    null)
            },
            useDatabaseNames: useDatabaseNames);

    private static string InvokeScaffoldContextWithSequenceOnly()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            Array.Empty<string>(),
            sequenceStubs: new[]
            {
                new ScaffoldSkippedObject(
                    "dbo",
                    "OrderNo",
                    "Sequence",
                    "SQL Server sequence; dataType=bigint",
                    null)
            });

    private static string InvokeScaffoldContextWithIdentityOptions()
        => WriteScaffoldContext(
            "MyApp",
            "AppDbContext",
            new[] { "User" },
            identityOptionConfigurations: new[]
            {
                new ScaffoldIdentityOptionConfiguration(
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
                new ScaffoldPrecisionConfiguration(
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
                new ScaffoldColumnFacetConfiguration(
                    "dbo.Customers",
                    "Customer",
                    "Code",
                    "Code",
                    40,
                    false,
                    true),
                new ScaffoldColumnFacetConfiguration(
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
        IReadOnlyList<ScaffoldRelationship>? relationships = null,
        IReadOnlyList<ScaffoldManyToManyJoin>? manyToManyJoins = null,
        IReadOnlyList<ScaffoldSkippedObject>? routineStubs = null,
        IReadOnlyList<ScaffoldPrimaryKey>? compositePrimaryKeys = null,
        IReadOnlyList<ScaffoldDefaultValueConfiguration>? defaultValueConfigurations = null,
        IReadOnlyList<ScaffoldCheckConstraintConfiguration>? checkConstraintConfigurations = null,
        IReadOnlyList<ScaffoldComputedColumnConfiguration>? computedColumnConfigurations = null,
        IReadOnlyList<ScaffoldExpressionIndexConfiguration>? expressionIndexConfigurations = null,
        IReadOnlyList<ScaffoldCollationConfiguration>? collationConfigurations = null,
        IReadOnlyList<ScaffoldSkippedObject>? sequenceStubs = null,
        IReadOnlyList<ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null,
        IReadOnlyList<ScaffoldPrecisionConfiguration>? precisionConfigurations = null,
        IReadOnlyList<ScaffoldColumnFacetConfiguration>? columnFacetConfigurations = null,
        bool usePluralizer = true,
        bool useNullableReferenceTypes = true,
        string? entityNamespaceName = null,
        bool useDatabaseNames = false)
        => ScaffoldContextAdapter.Write(
            namespaceName,
            contextName,
            entities,
            relationships ?? Array.Empty<ScaffoldRelationship>(),
            manyToManyJoins ?? Array.Empty<ScaffoldManyToManyJoin>(),
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
