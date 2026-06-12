#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Scaffolding;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    private static string InvokeGetUnqualifiedName(string identifier)
    {
        var m = GetMethod("GetUnqualifiedName", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { identifier })!;
    }

    private static string? InvokeGetSchemaNameOrNull(string identifier)
    {
        var m = GetMethod("GetSchemaNameOrNull", new[] { typeof(string) });
        return (string?)m.Invoke(null, new object[] { identifier });
    }

    private static string InvokeScaffoldContext(string ns, string ctxName, IEnumerable<string> entities)
    {
        var m = GetMethod("ScaffoldContext", new[] { typeof(string), typeof(string), typeof(IEnumerable<string>) });
        return (string)m.Invoke(null, new object[] { ns, ctxName, entities })!;
    }

    private static string InvokeScaffoldContext(string ns, string ctxName, IEnumerable<string> entities, bool pluralizeQueryProperties)
    {
        var m = GetMethod("ScaffoldContext", new[] { typeof(string), typeof(string), typeof(IEnumerable<string>), typeof(bool) });
        return (string)m.Invoke(null, new object[] { ns, ctxName, entities, pluralizeQueryProperties })!;
    }

    private static string InvokeScaffoldContextWithNamedRelationship()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var relationship = Activator.CreateInstance(
            relationshipType,
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
            "FK_Book_Author_Custom")!;
        var relationships = Array.CreateInstance(relationshipType, 1);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        relationships.SetValue(relationship, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "Author", "Book" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    private static string InvokeScaffoldContextWithPrimaryKeyConstraintName()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var primaryKey = Activator.CreateInstance(
            primaryKeyType,
            "User",
            new[] { "Id" },
            "PK_User_Custom")!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 1);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        primaryKeys.SetValue(primaryKey, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    private static string InvokeScaffoldContextWithRoutineStub()
        => InvokeScaffoldContextWithRoutine("dbo", "GetRevenue", "SQL Server stored procedure; parameters=3; outputParameters=2; parameterModes=@tenantId:IN:int,@total:OUT:decimal(18,2),@message:INOUT:nvarchar(32); resultColumns=Id:int:0|Name:nvarchar(40):0");

    private static string InvokeScaffoldContextWithRoutineReturnStub()
        => InvokeScaffoldContextWithRoutine("dbo", "ApplyDiscount", "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@orderId:IN:int,return:RETURN:int");

    private static string InvokeScaffoldContextWithRoutine(string? schema, string name, string detail, bool useDatabaseNames = false)
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            schema,
            name,
            "Routine",
            detail,
            null)!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 1);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        routines.SetValue(routine, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, useDatabaseNames })!;
    }

    private static string InvokeScaffoldContextWithSequence(string? schema, string name, string detail, bool useDatabaseNames = false)
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var sequence = Activator.CreateInstance(
            skippedObjectType,
            schema,
            name,
            "Sequence",
            detail,
            null)!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 1);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        sequences.SetValue(sequence, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, useDatabaseNames })!;
    }

    private static string InvokeScaffoldContextWithIdentityOptions()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var identity = Activator.CreateInstance(
            identityOptionType,
            "dbo.Users",
            "User",
            "Id",
            "Id",
            1000L,
            25L)!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 1);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        identityOptions.SetValue(identity, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "User" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }


    private static string InvokeScaffoldContextWithPrecision(int? scale = 6)
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var precision = Activator.CreateInstance(
            precisionType,
            new object?[]
            {
                "dbo.Invoices",
                "Invoice",
                "Amount",
                "Amount",
                28,
                scale
            })!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 1);
        var columnFacets = Array.CreateInstance(columnFacetType, 0);
        precisions.SetValue(precision, 0);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "Invoice" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }

    private static string InvokeScaffoldContextWithColumnFacets()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var relationshipType = scaffolder.GetNestedType("ScaffoldRelationship", BindingFlags.NonPublic)!;
        var manyToManyType = scaffolder.GetNestedType("ScaffoldManyToManyJoin", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var primaryKeyType = scaffolder.GetNestedType("ScaffoldPrimaryKey", BindingFlags.NonPublic)!;
        var defaultValueType = scaffolder.GetNestedType("ScaffoldDefaultValueConfiguration", BindingFlags.NonPublic)!;
        var checkConstraintType = scaffolder.GetNestedType("ScaffoldCheckConstraintConfiguration", BindingFlags.NonPublic)!;
        var computedColumnType = scaffolder.GetNestedType("ScaffoldComputedColumnConfiguration", BindingFlags.NonPublic)!;
        var expressionIndexType = scaffolder.GetNestedType("ScaffoldExpressionIndexConfiguration", BindingFlags.NonPublic)!;
        var collationType = scaffolder.GetNestedType("ScaffoldCollationConfiguration", BindingFlags.NonPublic)!;
        var identityOptionType = scaffolder.GetNestedType("ScaffoldIdentityOptionConfiguration", BindingFlags.NonPublic)!;
        var precisionType = scaffolder.GetNestedType("ScaffoldPrecisionConfiguration", BindingFlags.NonPublic)!;
        var columnFacetType = scaffolder.GetNestedType("ScaffoldColumnFacetConfiguration", BindingFlags.NonPublic)!;
        var codeFacet = Activator.CreateInstance(
            columnFacetType,
            new object?[]
            {
                "dbo.Customers",
                "Customer",
                "Code",
                "Code",
                40,
                false,
                true
            })!;
        var tokenFacet = Activator.CreateInstance(
            columnFacetType,
            new object?[]
            {
                "dbo.Customers",
                "Customer",
                "Token",
                "Token",
                16,
                null,
                true
            })!;
        var relationships = Array.CreateInstance(relationshipType, 0);
        var manyToMany = Array.CreateInstance(manyToManyType, 0);
        var routines = Array.CreateInstance(skippedObjectType, 0);
        var primaryKeys = Array.CreateInstance(primaryKeyType, 0);
        var defaultValues = Array.CreateInstance(defaultValueType, 0);
        var checkConstraints = Array.CreateInstance(checkConstraintType, 0);
        var computedColumns = Array.CreateInstance(computedColumnType, 0);
        var expressionIndexes = Array.CreateInstance(expressionIndexType, 0);
        var collations = Array.CreateInstance(collationType, 0);
        var sequences = Array.CreateInstance(skippedObjectType, 0);
        var identityOptions = Array.CreateInstance(identityOptionType, 0);
        var precisions = Array.CreateInstance(precisionType, 0);
        var columnFacets = Array.CreateInstance(columnFacetType, 2);
        columnFacets.SetValue(codeFacet, 0);
        columnFacets.SetValue(tokenFacet, 1);
        var method = scaffolder
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == "ScaffoldContextWithRelationships");

        return (string)method.Invoke(null, new object?[] { "MyApp", "AppDbContext", new[] { "Customer" }, relationships, manyToMany, routines, primaryKeys, defaultValues, checkConstraints, computedColumns, expressionIndexes, collations, sequences, identityOptions, precisions, columnFacets, true, true, null, false })!;
    }
}
