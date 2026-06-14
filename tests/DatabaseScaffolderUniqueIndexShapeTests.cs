#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void UniqueIndexShape_RejectsIncompleteColumnCountForUnorderedColumnSet()
    {
        var indexes = new[]
        {
            new ScaffoldIndexInfo(
                "Order",
                "TenantId",
                "UX_Order_Tenant_ExternalNo_Status",
                IsUnique: true,
                ColumnCount: 3,
                Ordinal: 0,
                IsDescending: false,
                IsIncluded: false,
                NullSortOrder: IndexNullSortOrder.Default,
                NullsNotDistinct: false,
                FilterSql: null),
            new ScaffoldIndexInfo(
                "Order",
                "ExternalNo",
                "UX_Order_Tenant_ExternalNo_Status",
                IsUnique: true,
                ColumnCount: 3,
                Ordinal: 1,
                IsDescending: false,
                IsIncluded: false,
                NullSortOrder: IndexNullSortOrder.Default,
                NullsNotDistinct: false,
                FilterSql: null)
        };

        Assert.False(ScaffoldForeignKeyShape.HasExactUniqueColumnSet(
            indexes,
            "Order",
            new HashSet<string>(new[] { "TenantId", "ExternalNo" }, StringComparer.OrdinalIgnoreCase)));
    }

    [Fact]
    public void BuildManyToManyJoins_WithIncompleteUniqueIndexMetadata_DoesNotEmitUsingTable()
    {
        var shape = CreateSyntheticBridgeShape(
            joinPrimaryKeyColumns: new[] { "BridgeHash" },
            joinColumnNames: new[] { "BridgeHash", "AuthorId", "BookId" },
            databaseGeneratedColumns: new[] { "BridgeHash" },
            identityColumns: Array.Empty<string>(),
            includeUniqueForeignKeyIndex: true,
            uniqueForeignKeyIndexColumnCount: 3);

        var joins = ScaffoldRelationshipAdapter.BuildManyToManyJoins(
            shape.ForeignKeys,
            shape.Tables,
            shape.EntityByTable,
            shape.ColumnProperties,
            shape.PrimaryKeys,
            shape.IdentityColumns,
            shape.DatabaseGeneratedColumns,
            shape.Indexes,
            shape.NonNullableColumns,
            shape.ProviderOwnedWriteBlockedTableKeys,
            shape.MemberNames);

        Assert.Empty(joins);

        var reasons = ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableReasons(
            "AuthorBook",
            shape.ForeignKeys,
            shape.PrimaryKeys,
            shape.ColumnProperties,
            shape.NonNullableColumns,
            shape.DatabaseGeneratedColumns,
            shape.IdentityColumns,
            shape.Indexes,
            shape.ProviderOwnedWriteBlockedTableKeys);

        Assert.Contains("missing-exact-unique-index", reasons);
        var metadata = ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableMetadata(
            "AuthorBook",
            shape.ForeignKeys,
            shape.PrimaryKeys,
            shape.ColumnProperties,
            shape.NonNullableColumns,
            shape.DatabaseGeneratedColumns,
            shape.IdentityColumns,
            shape.Indexes,
            shape.ProviderOwnedWriteBlockedTableKeys);
        Assert.False((bool)metadata["hasExactForeignKeyUniqueIndex"]!);
    }
}
