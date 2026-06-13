#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Index metadata scaffold integration tests.

    [Fact]
    public async Task ScaffoldAsync_WithSingleColumnIndexes_GeneratesIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedWidget (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Code TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedWidget_Code ON IndexedWidget(Code);
            CREATE INDEX IX_IndexedWidget_Name ON IndexedWidget(Name);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "IndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedWidget.cs"));
            Assert.Contains("using nORM.Configuration;", entityCode);
            Assert.Contains("[Index(\"IX_IndexedWidget_Code\", IsUnique = true)]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedWidget_Name\")]", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUnnamedSqliteUniqueConstraint_UsesStableIndexName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE UniqueConstraintWidget (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Code TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "UniqueConstraintIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "UniqueConstraintWidget.cs"));
            Assert.Contains("[Index(\"UX_UniqueConstraintWidget_Code\", IsUnique = true)]", entityCode);
            Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeIndex_GeneratesOrderedIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedOrder (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TenantId INTEGER NOT NULL,
                OrderNo TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedOrder_Tenant_OrderNo ON IndexedOrder(TenantId, OrderNo);
            CREATE INDEX IX_IndexedOrder_Tenant ON IndexedOrder(TenantId);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedOrder.cs"));
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant\")]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant_OrderNo\", IsUnique = true, Order = 0)]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant_OrderNo\", IsUnique = true, Order = 1)]", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqlitePartialAndExpressionIndexes_EmitsIndexMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedProviderSpecific (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE INDEX IX_IndexedProviderSpecific_Name_Active ON IndexedProviderSpecific(Name) WHERE Active = 1;
            CREATE INDEX IX_IndexedProviderSpecific_LowerName ON IndexedProviderSpecific(lower(Name));
            CREATE INDEX IX_IndexedProviderSpecific_LowerName_Active ON IndexedProviderSpecific(lower(Name)) WHERE Active = 1;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ProviderIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedProviderSpecific.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ProviderIndexCtx.cs"));

            Assert.Contains("[Index(\"IX_IndexedProviderSpecific_Name_Active\", FilterSql = \"Active = 1\")]", entityCode);
            Assert.DoesNotContain("[Index(\"IX_IndexedProviderSpecific_LowerName\")]", entityCode);
            Assert.Contains("mb.Entity<IndexedProviderSpecific>().HasExpressionIndex(\"IX_IndexedProviderSpecific_LowerName\", \"lower(Name)\");", contextCode);
            Assert.Contains("mb.Entity<IndexedProviderSpecific>().HasExpressionIndex(\"IX_IndexedProviderSpecific_LowerName_Active\", \"lower(Name)\", filterSql: \"Active = 1\");", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithFormattedSqlitePartialIndex_EmitsFilterMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedFormattedPartial (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE INDEX IX_IndexedFormattedPartial_Name_Active
                ON IndexedFormattedPartial(Name)
                WHERE Active = 1;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FormattedPartialIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedFormattedPartial.cs"));

            Assert.Contains("[Index(\"IX_IndexedFormattedPartial_Name_Active\", FilterSql = \"Active = 1\")]", entityCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDescendingIndex_EmitsIndexMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedDirection (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE INDEX IX_IndexedDirection_Name_Desc ON IndexedDirection(Name DESC);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DirectionIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedDirection.cs"));
            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");

            Assert.Contains("[Index(\"IX_IndexedDirection_Name_Desc\", IsDescending = true)]", entityCode);
            Assert.False(File.Exists(warningPath), "Descending ordinary column indexes should scaffold as portable index metadata.");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDescendingPartialExpressionIndex_EmitsExpressionIndexMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedExpressionWarning (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedExpressionWarning_LowerName_Desc
                ON IndexedExpressionWarning(lower(Name) DESC)
                WHERE Active = 1;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ExpressionWarningIndexCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "ExpressionWarningIndexCtx.cs"));
            var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

            Assert.Contains(
                "mb.Entity<IndexedExpressionWarning>().HasExpressionIndex(\"IX_IndexedExpressionWarning_LowerName_Desc\", \"lower(Name) DESC\", isUnique: true, filterSql: \"Active = 1\");",
                contextCode,
                StringComparison.Ordinal);
            Assert.False(File.Exists(warningJsonPath), "Supported descending expression indexes should be emitted instead of warned.");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ScaffoldContext_WithExpressionIndexFacets_EmitsExtendedExpressionIndexCall()
    {
        var expressionIndex = new DatabaseScaffolder.ScaffoldExpressionIndexConfiguration(
            "main.IndexedProviderSpecific",
            "IndexedProviderSpecific",
            "IX_IndexedProviderSpecific_LowerName",
            "lower(Name)",
            IsUnique: true,
            FilterSql: "Active = 1")
        {
            IncludedColumnNames = new[] { "Score" },
            NullSortOrder = IndexNullSortOrder.First,
            NullsNotDistinct = true
        };

        var code = ScaffoldContextAdapter.Write(
            "TestNs",
            "ProviderIndexCtx",
            new[] { "IndexedProviderSpecific" },
            Array.Empty<DatabaseScaffolder.ScaffoldRelationship>(),
            Array.Empty<DatabaseScaffolder.ScaffoldManyToManyJoin>(),
            expressionIndexConfigurations: new[] { expressionIndex });

        Assert.Contains(
            "mb.Entity<IndexedProviderSpecific>().HasExpressionIndex(\"IX_IndexedProviderSpecific_LowerName\", \"lower(Name)\", isUnique: true, filterSql: \"Active = 1\", includedColumnNames: new[] { \"Score\" }, nullsNotDistinct: true, nullSortOrder: IndexNullSortOrder.First);",
            code,
            StringComparison.Ordinal);
    }

}
