#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider relationship and alternate-key scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_same_single_fk_model_shape_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldContext",
                    new ScaffoldOptions { Tables = new[] { AuthorTable, BookTable, LabelTable, BookLabelTable }, OverwriteFiles = false });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, AuthorTable + ".cs"));
                var bookCode = await File.ReadAllTextAsync(Path.Combine(dir, BookTable + ".cs"));
                var labelCode = await File.ReadAllTextAsync(Path.Combine(dir, LabelTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, BookLabelTable + ".cs")));
                Assert.Contains("public List<ScaffoldLiveBook> ScaffoldLiveBooks { get; set; } = new();", authorCode);
                Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
                Assert.Contains("[Index(\"IX_ScaffoldLiveBook_Author_Title\", Order = 0)]", bookCode);
                Assert.Contains("[Index(\"IX_ScaffoldLiveBook_Author_Title\", Order = 1)]", bookCode);
                Assert.Contains("public ScaffoldLiveAuthor ScaffoldLiveAuthor { get; set; } = default!;", bookCode);
                Assert.Contains("public List<ScaffoldLiveLabel> ScaffoldLiveLabels { get; set; } = new();", bookCode);
                Assert.Contains("public List<ScaffoldLiveBook> ScaffoldLiveBooks { get; set; } = new();", labelCode);
                Assert.Contains(".HasMany(p => p.ScaffoldLiveBooks)", contextCode);
                Assert.Contains(".WithOne(d => d.ScaffoldLiveAuthor)", contextCode);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.AuthorId", "p => p.Id", FkName), contextCode);
                Assert.Contains(".HasMany<ScaffoldLiveLabel>(p => p.ScaffoldLiveLabels)", contextCode);
                Assert.Contains(".WithMany(p => p.ScaffoldLiveBooks)", contextCode);
                Assert.Contains($".UsingTable(\"", contextCode);
                Assert.Contains(BookLabelTable, contextCode);
                if (kind == ProviderKind.SqlServer)
                    Assert.Contains("\"BookId\", \"LabelId\", schema: \"dbo\");", contextCode);
                else if (kind == ProviderKind.Postgres)
                    Assert.Contains("\"BookId\", \"LabelId\", schema: \"public\");", contextCode);
                else
                    Assert.Contains("\"BookId\", \"LabelId\");", contextCode);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_no_relationships_keeps_scalar_fk_columns_and_join_entity_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_no_relationships_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldNoRelationshipsContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { AuthorTable, BookTable, LabelTable, BookLabelTable },
                        NoRelationships = true,
                        OverwriteFiles = false
                    });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, AuthorTable + ".cs"));
                var bookCode = await File.ReadAllTextAsync(Path.Combine(dir, BookTable + ".cs"));
                var labelCode = await File.ReadAllTextAsync(Path.Combine(dir, LabelTable + ".cs"));
                var joinCode = await File.ReadAllTextAsync(Path.Combine(dir, BookLabelTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldNoRelationshipsContext.cs"));

                Assert.DoesNotContain("List<ScaffoldLiveBook>", authorCode, StringComparison.Ordinal);
                Assert.DoesNotContain("List<ScaffoldLiveBook>", labelCode, StringComparison.Ordinal);
                Assert.Matches(@"public (?:int|long) AuthorId \{ get; set; \}", bookCode);
                Assert.Matches(@"public (?:int|long) BookId \{ get; set; \}", joinCode);
                Assert.Matches(@"public (?:int|long) LabelId \{ get; set; \}", joinCode);
                Assert.DoesNotContain("[ForeignKey(", bookCode, StringComparison.Ordinal);
                Assert.DoesNotContain("public ScaffoldLiveAuthor", bookCode, StringComparison.Ordinal);
                Assert.DoesNotContain("List<ScaffoldLiveLabel>", bookCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasMany", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("WithOne", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("WithMany", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("UsingTable", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_preserves_database_names_and_role_navigations_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupDatabaseNamesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_database_names_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldDatabaseNamesContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { DatabaseNamesCustomerTable, DatabaseNamesOrderLineTable },
                        UseDatabaseNames = true,
                        OverwriteFiles = false
                    });

                var customerCode = await File.ReadAllTextAsync(Path.Combine(dir, DatabaseNamesCustomerTable + ".cs"));
                var orderLineCode = await File.ReadAllTextAsync(Path.Combine(dir, DatabaseNamesOrderLineTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldDatabaseNamesContext.cs"));

                Assert.Contains($"public partial class {DatabaseNamesCustomerTable}", customerCode, StringComparison.Ordinal);
                Assert.Contains($"public partial class {DatabaseNamesOrderLineTable}", orderLineCode, StringComparison.Ordinal);
                Assert.Contains(" scaffold_database_names_customer_id { get; set; }", customerCode, StringComparison.Ordinal);
                Assert.Contains(" display_name { get; set; }", customerCode, StringComparison.Ordinal);
                Assert.Contains(" scaffold_database_names_order_line_id { get; set; }", orderLineCode, StringComparison.Ordinal);
                Assert.Contains(" billing_scaffold_database_names_customer_id { get; set; }", orderLineCode, StringComparison.Ordinal);
                Assert.Contains(" shipping_scaffold_database_names_customer_id { get; set; }", orderLineCode, StringComparison.Ordinal);
                Assert.Contains("public string SKU { get; set; } = default!;", orderLineCode, StringComparison.Ordinal);
                Assert.Contains("public string? @class { get; set; }", orderLineCode, StringComparison.Ordinal);
                Assert.Contains("public string? has_space { get; set; }", orderLineCode, StringComparison.Ordinal);
                Assert.Contains($"public {DatabaseNamesCustomerTable} BillingScaffoldDatabaseNamesCustomer {{ get; set; }} = default!;", orderLineCode, StringComparison.Ordinal);
                Assert.Contains($"public {DatabaseNamesCustomerTable}? ShippingScaffoldDatabaseNamesCustomer {{ get; set; }}", orderLineCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{DatabaseNamesOrderLineTable}> ScaffoldDatabaseNamesOrderLinesByBillingScaffoldDatabaseNamesCustomerId {{ get; set; }} = new();", customerCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{DatabaseNamesOrderLineTable}> ScaffoldDatabaseNamesOrderLinesByShippingScaffoldDatabaseNamesCustomerId {{ get; set; }} = new();", customerCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{DatabaseNamesCustomerTable}> scaffold_database_names_customers", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{DatabaseNamesOrderLineTable}> scaffold_database_names_order_lines", contextCode, StringComparison.Ordinal);
                Assert.Contains(".HasMany(p => p.ScaffoldDatabaseNamesOrderLinesByBillingScaffoldDatabaseNamesCustomerId)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.BillingScaffoldDatabaseNamesCustomer)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.billing_scaffold_database_names_customer_id", "p => p.scaffold_database_names_customer_id", DatabaseNamesBillingFkName), contextCode, StringComparison.Ordinal);
                Assert.Contains(".HasMany(p => p.ScaffoldDatabaseNamesOrderLinesByShippingScaffoldDatabaseNamesCustomerId)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.ShippingScaffoldDatabaseNamesCustomer)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.shipping_scaffold_database_names_customer_id", "p => p.scaffold_database_names_customer_id", DatabaseNamesShippingFkName), contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".UsingTable(\"{DatabaseNamesOrderLineTable}\"", contextCode, StringComparison.Ordinal);
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();
                Assert.Contains(joinTables, item =>
                    item.GetProperty("table").GetString()?.EndsWith(DatabaseNamesOrderLineTable, StringComparison.OrdinalIgnoreCase) == true &&
                    item.GetProperty("reasons").EnumerateArray().Any(reason => reason.GetString() == "payload-columns"));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownDatabaseNamesAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_composite_fk_to_unique_index_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeUniqueAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_unique_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldUniqueContext",
                    new ScaffoldOptions { Tables = new[] { UniqueParentTable, UniqueChildTable }, OverwriteFiles = false });

                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, UniqueChildTable + ".cs"));
                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, UniqueParentTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldUniqueContext.cs"));

                Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
                Assert.Contains($"public {UniqueParentTable} {UniqueParentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{UniqueChildTable}> {UniqueChildTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.ExternalNo }", "p => new { p.TenantId, p.ExternalNo }", UniqueFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeUniqueAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_role_named_navigations_for_multiple_composite_fks_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeRoleForeignKeysAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_role_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeRoleContext",
                    new ScaffoldOptions { Tables = new[] { CompositeRoleAccountTable, CompositeRoleTransferTable }, OverwriteFiles = false });

                var accountCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeRoleAccountTable + ".cs"));
                var transferCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeRoleTransferTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeRoleContext.cs"));

                Assert.Contains($"public List<{CompositeRoleTransferTable}> {CompositeRoleTransferTable}sByBackupAccountNo {{ get; set; }} = new();", accountCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{CompositeRoleTransferTable}> {CompositeRoleTransferTable}sByPrimaryAccountNo {{ get; set; }} = new();", accountCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositeRoleAccountTable} BackupAccount {{ get; set; }} = default!;", transferCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositeRoleAccountTable} PrimaryAccount {{ get; set; }} = default!;", transferCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"public {CompositeRoleAccountTable} Tenant", transferCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"{CompositeRoleTransferTable}sByTenantId", accountCode, StringComparison.Ordinal);
                Assert.Contains($".HasMany(p => p.{CompositeRoleTransferTable}sByBackupAccountNo)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.BackupAccount)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.BackupAccountNo }", "p => new { p.TenantId, p.AccountNo }", CompositeRoleBackupFkName), contextCode, StringComparison.Ordinal);
                Assert.Contains($".HasMany(p => p.{CompositeRoleTransferTable}sByPrimaryAccountNo)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.PrimaryAccount)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.PrimaryAccountNo }", "p => new { p.TenantId, p.AccountNo }", CompositeRolePrimaryFkName), contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".UsingTable(\"{CompositeRoleTransferTable}\"", contextCode, StringComparison.Ordinal);
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();
                Assert.Contains(joinTables, item =>
                    item.GetProperty("table").GetString()?.EndsWith(CompositeRoleTransferTable, StringComparison.OrdinalIgnoreCase) == true &&
                    item.GetProperty("reasons").EnumerateArray().Any(reason => reason.GetString() == "payload-columns"));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeRoleForeignKeysAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_single_column_fk_to_unique_index_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSingleColumnAlternateKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_single_alt_key_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSingleAlternateKeyContext",
                    new ScaffoldOptions { Tables = new[] { SingleAlternateParentTable, SingleAlternateChildTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, SingleAlternateParentTable + ".cs"));
                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, SingleAlternateChildTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSingleAlternateKeyContext.cs"));

                Assert.Contains("[Index(\"" + SingleAlternateIndexName + "\", IsUnique = true)]", parentCode, StringComparison.Ordinal);
                Assert.Contains("public List<ScaffoldLiveSingleAlternateChild> ScaffoldLiveSingleAlternateChilds { get; set; } = new();", parentCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(ParentCode))]", childCode, StringComparison.Ordinal);
                Assert.Contains("public ScaffoldLiveSingleAlternateParent ScaffoldLiveSingleAlternateParent { get; set; } = default!;", childCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentCode", "p => p.Code", SingleAlternateFkName), contextCode, StringComparison.Ordinal);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSingleColumnAlternateKeyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_fk_to_nullable_unique_index_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupNullableAlternateKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_nullable_alt_key_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldNullableAlternateKeyContext",
                    new ScaffoldOptions { Tables = new[] { NullableAlternateParentTable, NullableAlternateChildTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, NullableAlternateParentTable + ".cs"));
                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, NullableAlternateChildTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldNullableAlternateKeyContext.cs"));

                Assert.Contains("[Index(\"" + NullableAlternateIndexName + "\", IsUnique = true)]", parentCode, StringComparison.Ordinal);
                Assert.Contains("public string? Code { get; set; }", parentCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{NullableAlternateChildTable}> {NullableAlternateChildTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(ParentCode))]", childCode, StringComparison.Ordinal);
                Assert.Contains($"public {NullableAlternateParentTable} {NullableAlternateParentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentCode", "p => p.Code", NullableAlternateFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownNullableAlternateKeyAsync(connection, provider, kind);
            }
        }
    }
}
