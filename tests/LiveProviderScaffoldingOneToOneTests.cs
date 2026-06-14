#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_one_to_one_for_unique_dependent_fk_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupUniqueDependentForeignKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_unique_dependent_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldUniqueDependentContext",
                    new ScaffoldOptions { Tables = new[] { UniqueDependentParentTable, UniqueDependentProfileTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, UniqueDependentParentTable + ".cs"));
                var profileCode = await File.ReadAllTextAsync(Path.Combine(dir, UniqueDependentProfileTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldUniqueDependentContext.cs"));

                Assert.Contains($"public {UniqueDependentProfileTable}? {UniqueDependentProfileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{UniqueDependentProfileTable}>", parentCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(ParentId))]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{UniqueDependentIndexName}\", IsUnique = true)]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {UniqueDependentParentTable} {UniqueDependentParentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{UniqueDependentProfileTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains($".WithOne(d => d.{UniqueDependentParentTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentId", "p => p.Id", UniqueDependentFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownUniqueDependentForeignKeyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_optional_one_to_one_for_nullable_unique_dependent_fk_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupOptionalUniqueDependentForeignKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_optional_unique_dependent_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldOptionalUniqueDependentContext",
                    new ScaffoldOptions { Tables = new[] { OptionalUniqueParentTable, OptionalUniqueProfileTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, OptionalUniqueParentTable + ".cs"));
                var profileCode = await File.ReadAllTextAsync(Path.Combine(dir, OptionalUniqueProfileTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldOptionalUniqueDependentContext.cs"));

                Assert.Contains($"public {OptionalUniqueProfileTable}? {OptionalUniqueProfileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{OptionalUniqueProfileTable}>", parentCode, StringComparison.Ordinal);
                Assert.Matches(@"public (int|long)\? ParentId \{ get; set; \}", profileCode);
                Assert.Contains("[ForeignKey(nameof(ParentId))]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{OptionalUniqueIndexName}\", IsUnique = true)]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {OptionalUniqueParentTable}? {OptionalUniqueParentTable} {{ get; set; }}", profileCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"public {OptionalUniqueParentTable} {OptionalUniqueParentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{OptionalUniqueProfileTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains($".WithOne(d => d.{OptionalUniqueParentTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentId", "p => p.Id", OptionalUniqueFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownOptionalUniqueDependentForeignKeyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_role_named_one_to_one_for_multiple_unique_dependent_fks_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupRoleNamedUniqueDependentForeignKeysAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_role_one_to_one_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldRoleOneToOneContext",
                    new ScaffoldOptions { Tables = new[] { RoleOneParentTable, RoleOneProfileTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, RoleOneParentTable + ".cs"));
                var profileCode = await File.ReadAllTextAsync(Path.Combine(dir, RoleOneProfileTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldRoleOneToOneContext.cs"));

                Assert.Contains($"public {RoleOneProfileTable}? {RoleOneProfileTable}ByBackupAccountId {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.Contains($"public {RoleOneProfileTable}? {RoleOneProfileTable}ByPrimaryAccountId {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{RoleOneProfileTable}>", parentCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{RoleOneBackupIndexName}\", IsUnique = true)]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{RoleOnePrimaryIndexName}\", IsUnique = true)]", profileCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(BackupAccountId))]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {RoleOneParentTable} BackupAccount {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(PrimaryAccountId))]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {RoleOneParentTable} PrimaryAccount {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{RoleOneProfileTable}ByBackupAccountId)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.BackupAccount)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.BackupAccountId", "p => p.Id", RoleOneBackupFkName), contextCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{RoleOneProfileTable}ByPrimaryAccountId)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.PrimaryAccount)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.PrimaryAccountId", "p => p.Id", RoleOnePrimaryFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownRoleNamedUniqueDependentForeignKeysAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_one_to_one_for_shared_primary_key_fk_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSharedPrimaryKeyForeignKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_shared_pk_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSharedPkOneToOneContext",
                    new ScaffoldOptions { Tables = new[] { SharedPkParentTable, SharedPkProfileTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, SharedPkParentTable + ".cs"));
                var profileCode = await File.ReadAllTextAsync(Path.Combine(dir, SharedPkProfileTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSharedPkOneToOneContext.cs"));

                Assert.Contains($"public {SharedPkProfileTable}? {SharedPkProfileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{SharedPkProfileTable}>", parentCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(Id))]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {SharedPkParentTable} {SharedPkParentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{SharedPkProfileTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains($".WithOne(d => d.{SharedPkParentTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.Id", "p => p.Id", SharedPkFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSharedPrimaryKeyForeignKeyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_one_to_one_for_composite_unique_dependent_fk_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_unique_dependent_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeUniqueDependentContext",
                    new ScaffoldOptions { Tables = new[] { CompositeUniqueDependentParentTable, CompositeUniqueDependentProfileTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeUniqueDependentParentTable + ".cs"));
                var profileCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeUniqueDependentProfileTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeUniqueDependentContext.cs"));

                Assert.Contains($"public {CompositeUniqueDependentProfileTable}? {CompositeUniqueDependentProfileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{CompositeUniqueDependentProfileTable}>", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ForeignKey(", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositeUniqueDependentParentTable} {CompositeUniqueDependentParentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{CompositeUniqueDependentProfileTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains($".WithOne(d => d.{CompositeUniqueDependentParentTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.AccountNo }", "p => new { p.TenantId, p.AccountNo }", CompositeUniqueDependentFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_optional_one_to_one_for_nullable_composite_unique_dependent_fk_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupOptionalCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_optional_composite_unique_dependent_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldOptionalCompositeUniqueDependentContext",
                    new ScaffoldOptions { Tables = new[] { OptionalCompositeUniqueDependentParentTable, OptionalCompositeUniqueDependentProfileTable }, OverwriteFiles = false });

                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, OptionalCompositeUniqueDependentParentTable + ".cs"));
                var profileCode = await File.ReadAllTextAsync(Path.Combine(dir, OptionalCompositeUniqueDependentProfileTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldOptionalCompositeUniqueDependentContext.cs"));

                Assert.Contains($"public {OptionalCompositeUniqueDependentProfileTable}? {OptionalCompositeUniqueDependentProfileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{OptionalCompositeUniqueDependentProfileTable}>", parentCode, StringComparison.Ordinal);
                Assert.Matches(@"public (int|long) TenantId \{ get; set; \}", profileCode);
                Assert.Matches(@"public (int|long)\? AccountNo \{ get; set; \}", profileCode);
                Assert.Contains($"[Index(\"{OptionalCompositeUniqueDependentProfileIndexName}\", IsUnique = true, Order = 0)]", profileCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{OptionalCompositeUniqueDependentProfileIndexName}\", IsUnique = true, Order = 1)]", profileCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ForeignKey(", profileCode, StringComparison.Ordinal);
                Assert.Contains($"public {OptionalCompositeUniqueDependentParentTable}? {OptionalCompositeUniqueDependentParentTable} {{ get; set; }}", profileCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"public {OptionalCompositeUniqueDependentParentTable} {OptionalCompositeUniqueDependentParentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{OptionalCompositeUniqueDependentProfileTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains($".WithOne(d => d.{OptionalCompositeUniqueDependentParentTable})", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.AccountNo }", "p => new { p.TenantId, p.AccountNo }", OptionalCompositeUniqueDependentFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownOptionalCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);
            }
        }
    }
}
