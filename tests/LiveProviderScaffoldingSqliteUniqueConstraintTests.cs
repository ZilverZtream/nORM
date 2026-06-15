#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Fact]
    public async Task ScaffoldAsync_preserves_sqlite_named_unique_constraint_index_names()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Sqlite);
        if (Skip.If(live is null, "Live provider Sqlite not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            const string tableName = "ScaffoldLiveSqliteNamedUnique";
            var table = provider.Escape(tableName);
            var id = provider.Escape("Id");
            var code = provider.Escape("Code");
            var tenantId = provider.Escape("TenantId");
            var externalNo = provider.Escape("ExternalNo");

            try
            {
                await ExecuteAsync(connection, DropTable(ProviderKind.Sqlite, tableName, table));
                await ExecuteAsync(connection,
                    $"""
                    CREATE TABLE {table} (
                        {id} INTEGER NOT NULL PRIMARY KEY,
                        {code} TEXT NOT NULL CONSTRAINT UQ_ScaffoldLiveSqliteNamedUnique_Code UNIQUE,
                        {tenantId} INTEGER NOT NULL,
                        {externalNo} TEXT NOT NULL,
                        CONSTRAINT UQ_ScaffoldLiveSqliteNamedUnique_Tenant_External UNIQUE ({tenantId}, {externalNo})
                    )
                    """);

                var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlite_named_unique_" + Guid.NewGuid().ToString("N"));
                try
                {
                    await DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldSqliteNamedUniqueContext",
                        new ScaffoldOptions { Tables = new[] { tableName }, OverwriteFiles = false });

                    var entityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, tableName));

                    Assert.Contains("[Index(\"UQ_ScaffoldLiveSqliteNamedUnique_Code\", IsUnique = true)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Index(\"UQ_ScaffoldLiveSqliteNamedUnique_Tenant_External\", IsUnique = true, Order = 0)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Index(\"UQ_ScaffoldLiveSqliteNamedUnique_Tenant_External\", IsUnique = true, Order = 1)]", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
                    Assert.DoesNotContain("UX_ScaffoldLiveSqliteNamedUnique", entityCode, StringComparison.OrdinalIgnoreCase);

                    AssertScaffoldOutputBuilds(dir);
                }
                finally
                {
                    if (Directory.Exists(dir))
                        Directory.Delete(dir, recursive: true);
                }
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(ProviderKind.Sqlite, tableName, table));
            }
        }
    }
}
