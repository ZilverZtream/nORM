#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Fact]
    public async Task ScaffoldAsync_emits_sqlserver_local_view_synonym_as_read_only_query_artifact()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var suffix = Guid.NewGuid().ToString("N")[..6];
        var baseTable = "ScaffoldLiveViewSynBase" + suffix;
        var viewName = "ScaffoldLiveViewSynReport" + suffix;
        var synonymName = "ScaffoldLiveViewSynonym" + suffix;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerViewSynonymAsync(connection, provider, baseTable, viewName, synonymName);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_view_synonym_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerViewSynonymContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { "dbo." + synonymName },
                        EmitQueryArtifacts = true,
                        OverwriteFiles = false
                    });

                var synonymCode = await File.ReadAllTextAsync(Path.Combine(dir, synonymName + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqlServerViewSynonymContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.Contains("[ReadOnlyEntity]", synonymCode, StringComparison.Ordinal);
                Assert.Contains($"[Table(\"{synonymName}", synonymCode, StringComparison.Ordinal);
                Assert.Contains("Schema = \"dbo\"", synonymCode, StringComparison.Ordinal);
                Assert.Contains("/// View &lt;summary&gt; &amp; description", synonymCode, StringComparison.Ordinal);
                Assert.Contains("/// Name &lt;view&gt; &amp; details", synonymCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[Key]", synonymCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{synonymName}>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasKey", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, baseTable + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, viewName + ".cs")));
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString() == "dbo." + synonymName);

                var dynamicSynonymType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(connection, "dbo." + synonymName);
                Assert.NotNull(dynamicSynonymType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerViewSynonymAsync(connection, provider, baseTable, viewName, synonymName);
            }
        }
    }

    private static async Task SetupSqlServerViewSynonymAsync(
        DbConnection connection,
        nORM.Providers.DatabaseProvider provider,
        string baseTable,
        string viewName,
        string synonymName)
    {
        await TeardownSqlServerViewSynonymAsync(connection, provider, baseTable, viewName, synonymName);

        var table = SqlServerQualified(provider, baseTable);
        var view = SqlServerQualified(provider, viewName);
        var synonym = SqlServerQualified(provider, synonymName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection, $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) NOT NULL)");
        await ExecuteAsync(connection, $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
        await ExecuteAsync(connection,
            "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName));
        await ExecuteAsync(connection,
            "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
        await ExecuteAsync(connection, $"CREATE SYNONYM {synonym} FOR {view}");
    }

    private static async Task TeardownSqlServerViewSynonymAsync(
        DbConnection connection,
        nORM.Providers.DatabaseProvider provider,
        string baseTable,
        string viewName,
        string synonymName)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{synonymName}', N'SN') IS NOT NULL DROP SYNONYM {SqlServerQualified(provider, synonymName)}");
            await ExecuteAsync(connection, DropView(ProviderKind.SqlServer, "dbo." + viewName, SqlServerQualified(provider, viewName)));
            await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, "dbo." + baseTable, SqlServerQualified(provider, baseTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
