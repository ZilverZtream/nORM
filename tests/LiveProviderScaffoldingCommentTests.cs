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
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_preserves_database_comments_as_xml_docs_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupDatabaseCommentsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_comments_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCommentContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { DefaultSchemaTableFilter(kind, DatabaseCommentsTable) },
                        OverwriteFiles = false
                    });

                var entityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, DatabaseCommentsTable));

                if (kind == ProviderKind.Sqlite)
                {
                    Assert.Contains("/// Maps to column Name", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("Table &lt;summary&gt;", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("Name &lt;tag&gt;", entityCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains("/// Table &lt;summary&gt; &amp; description", entityCode, StringComparison.Ordinal);
                    Assert.Contains("/// Name &lt;tag&gt; &amp; details", entityCode, StringComparison.Ordinal);
                    Assert.Contains("/// <remarks>Maps to column Name</remarks>", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(DatabaseCommentsTableComment, entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(DatabaseCommentsColumnComment, entityCode, StringComparison.Ordinal);
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownDatabaseCommentsAsync(connection, provider, kind);
            }
        }
    }
}
