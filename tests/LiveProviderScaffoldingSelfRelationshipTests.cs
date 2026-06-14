#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string SelfOnePersonTable = "ScaffoldLiveSelfOnePerson";
    private const string SelfOnePersonFkName = "FK_ScaffoldLiveSelfOnePerson_Spouse";
    private const string SelfOnePersonIndexName = "UX_ScaffoldLiveSelfOnePerson_Spouse";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_self_referencing_one_to_one_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSelfReferencingUniqueDependentForeignKeyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_self_one_to_one_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSelfOneToOneContext",
                    new ScaffoldOptions { Tables = new[] { SelfOnePersonTable }, OverwriteFiles = false });

                var personCode = await File.ReadAllTextAsync(Path.Combine(dir, SelfOnePersonTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSelfOneToOneContext.cs"));

                Assert.Contains($"[Index(\"{SelfOnePersonIndexName}\", IsUnique = true)]", personCode, StringComparison.Ordinal);
                Assert.Contains("[ForeignKey(nameof(SpouseId))]", personCode, StringComparison.Ordinal);
                Assert.Matches(@"public (int|long)\? SpouseId \{ get; set; \}", personCode);
                Assert.Contains($"public {SelfOnePersonTable}? Spouse {{ get; set; }}", personCode, StringComparison.Ordinal);
                Assert.Contains($"public {SelfOnePersonTable}? {SelfOnePersonTable}BySpouseId {{ get; set; }}", personCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"List<{SelfOnePersonTable}>", personCode, StringComparison.Ordinal);
                Assert.Contains($".HasOne(p => p.{SelfOnePersonTable}BySpouseId)", contextCode, StringComparison.Ordinal);
                Assert.Contains(".WithOne(d => d.Spouse)", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.SpouseId", "p => p.Id", SelfOnePersonFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSelfReferencingUniqueDependentForeignKeyAsync(connection, provider, kind);
            }
        }
    }

    private static async Task SetupSelfReferencingUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSelfReferencingUniqueDependentForeignKeyAsync(connection, provider, kind);

        var person = provider.Escape(SelfOnePersonTable);
        var id = provider.Escape("Id");
        var spouseId = provider.Escape("SpouseId");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {person} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {spouseId} {IntType(kind)} NULL, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(SelfOnePersonFkName)} FOREIGN KEY ({spouseId}) REFERENCES {person} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(SelfOnePersonIndexName)} ON {person} ({spouseId})");
    }

    private static async Task TeardownSelfReferencingUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SelfOnePersonTable, provider.Escape(SelfOnePersonTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
