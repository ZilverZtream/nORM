#nullable enable
using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderScaffoldingParityTests
{
    private const string AuthorTable = "ScaffoldLiveAuthor";
    private const string BookTable = "ScaffoldLiveBook";
    private const string LabelTable = "ScaffoldLiveLabel";
    private const string BookLabelTable = "ScaffoldLiveBookLabel";
    private const string FkName = "FK_ScaffoldLiveBook_Author";
    private const string BookLabelBookFkName = "FK_ScaffoldLiveBookLabel_Book";
    private const string BookLabelLabelFkName = "FK_ScaffoldLiveBookLabel_Label";
    private const string CompositeParentTable = "ScaffoldLiveCompositeParent";
    private const string CompositeChildTable = "ScaffoldLiveCompositeChild";
    private const string CompositeFkName = "FK_ScaffoldLiveCompositeChild_Parent";
    private const string WarningTable = "ScaffoldLiveWarning";
    private const string KeylessTable = "ScaffoldLiveKeyless";
    private const string WarningView = "ScaffoldLiveWarningView";
    private const string ProviderIndexTable = "ScaffoldLiveProviderIndex";
    private const string ProviderPartialIndex = "IX_ScaffoldLiveProviderIndex_Partial";
    private const string ProviderExpressionIndex = "IX_ScaffoldLiveProviderIndex_Expression";
    private const string ProviderIncludedIndex = "IX_ScaffoldLiveProviderIndex_Included";

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
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, BookLabelTable + ".cs")));
                Assert.Contains("public List<ScaffoldLiveBook> ScaffoldLiveBooks { get; set; } = new();", authorCode);
                Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
                Assert.Contains("[Index(\"IX_ScaffoldLiveBook_Author_Title\", Order = 0)]", bookCode);
                Assert.Contains("[Index(\"IX_ScaffoldLiveBook_Author_Title\", Order = 1)]", bookCode);
                Assert.Contains("public ScaffoldLiveAuthor? ScaffoldLiveAuthor { get; set; }", bookCode);
                Assert.Contains("public List<ScaffoldLiveLabel> ScaffoldLiveLabels { get; set; } = new();", bookCode);
                Assert.Contains(".HasMany(p => p.ScaffoldLiveBooks)", contextCode);
                Assert.Contains(".WithOne(d => d.ScaffoldLiveAuthor)", contextCode);
                Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id);", contextCode);
                Assert.Contains(".HasMany<ScaffoldLiveLabel>(p => p.ScaffoldLiveLabels)", contextCode);
                Assert.Contains($".UsingTable(\"", contextCode);
                Assert.Contains(BookLabelTable, contextCode);
                Assert.Contains("\"BookId\", \"LabelId\");", contextCode);
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
    public async Task ScaffoldAsync_reports_same_composite_fk_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeContext",
                    new ScaffoldOptions { Tables = new[] { CompositeParentTable, CompositeChildTable }, OverwriteFiles = false });

                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeChildTable + ".cs"));
                var warnings = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
                Assert.Contains("Composite Foreign Keys", warnings, StringComparison.Ordinal);
                if (kind != ProviderKind.Sqlite)
                    Assert.Contains(CompositeFkName, warnings, StringComparison.Ordinal);

                var composites = warningJson.RootElement
                    .GetProperty("compositeForeignKeys")
                    .EnumerateArray()
                    .ToArray();
                var composite = kind == ProviderKind.Sqlite
                    ? composites.Single()
                    : composites.Single(e => e.GetProperty("constraint").GetString() == CompositeFkName);

                Assert.Equal(CompositeChildTable, composite.GetProperty("dependentTable").GetString()!.Split('.').Last());
                Assert.Equal(new[] { "TenantId", "OrderNo" }, composite.GetProperty("dependentColumns").EnumerateArray().Select(e => e.GetString()).ToArray());
                Assert.Equal(CompositeParentTable, composite.GetProperty("principalTable").GetString()!.Split('.').Last());
                Assert.Equal(new[] { "TenantId", "OrderNo" }, composite.GetProperty("principalColumns").EnumerateArray().Select(e => e.GetString()).ToArray());
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_owned_and_keyless_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupWarningDiagnosticsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_warnings_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldWarningContext",
                    new ScaffoldOptions { Tables = new[] { WarningTable, KeylessTable }, OverwriteFiles = false });

                var warnings = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.Contains("Provider-Owned Schema Features", warnings, StringComparison.Ordinal);
                Assert.Contains("Default", warnings, StringComparison.Ordinal);
                Assert.Contains("MissingPrimaryKey", warnings, StringComparison.Ordinal);

                var providerOwned = warningJson.RootElement
                    .GetProperty("providerOwnedSchemaFeatures")
                    .EnumerateArray()
                    .ToArray();

                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "Default" &&
                    item.GetProperty("table").GetString()!.Split('.').Last() == WarningTable &&
                    item.GetProperty("name").GetString() == "Status" &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("default", StringComparison.OrdinalIgnoreCase));

                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString()!.Split('.').Last() == KeylessTable &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("primary key", StringComparison.OrdinalIgnoreCase));
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownWarningDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_view_filter_fails_with_skipped_object_diagnostic_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSkippedViewAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_view_filter_" + Guid.NewGuid().ToString("N"));
            try
            {
                var ex = await Assert.ThrowsAsync<nORM.Core.NormConfigurationException>(() =>
                    DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldViewContext",
                        new ScaffoldOptions { Tables = new[] { WarningView }, OverwriteFiles = false }));

                Assert.Contains("matched database object", ex.Message, StringComparison.Ordinal);
                Assert.Contains("View", ex.Message, StringComparison.Ordinal);
                Assert.Contains(WarningView, ex.Message, StringComparison.Ordinal);
                Assert.Contains("does not emit as entity classes", ex.Message, StringComparison.Ordinal);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSkippedViewAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_specific_index_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupProviderSpecificIndexesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var warnings = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.DoesNotContain(ProviderPartialIndex, entityCode, StringComparison.Ordinal);
                Assert.Contains("PartialIndex", warnings, StringComparison.Ordinal);
                Assert.Contains(ProviderPartialIndex, warnings, StringComparison.Ordinal);

                if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
                {
                    Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains("ExpressionIndex", warnings, StringComparison.Ordinal);
                    Assert.Contains(ProviderExpressionIndex, warnings, StringComparison.Ordinal);
                }

                if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                {
                    Assert.DoesNotContain(ProviderIncludedIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains("IncludedColumnIndex", warnings, StringComparison.Ordinal);
                    Assert.Contains(ProviderIncludedIndex, warnings, StringComparison.Ordinal);
                }

                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PartialIndex" &&
                    item.GetProperty("name").GetString() == ProviderPartialIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
            }
        }
    }

    private static async Task SetupAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, BookLabelTable, provider.Escape(BookLabelTable)));
        await ExecuteAsync(connection, DropTable(kind, BookTable, provider.Escape(BookTable)));
        await ExecuteAsync(connection, DropTable(kind, LabelTable, provider.Escape(LabelTable)));
        await ExecuteAsync(connection, DropTable(kind, AuthorTable, provider.Escape(AuthorTable)));

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("Author_Id");
        var bookId = provider.Escape("BookId");
        var labelId = provider.Escape("LabelId");
        var author = provider.Escape(AuthorTable);
        var book = provider.Escape(BookTable);
        var label = provider.Escape(LabelTable);
        var bookLabel = provider.Escape(BookLabelTable);

        await ExecuteAsync(connection, $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {authorId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(FkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}))");
        await ExecuteAsync(connection, $"CREATE TABLE {label} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {bookLabel} ({bookId} {IntType(kind)} NOT NULL, {labelId} {IntType(kind)} NOT NULL, PRIMARY KEY ({bookId}, {labelId}), " +
            $"CONSTRAINT {provider.Escape(BookLabelBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}), " +
            $"CONSTRAINT {provider.Escape(BookLabelLabelFkName)} FOREIGN KEY ({labelId}) REFERENCES {label} ({id}))");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveBook_Author_Title")} ON {book} ({authorId}, {title})");
    }

    private static async Task SetupCompositeAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeChildTable, provider.Escape(CompositeChildTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeParentTable, provider.Escape(CompositeParentTable)));

        var parent = provider.Escape(CompositeParentTable);
        var child = provider.Escape(CompositeChildTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({tenantId} {IntType(kind)} NOT NULL, {orderNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {orderNo}))");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {orderNo} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeFkName)} FOREIGN KEY ({tenantId}, {orderNo}) REFERENCES {parent} ({tenantId}, {orderNo}))");
    }

    private static async Task SetupWarningDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, KeylessTable, provider.Escape(KeylessTable)));
        await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));

        var warning = provider.Escape(WarningTable);
        var keyless = provider.Escape(KeylessTable);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");
        var externalId = provider.Escape("ExternalId");
        var payload = provider.Escape("Payload");
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape("DF_ScaffoldLiveWarning_Status")} DEFAULT ('new')"
            : "DEFAULT 'new'";

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL {defaultClause})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {keyless} ({externalId} {TextType(kind, 40)} NOT NULL, {payload} {TextType(kind, 80)} NOT NULL)");
    }

    private static async Task SetupSkippedViewAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropView(kind, WarningView, provider.Escape(WarningView)));
        await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));

        var warning = provider.Escape(WarningTable);
        var view = provider.Escape(WarningView);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE VIEW {view} AS SELECT {id}, {status} FROM {warning}");
    }

    private static async Task SetupProviderSpecificIndexesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var activeType = kind == ProviderKind.SqlServer ? "BIT" : kind == ProviderKind.Postgres ? "BOOLEAN" : "INTEGER";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL, {active} {activeType} NOT NULL, {includedValue} {IntType(kind)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialIndex)} ON {table} ({name}) WHERE {activePredicate}");

        if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} (lower({name}))");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderIncludedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static async Task TeardownAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, BookLabelTable, provider.Escape(BookLabelTable)));
            await ExecuteAsync(connection, DropTable(kind, BookTable, provider.Escape(BookTable)));
            await ExecuteAsync(connection, DropTable(kind, LabelTable, provider.Escape(LabelTable)));
            await ExecuteAsync(connection, DropTable(kind, AuthorTable, provider.Escape(AuthorTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownCompositeAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositeChildTable, provider.Escape(CompositeChildTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeParentTable, provider.Escape(CompositeParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownWarningDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, KeylessTable, provider.Escape(KeylessTable)));
            await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSkippedViewAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropView(kind, WarningView, provider.Escape(WarningView)));
            await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownProviderSpecificIndexesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static string DropView(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'V') IS NOT NULL DROP VIEW {escapedName}"
        : $"DROP VIEW IF EXISTS {escapedName}";

    private static string IntType(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string TextType(ProviderKind kind, int length) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({length})"
        : kind == ProviderKind.Sqlite
            ? "TEXT"
            : $"VARCHAR({length})";
}
