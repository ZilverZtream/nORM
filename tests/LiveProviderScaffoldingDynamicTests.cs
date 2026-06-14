#nullable enable

using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider dynamic scaffold parity tests.

    private const string DynamicUnmodeledDefaultTable = "ScaffoldLiveDynamicUnmodeledDefault";
    private const string DynamicUnmodeledDefaultName = "DF_ScaffoldLiveDynamicUnmodeledDefault_Status";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_view_query_artifacts_read_only_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSkippedViewAsync(connection, provider, kind);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DefaultSchemaTableFilter(kind, WarningView));

                Assert.NotNull(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                Assert.NotNull(type.GetProperty("Id"));
                Assert.NotNull(type.GetProperty("Status"));
            }
            finally
            {
                await TeardownSkippedViewAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_keyless_tables_read_only_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupWarningDiagnosticsAsync(connection, provider, kind);
            try
            {
                var keylessType = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DefaultSchemaTableFilter(kind, KeylessTable));
                var keyedType = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DefaultSchemaTableFilter(kind, WarningTable));

                Assert.NotNull(keylessType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                Assert.Null(keyedType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                Assert.NotNull(keylessType.GetProperty("ExternalId"));
                Assert.NotNull(keylessType.GetProperty("Payload"));
            }
            finally
            {
                await TeardownWarningDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_unmodeled_defaults_read_only_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownDynamicUnmodeledDefaultAsync(connection, provider, kind);
            try
            {
                await ExecuteAsync(connection, DynamicUnmodeledDefaultSql(kind, provider));

                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DefaultSchemaTableFilter(kind, DynamicUnmodeledDefaultTable));

                Assert.NotNull(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                Assert.NotNull(type.GetProperty("Status"));
            }
            finally
            {
                await TeardownDynamicUnmodeledDefaultAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_generated_columns_as_computed_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(kind, DynamicComputedTable, provider.Escape(DynamicComputedTable)));
            try
            {
                await ExecuteAsync(connection, GeneratedColumnTableSql(kind, provider));

                var type = new DynamicEntityTypeGenerator().GenerateEntityType(connection, DynamicComputedTable);
                var generated = type.GetProperty("NameLength")!
                    .GetCustomAttributes(typeof(DatabaseGeneratedAttribute), inherit: false)
                    .Cast<DatabaseGeneratedAttribute>()
                    .SingleOrDefault();

                Assert.NotNull(generated);
                Assert.Equal(DatabaseGeneratedOption.Computed, generated.DatabaseGeneratedOption);
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(kind, DynamicComputedTable, provider.Escape(DynamicComputedTable)));
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_identity_columns_as_database_generated_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(kind, DynamicIdentityTable, provider.Escape(DynamicIdentityTable)));
            try
            {
                await ExecuteAsync(connection, IdentityColumnTableSql(kind, provider));

                var type = new DynamicEntityTypeGenerator().GenerateEntityType(connection, DynamicIdentityTable);
                var generated = type.GetProperty("Id")!
                    .GetCustomAttributes(typeof(DatabaseGeneratedAttribute), inherit: false)
                    .Cast<DatabaseGeneratedAttribute>()
                    .SingleOrDefault();

                Assert.NotNull(generated);
                Assert.Equal(DatabaseGeneratedOption.Identity, generated.DatabaseGeneratedOption);
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(kind, DynamicIdentityTable, provider.Escape(DynamicIdentityTable)));
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_preserves_composite_primary_key_order_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(kind, DynamicCompositeKeyTable, provider.Escape(DynamicCompositeKeyTable)));
            try
            {
                await ExecuteAsync(connection, DynamicCompositeKeyTableSql(kind, provider));

                var type = new DynamicEntityTypeGenerator().GenerateEntityType(connection, DynamicCompositeKeyTable);
                var properties = type.GetProperties().Select(prop => prop.Name).ToArray();

                Assert.Equal("LocalId", properties[0]);
                Assert.Equal("TenantId", properties[1]);
                Assert.Contains(
                    type.GetProperty("LocalId")!.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.KeyAttribute), inherit: false),
                    attr => attr is System.ComponentModel.DataAnnotations.KeyAttribute);
                Assert.Contains(
                    type.GetProperty("TenantId")!.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.KeyAttribute), inherit: false),
                    attr => attr is System.ComponentModel.DataAnnotations.KeyAttribute);
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(kind, DynamicCompositeKeyTable, provider.Escape(DynamicCompositeKeyTable)));
            }
        }
    }

    private static string DynamicUnmodeledDefaultSql(ProviderKind kind, DatabaseProvider provider)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, DynamicUnmodeledDefaultTable),
            ProviderKind.Postgres => Qualified(provider, "public", DynamicUnmodeledDefaultTable),
            _ => provider.Escape(DynamicUnmodeledDefaultTable)
        };
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");
        var defaultSql = kind switch
        {
            ProviderKind.SqlServer => $"CONSTRAINT {provider.Escape(DynamicUnmodeledDefaultName)} DEFAULT (COALESCE(N'NEW', N'OLD'))",
            ProviderKind.Postgres => "DEFAULT COALESCE('NEW','OLD')",
            ProviderKind.MySql => "DEFAULT (COALESCE('NEW','OLD'))",
            ProviderKind.Sqlite => "DEFAULT (coalesce('NEW','OLD'))",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        return $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL {defaultSql})";
    }

    private static async Task TeardownDynamicUnmodeledDefaultAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            var escaped = kind switch
            {
                ProviderKind.SqlServer => SqlServerQualified(provider, DynamicUnmodeledDefaultTable),
                ProviderKind.Postgres => Qualified(provider, "public", DynamicUnmodeledDefaultTable),
                _ => provider.Escape(DynamicUnmodeledDefaultTable)
            };
            await ExecuteAsync(connection, DropTable(kind, DefaultSchemaTableFilter(kind, DynamicUnmodeledDefaultTable), escaped));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

}
