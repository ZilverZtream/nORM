#nullable enable

using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider dynamic scaffold parity tests.

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

}
