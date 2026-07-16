using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public sealed class ProviderCapabilitiesTests
{
    [Fact]
    public void Providers_ExposeExpectedV1CapabilityDescriptors()
    {
        AssertProvider(
            new SqlServerProvider(),
            "SQL Server",
            new Version(13, 0),
            maxParameters: 2_100,
            nativeBulk: true);

        AssertProvider(
            new PostgresProvider(new SqliteParameterFactory()),
            "PostgreSQL",
            new Version(12, 0),
            maxParameters: 32_767,
            nativeBulk: true);

        AssertProvider(
            new MySqlProvider(new SqliteParameterFactory()),
            "MySQL",
            new Version(8, 0),
            maxParameters: 65_535,
            nativeBulk: true);

        AssertProvider(
            new SqliteProvider(),
            "SQLite",
            new Version(3, 25),
            maxParameters: 999,
            nativeBulk: false);
    }

    [Fact]
    public async Task Provider_initialization_rejects_unsupported_actual_server_version_async()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var provider = new VersionGateProvider(new Version(99, 0), "3.46.1");

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(
            () => provider.InitializeConnectionAsync(cn, CancellationToken.None));

        Assert.Contains("TestSQL server version 3.46.1 is not supported", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Minimum supported version is 99.0", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Provider_initialization_rejects_unsupported_actual_server_version_sync()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var provider = new VersionGateProvider(new Version(99, 0), "PostgreSQL 9.4.26 on x86_64");

        var ex = Assert.Throws<NormConfigurationException>(() => provider.InitializeConnection(cn));

        Assert.Contains("TestSQL server version 9.4.26 is not supported", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Minimum supported version is 99.0", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Sqlite_provider_initialization_validates_current_engine_version()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        await new SqliteProvider().InitializeConnectionAsync(cn, CancellationToken.None);
    }

    private static void AssertProvider(
        DatabaseProvider provider,
        string name,
        Version minimumVersion,
        int maxParameters,
        bool nativeBulk)
    {
        var capabilities = provider.Capabilities;

        Assert.Equal(name, capabilities.ProviderName);
        Assert.Equal(minimumVersion, capabilities.MinimumServerVersion);
        Assert.Equal(maxParameters, capabilities.MaxParameters);
        Assert.Equal(maxParameters, provider.MaxParameters);
        Assert.True(capabilities.SupportsJson);
        Assert.True(capabilities.SupportsTemporalVersioning);
        Assert.True(capabilities.SupportsSavepoints);
        Assert.Equal(nativeBulk, capabilities.SupportsNativeBulkInsert);
        Assert.False(string.IsNullOrWhiteSpace(capabilities.Notes));
    }

    private sealed class VersionGateProvider : DatabaseProvider
    {
        private readonly Version _minimumVersion;
        private readonly string _versionText;

        public VersionGateProvider(Version minimumVersion, string versionText)
        {
            _minimumVersion = minimumVersion;
            _versionText = versionText;
        }

        public override ProviderCapabilities Capabilities => new(
            "TestSQL",
            _minimumVersion,
            100,
            supportsJson: false,
            supportsTemporalVersioning: false,
            supportsNativeBulkInsert: false,
            supportsSavepoints: false,
            "Test provider.");

        public override string Escape(string id) => id;

        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
        }

        public override string GetIdentityRetrievalString(TableMapping m) => string.Empty;

        public override DbParameter CreateParameter(string name, object? value)
            => new SqliteParameter(name, value ?? DBNull.Value);

        public override string? TranslateFunction(string name, Type declaringType, params string[] args) => null;

        public override string TranslateJsonPathAccess(string columnName, string jsonPath) => columnName;

        public override string GenerateCreateHistoryTableSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
            => string.Empty;

        public override string GenerateTemporalTriggersSql(TableMapping mapping, System.Collections.Generic.IReadOnlyList<LiveColumnInfo>? liveColumns = null) => string.Empty;

        protected override Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
            => Task.FromResult<string?>(_versionText);

        protected override string? GetServerVersionString(DbConnection connection) => _versionText;
    }
}
