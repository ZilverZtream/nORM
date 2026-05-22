using System;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

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
            new Version(9, 5),
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
            new Version(3, 9),
            maxParameters: 999,
            nativeBulk: false);
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
}
