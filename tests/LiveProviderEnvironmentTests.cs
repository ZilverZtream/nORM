using System;
using Xunit;

namespace nORM.Tests;

public class LiveProviderEnvironmentTests
{
    [Fact]
    public void GetByCanonicalName_FlagCanonicalValue_UsesConnectionStringAlias()
    {
        const string envVar = "NORM_TEST_SENTINEL_PROVIDER";
        const string aliasVar = envVar + "_CS";
        var original = Environment.GetEnvironmentVariable(envVar);
        var originalAlias = Environment.GetEnvironmentVariable(aliasVar);

        try
        {
            Environment.SetEnvironmentVariable(envVar, "1");
            Environment.SetEnvironmentVariable(aliasVar, "Host=127.0.0.1;Database=normtest");

            Assert.Equal("Host=127.0.0.1;Database=normtest", LiveProviderEnvironment.GetByCanonicalName(envVar));
        }
        finally
        {
            Environment.SetEnvironmentVariable(envVar, original);
            Environment.SetEnvironmentVariable(aliasVar, originalAlias);
        }
    }

    [Fact]
    public void GetByCanonicalName_RealCanonicalValue_WinsOverAlias()
    {
        const string envVar = "NORM_TEST_SENTINEL_PROVIDER";
        const string aliasVar = envVar + "_CS";
        var original = Environment.GetEnvironmentVariable(envVar);
        var originalAlias = Environment.GetEnvironmentVariable(aliasVar);

        try
        {
            Environment.SetEnvironmentVariable(envVar, "Host=canonical;Database=normtest");
            Environment.SetEnvironmentVariable(aliasVar, "Host=alias;Database=normtest");

            Assert.Equal("Host=canonical;Database=normtest", LiveProviderEnvironment.GetByCanonicalName(envVar));
        }
        finally
        {
            Environment.SetEnvironmentVariable(envVar, original);
            Environment.SetEnvironmentVariable(aliasVar, originalAlias);
        }
    }
}
