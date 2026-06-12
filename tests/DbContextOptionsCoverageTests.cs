using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class DbContextOptionsCoverageTests
{
    [Fact]
    public void BulkBatchSize_Zero_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.BulkBatchSize = 0);
    }

    [Fact]
    public void BulkBatchSize_TooLarge_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.BulkBatchSize = 10001);
    }

    [Fact]
    public void BulkBatchSize_ValidValue_Succeeds()
    {
        var opts = new DbContextOptions();
        opts.BulkBatchSize = 500;
        Assert.Equal(500, opts.BulkBatchSize);
    }

    [Fact]
    public void MaxRecursionDepth_Zero_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxRecursionDepth = 0);
    }

    [Fact]
    public void MaxRecursionDepth_TooLarge_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxRecursionDepth = 201);
    }

    [Fact]
    public void MaxGroupJoinSize_Zero_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxGroupJoinSize = 0);
    }

    [Fact]
    public void MaxGroupJoinSize_Negative_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxGroupJoinSize = -1);
    }

    [Fact]
    public void Validate_ValidConfig_DoesNotThrow()
    {
        var opts = new DbContextOptions();
        opts.Validate(); // default config is valid
    }

    [Fact]
    public void Validate_InvalidRetryMaxRetries_Throws()
    {
        var opts = new DbContextOptions { RetryPolicy = new nORM.Enterprise.RetryPolicy { MaxRetries = 11 } };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_InvalidRetryBaseDelay_Throws()
    {
        var opts = new DbContextOptions
        {
            RetryPolicy = new nORM.Enterprise.RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.Zero }
        };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_InvalidBaseTimeout_Throws()
    {
        var opts = new DbContextOptions();
        opts.TimeoutConfiguration.BaseTimeout = TimeSpan.Zero;
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_EmptyTenantColumnName_Throws()
    {
        var opts = new DbContextOptions { TenantColumnName = "" };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_NegativeCacheExpiration_Throws()
    {
        var opts = new DbContextOptions { CacheExpiration = TimeSpan.Zero };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_NullInCommandInterceptors_Throws()
    {
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(null!);
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_NullInSaveChangesInterceptors_Throws()
    {
        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(null!);
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void AddGlobalFilter_EntityOnlyLambda_RegistersFilter()
    {
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<CovItem>(e => e.IsActive);
        Assert.True(opts.GlobalFilters.ContainsKey(typeof(CovItem)));
        Assert.Single(opts.GlobalFilters[typeof(CovItem)]);
    }
}

// Covers: CleanupNavigationContext full path, CleanupFromBatchedLoaders,
//         LazyNavigationCollection.Count/Add/Clear/Contains/Remove/CopyTo/IsReadOnly/
//         IndexOf/Insert/RemoveAt/Indexer/GetEnumerator/GetAsyncEnumerator,
//         NavigationPropertyExtensions.IsLoaded false-when-no-context
