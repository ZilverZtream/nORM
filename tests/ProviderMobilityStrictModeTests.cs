using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class ProviderMobilityStrictModeTests
{
    [Fact]
    public async Task Strict_provider_mobility_allows_generated_query_paths()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateMobilityTableAsync(connection);

        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        var rows = await ctx.Query<MobilityItem>()
            .Where(i => i.Id == 1)
            .Where(i => NormFunctions.Like(i.Name, "port%"))
            .Select(i => new MobilityItemDto { Id = i.Id, Name = i.Name })
            .ToListAsync();

        Assert.Single(rows);
        Assert.Equal("portable", rows[0].Name);
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_normal_application_linq_and_write_surface()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateNormalUsageTableAsync(connection);

        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        var inserted = await ctx.BulkInsertAsync(new[]
        {
            new StrictNormalItem { Id = 4, Category = "a", Name = "apricot", Price = 4.25m, Quantity = 4, Archived = false },
            new StrictNormalItem { Id = 5, Category = "b", Name = "banana", Price = 5.50m, Quantity = 5, Archived = false }
        });
        Assert.Equal(2, inserted);

        var page = await ctx.Query<StrictNormalItem>()
            .Where(item => !item.Archived && item.Price >= 2m && item.Name.Contains("a"))
            .OrderBy(item => item.Category)
            .ThenByDescending(item => item.Price)
            .Skip(0)
            .Take(10)
            .Select(item => new StrictNormalDto
            {
                Id = item.Id,
                Label = item.Category + ":" + item.Name,
                Total = item.Price * item.Quantity
            })
            .ToListAsync();

        Assert.Equal(new[] { 4, 2, 5 }, page.Select(item => item.Id).ToArray());

        var groups = await ctx.Query<StrictNormalItem>()
            .Where(item => !item.Archived)
            .GroupBy(item => item.Category)
            .Select(group => new StrictNormalGroupDto
            {
                Category = group.Key,
                Count = group.Count(),
                Quantity = group.Sum(item => item.Quantity)
            })
            .OrderBy(group => group.Category)
            .ToListAsync();

        Assert.Equal(2, groups.Count);
        Assert.Equal("a", groups[0].Category);
        Assert.Equal(3, groups[0].Count);
        Assert.Equal(7, groups[0].Quantity);

        var compiled = Norm.CompileQuery<DbContext, string, StrictNormalItem>(
            (db, category) => db.Query<StrictNormalItem>()
                .Where(item => item.Category == category && !item.Archived)
                .OrderBy(item => item.Id));
        var compiledRows = await compiled(ctx, "a");
        Assert.Equal(new[] { 1, 2, 4 }, compiledRows.Select(item => item.Id).ToArray());

        var updated = await ctx.Query<StrictNormalItem>()
            .Where(item => item.Category == "b")
            .ExecuteUpdateAsync(setters => setters.SetProperty(item => item.Archived, true));
        Assert.Equal(2, updated);

        var deleted = await ctx.Query<StrictNormalItem>()
            .Where(item => item.Archived)
            .ExecuteDeleteAsync();
        Assert.Equal(2, deleted);

        var remaining = await ctx.Query<StrictNormalItem>().CountAsync();
        Assert.Equal(3, remaining);
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_explicit_warn_client_projection_tail_after_server_shape()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateNormalUsageTableAsync(connection);

        var logger = new StrictCapturingLogger();
        var options = new DbContextOptions
        {
            Logger = logger
        }.UseStrictProviderMobility(ClientEvaluationPolicy.Warn);
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        var rows = await ctx.Query<StrictNormalItem>()
            .Where(item => !item.Archived && item.Category == "a")
            .OrderBy(item => item.Id)
            .Take(1)
            .Select(item => new StrictNormalDto
            {
                Id = item.Id,
                Label = FormatStrictLabel(item.Name),
                Total = item.Price * item.Quantity
            })
            .ToListAsync();

        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal("APPLE", rows[0].Label);
        Assert.Contains(logger.Messages, message => message.Contains("-- CLIENT-EVAL", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Strict_provider_mobility_blocks_custom_sql_function_attributes()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateMobilityTableAsync(connection);

        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.Query<MobilityItem>()
                .Where(i => CustomSoundex(i.Name) == "P634")
                .ToListAsync());
    }

    [Fact]
    public async Task Strict_provider_mobility_blocks_raw_sql_escape_hatches()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Connection);
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Provider);
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query("MobilityItem"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.FromSqlRawAsync<MobilityItem>("SELECT 1 AS Id, 'x' AS Name"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.QueryUnchangedAsync<MobilityItem>("SELECT 1 AS Id, 'x' AS Name"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.FromSqlInterpolatedAsync<MobilityItem>($"SELECT {1} AS Id, {'x'} AS Name"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.QueryUnchangedInterpolatedAsync<MobilityItem>($"SELECT {1} AS Id, {'x'} AS Name"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.CreateCompiledQueryCommandAsync());
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.ExecuteCompiledQueryListAsync(
                connection.CreateCommand(),
                static (_, _) => Task.FromResult(new MobilityItem())));
        Assert.Throws<NormUnsupportedFeatureException>(
            () => ctx.GetCompiledQueryMaterializer<MobilityItem>("MobilityItem"));
    }

    [Fact]
    public async Task Strict_provider_mobility_blocks_raw_transaction_handles()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        Assert.Null(ctx.Database.CurrentContextTransaction);
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Database.CurrentTransaction);
        await using var tx = await ctx.Database.BeginTransactionAsync();
        Assert.Same(tx, ctx.Database.CurrentContextTransaction);
        Assert.Throws<NormUnsupportedFeatureException>(() => tx.Transaction);
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.CreateSavepointAsync(null!, "sp"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.RollbackToSavepointAsync(null!, "sp"));

        await tx.RollbackAsync();
        Assert.Null(ctx.Database.CurrentContextTransaction);
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_wrapped_savepoint_workflow()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateMobilityTableAsync(connection);
        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new MobilityItem { Id = 2, Name = "before-savepoint" });
        await tx.CreateSavepointAsync("portable_sp");
        await ctx.InsertAsync(new MobilityItem { Id = 3, Name = "after-savepoint" });
        await tx.RollbackToSavepointAsync("portable_sp");
        await tx.CommitAsync();

        var ids = await ctx.Query<MobilityItem>()
            .OrderBy(item => item.Id)
            .Select(item => new MobilityItemDto { Id = item.Id, Name = item.Name })
            .ToListAsync();

        Assert.Equal(new[] { 1, 2 }, ids.Select(item => item.Id));
    }

    [Fact]
    public async Task Strict_provider_mobility_blocks_external_transaction_write_overloads()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateMobilityTableAsync(connection);
        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);
        await using var rawTx = await connection.BeginTransactionAsync();

        var item = new MobilityItem { Id = 2, Name = "outside" };
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.InsertAsync(item, rawTx));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.UpdateAsync(item, rawTx));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.DeleteAsync(item, rawTx));

        await rawTx.RollbackAsync();
    }

    [Fact]
    public async Task Strict_provider_mobility_blocks_stored_procedure_escape_hatches()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.ExecuteStoredProcedureAsync<MobilityItem>("SELECT 1 AS Id, 'x' AS Name"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.ExecuteStoredProcedureWithOutputAsync<MobilityItem>("SELECT 1 AS Id, 'x' AS Name"));
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
        {
            await foreach (var _ in ctx.ExecuteStoredProcedureAsAsyncEnumerable<MobilityItem>("SELECT 1 AS Id, 'x' AS Name"))
            {
            }
        });
    }

    [Fact]
    public void Strict_provider_mobility_blocks_provider_native_configuration()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        Assert.Throws<NormConfigurationException>(
            () => new DbContextOptions()
                .UseStrictProviderMobility()
                .EnableTemporalVersioning(TemporalStorageMode.ProviderNative));

        Assert.Throws<NormConfigurationException>(
            () => new DbContextOptions { TenantProvider = new FixedTenantProvider(1) }
                .UseStrictProviderMobility()
                .EnableNativeTenantSessionContext());

        var clientEval = new DbContextOptions().UseStrictProviderMobility();
        Assert.Throws<NormConfigurationException>(
            () => clientEval.ClientEvaluationPolicy = ClientEvaluationPolicy.Allow);
        clientEval.ClientEvaluationPolicy = ClientEvaluationPolicy.Warn;
        Assert.Equal(ClientEvaluationPolicy.Warn, clientEval.ClientEvaluationPolicy);
    }

    [Fact]
    public void Strict_provider_mobility_cannot_be_weakened_by_mutating_options()
    {
        var options = new DbContextOptions().UseStrictProviderMobility();

        options.ClientEvaluationPolicy = ClientEvaluationPolicy.Warn;
        Assert.Equal(ClientEvaluationPolicy.Warn, options.ClientEvaluationPolicy);
        Assert.Throws<NormConfigurationException>(
            () => options.ClientEvaluationPolicy = ClientEvaluationPolicy.Allow);
        Assert.Throws<NormConfigurationException>(
            () => options.TemporalStorageMode = TemporalStorageMode.ProviderNative);
        Assert.Throws<NormConfigurationException>(
            () => options.NativeTenantSecurityMode = NativeTenantSecurityMode.SessionContext);
        Assert.Throws<NormConfigurationException>(() =>
        {
            var withInterceptor = new DbContextOptions().UseStrictProviderMobility();
            withInterceptor.CommandInterceptors.Add(new StrictTestCommandInterceptor());
            withInterceptor.Validate();
        });

        var nativeFirst = new DbContextOptions();
        nativeFirst.TemporalStorageMode = TemporalStorageMode.ProviderNative;
        Assert.Throws<NormConfigurationException>(
            () => nativeFirst.ProviderMobilityMode = ProviderMobilityMode.Strict);

        var interceptorFirst = new DbContextOptions();
        interceptorFirst.CommandInterceptors.Add(new StrictTestCommandInterceptor());
        Assert.Throws<NormConfigurationException>(
            () => interceptorFirst.ProviderMobilityMode = ProviderMobilityMode.Strict);

        var clientEvalFirst = new DbContextOptions
        {
            ClientEvaluationPolicy = ClientEvaluationPolicy.Allow
        };
        Assert.Throws<NormConfigurationException>(
            () => clientEvalFirst.ProviderMobilityMode = ProviderMobilityMode.Strict);

        var warnFirst = new DbContextOptions
        {
            ClientEvaluationPolicy = ClientEvaluationPolicy.Warn
        };
        warnFirst.ProviderMobilityMode = ProviderMobilityMode.Strict;
        Assert.Equal(ClientEvaluationPolicy.Warn, warnFirst.ClientEvaluationPolicy);
    }

    [Fact]
    public void Strict_provider_mobility_is_pinned_to_the_context_instance()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var options = new DbContextOptions().UseStrictProviderMobility();
        using var ctx = new DbContext(connection, new SqliteProvider(), options);

        options.ProviderMobilityMode = ProviderMobilityMode.Compatibility;

        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Connection);
        Assert.True(ctx.IsStrictProviderMobility);
    }

    [Fact]
    public async Task Strict_provider_mobility_rejects_command_interceptors_added_after_context_creation()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateMobilityTableAsync(connection);
        var options = new DbContextOptions().UseStrictProviderMobility();
        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        options.CommandInterceptors.Add(new StrictTestCommandInterceptor());

        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.Query<MobilityItem>().Where(i => i.Id == 1).ToListAsync());
    }

    [Fact]
    public void Strict_provider_mobility_blocks_provider_native_ddl_escape_hatches()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var options = new DbContextOptions().UseStrictProviderMobility();
        using var ctx = new DbContext(connection, new SqliteProvider(), options);

        Assert.Throws<NormUnsupportedFeatureException>(
            () => ctx.GenerateNativeTenantPolicySql<MobilityItem>());
        Assert.Throws<NormUnsupportedFeatureException>(
            () => ctx.GenerateProviderNativeTemporalBootstrapSql<MobilityItem>());
    }

    private static async Task CreateMobilityTableAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE MobilityItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO MobilityItem (Id, Name) VALUES (1, 'portable');
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task CreateNormalUsageTableAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE StrictNormalItem (
                Id INTEGER PRIMARY KEY,
                Category TEXT NOT NULL,
                Name TEXT NOT NULL,
                Price NUMERIC NOT NULL,
                Quantity INTEGER NOT NULL,
                Archived INTEGER NOT NULL
            );
            INSERT INTO StrictNormalItem (Id, Category, Name, Price, Quantity, Archived) VALUES
                (1, 'a', 'apple', 1.50, 1, 0),
                (2, 'a', 'avocado', 2.00, 2, 0),
                (3, 'b', 'berry', 3.75, 3, 0);
        """;
        await command.ExecuteNonQueryAsync();
    }

    private static string FormatStrictLabel(string value) => value.ToUpperInvariant();

    private sealed class StrictCapturingLogger : ILogger
    {
        public List<string> Messages { get; } = new();

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Messages.Add(formatter(state, exception));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose()
            {
            }
        }
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    public sealed class MobilityItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class MobilityItemDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class StrictNormalItem
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
        public string Name { get; set; } = "";
        public decimal Price { get; set; }
        public int Quantity { get; set; }
        public bool Archived { get; set; }
    }

    public sealed class StrictNormalDto
    {
        public int Id { get; set; }
        public string Label { get; set; } = "";
        public decimal Total { get; set; }
    }

    public sealed class StrictNormalGroupDto
    {
        public string Category { get; set; } = "";
        public int Count { get; set; }
        public int Quantity { get; set; }
    }

    [SqlFunction("SOUNDEX({0})")]
    public static string CustomSoundex(string value)
        => throw new InvalidOperationException("SQL-only test function.");

    private sealed class StrictTestCommandInterceptor : BaseDbCommandInterceptor
    {
        public StrictTestCommandInterceptor()
            : base(NullLogger.Instance)
        {
        }
    }
}
