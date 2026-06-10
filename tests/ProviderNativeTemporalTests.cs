using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class ProviderNativeTemporalTests
{
    [Table("PntOrder")]
    private sealed class PntOrder
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Status { get; set; } = "";
    }

    [Table("PntNoKey")]
    private sealed class PntNoKey
    {
        public string Status { get; set; } = "";
    }

    private sealed class NativeTemporalSqliteProvider : SqliteProvider
    {
        public override bool SupportsProviderNativeTemporalTables => true;

        public override string GenerateProviderNativeTemporalBootstrapSql(nORM.Mapping.TableMapping mapping)
            => """
CREATE TABLE PntNativeBootstrapOne (Id INTEGER PRIMARY KEY);
GO
CREATE TABLE PntNativeBootstrapTwo (Id INTEGER PRIMARY KEY);
""";

        public override string GetProviderNativeTemporalAsOfFromClause(
            nORM.Mapping.TableMapping mapping,
            string timestampParameterName)
            => Escape(mapping.TableName);
    }

    [Fact]
    public void SqlServer_native_temporal_bootstrap_sql_is_reviewable_system_versioning_ddl()
    {
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqlServerProvider());

        var sql = ctx.GenerateProviderNativeTemporalBootstrapSql<PntOrder>();

        Assert.Contains("GENERATED ALWAYS AS ROW START HIDDEN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GENERATED ALWAYS AS ROW END HIDDEN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PERIOD FOR SYSTEM_TIME", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SYSTEM_VERSIONING = ON", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("HISTORY_TABLE = [dbo].[PntOrder_SystemHistory]", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DATA_CONSISTENCY_CHECK = ON", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_native_temporal_asof_from_clause_uses_for_system_time()
    {
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqlServerProvider());

        var sql = ctx.Provider.GetProviderNativeTemporalAsOfFromClause(
            ctx.GetMapping(typeof(PntOrder)),
            "@p0");

        Assert.Equal("[PntOrder] FOR SYSTEM_TIME AS OF @p0", sql);
    }

    [Fact]
    public void Provider_native_temporal_asof_translation_uses_sqlserver_for_system_time()
    {
        using var cn = OpenConnection();
        var options = new DbContextOptions()
            .EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
        using var ctx = new DbContext(cn, new SqlServerProvider(), options);
        var timestamp = new DateTime(2026, 5, 28, 12, 0, 0, DateTimeKind.Utc);

        var (sql, parameters) = TranslateQuery(ctx,
            ctx.Query<PntOrder>()
                .AsOf(timestamp)
                .Where(o => o.TenantId == 7));

        Assert.Contains("FOR SYSTEM_TIME AS OF", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("_History", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(parameters, p => p.Value is DateTime);
    }

    [Fact]
    public void Provider_native_temporal_bootstrap_requires_supported_provider()
    {
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = Assert.Throws<NormUnsupportedFeatureException>(
            () => ctx.GenerateProviderNativeTemporalBootstrapSql<PntOrder>());
        Assert.Contains("provider-native temporal tables", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Provider_native_temporal_storage_fails_closed_on_unsupported_provider()
    {
        await using var cn = OpenConnection();
        var options = new DbContextOptions()
            .EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
        options.OnModelCreating = mb => mb.Entity<PntOrder>();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            async () =>
            {
                await using var _ = await ctx.CreateCompiledQueryCommandAsync();
            });
        Assert.Contains("provider-native temporal tables", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Temporal_bootstrap_includes_runtime_cached_mappings()
    {
        await using var cn = OpenConnection();
        var options = new DbContextOptions()
            .EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        _ = ctx.GetMapping(typeof(PntOrder));

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            async () =>
            {
                await using var _ = await ctx.CreateCompiledQueryCommandAsync();
            });
        Assert.Contains("provider-native temporal tables", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Provider_native_temporal_operational_apis_fail_with_bounded_message()
    {
        await using var cn = OpenConnection();
        var options = new DbContextOptions()
            .EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
        using var ctx = new DbContext(cn, new SqlServerProvider(), options);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.GetTemporalHistoryAsync<PntOrder>(1));
        Assert.Contains("nORM-managed history metadata", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AsOf(DateTime)", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Apply_provider_native_temporal_bootstrap_executes_reviewed_ddl_batches()
    {
        await using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new NativeTemporalSqliteProvider());

        await ctx.ApplyProviderNativeTemporalBootstrapAsync<PntOrder>();

        Assert.Equal(1L, CountSqliteTable(cn, "PntNativeBootstrapOne"));
        Assert.Equal(1L, CountSqliteTable(cn, "PntNativeBootstrapTwo"));
    }

    [Fact]
    public void SqlServer_native_temporal_bootstrap_requires_primary_key()
    {
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqlServerProvider());

        var ex = Assert.Throws<NormConfigurationException>(
            () => ctx.GenerateProviderNativeTemporalBootstrapSql<PntNoKey>());
        Assert.Contains("requires a primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static SqliteConnection OpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static long CountSqliteTable(SqliteConnection cn, string tableName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $name";
        cmd.Parameters.AddWithValue("$name", tableName);
        return (long)cmd.ExecuteScalar()!;
    }

    private static (string Sql, Dictionary<string, object> Params) TranslateQuery<T>(
        DbContext ctx,
        IQueryable<T> query) where T : class
    {
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { query.Expression })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        var rawParameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
        var parameters = rawParameters
            .Where(kv => !kv.Key.EndsWith("_unused", StringComparison.Ordinal))
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        return (sql, parameters);
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderNativeTemporalTests
{
    private const string Table = "PntNativeTemporalLive";
    private const string HistoryTable = Table + "_SystemHistory";

    [Table(Table)]
    private sealed class PntLiveOrder
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Status { get; set; } = "";
    }

    [Fact]
    public async Task SqlServer_provider_native_temporal_mode_bootstraps_and_queries_asof()
    {
        var cs = LiveProviderEnvironment.GetConnectionString("sqlserver");
        if (string.IsNullOrEmpty(cs))
        {
            Assert.True(true, "NORM_TEST_SQLSERVER not set; SQL Server native temporal live test skipped.");
            return;
        }

        await using var cn = OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs);
        if (Skip.If(IsProtectedSqlServerDatabase(cn.Database),
                $"SQL Server native temporal live test requires an application database/schema; current database '{cn.Database}' is provider-owned and rejects temporal DDL."))
            return;

        var provider = new SqlServerProvider();
        await TeardownAsync(cn, provider);
        await ExecuteAsync(cn,
            $"CREATE TABLE {provider.Escape(Table)} (" +
            $"{provider.Escape("Id")} INT NOT NULL PRIMARY KEY, " +
            $"{provider.Escape("TenantId")} INT NOT NULL, " +
            $"{provider.Escape("Status")} NVARCHAR(50) NOT NULL)");

        try
        {
            var tagName = "native-before-update-" + Guid.NewGuid().ToString("N");
            var options = new DbContextOptions()
                .EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
            options.OnModelCreating = mb => mb.Entity<PntLiveOrder>();
            using var ctx = new DbContext(cn, provider, options, ownsConnection: false);

            await ctx.ApplyProviderNativeTemporalBootstrapAsync<PntLiveOrder>();
            await using (await ctx.CreateCompiledQueryCommandAsync())
            {
            }

            await ExecuteAsync(cn,
                $"INSERT INTO {provider.Escape(Table)} ({provider.Escape("Id")}, {provider.Escape("TenantId")}, {provider.Escape("Status")}) " +
                "VALUES (1, 10, 'draft')");
            await Task.Delay(20);
            await ctx.CreateTagAsync(tagName);
            await Task.Delay(20);
            await ExecuteAsync(cn,
                $"UPDATE {provider.Escape(Table)} SET {provider.Escape("Status")} = 'approved' WHERE {provider.Escape("Id")} = 1");

            var old = await ctx.Query<PntLiveOrder>()
                .AsOf(tagName)
                .SingleAsync(o => o.Id == 1);
            var current = await ctx.Query<PntLiveOrder>()
                .SingleAsync(o => o.Id == 1);

            Assert.Equal("draft", old.Status);
            Assert.Equal("approved", current.Status);
            Assert.True(Convert.ToInt64(await ExecuteScalarAsync(cn,
                $"SELECT COUNT(*) FROM {provider.Escape("dbo." + HistoryTable)}")) >= 1);
        }
        finally
        {
            await TeardownAsync(cn, provider);
        }
    }

    private static bool IsProtectedSqlServerDatabase(string? databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            return false;

        var name = databaseName.Trim();
        return name.Equals("master", StringComparison.OrdinalIgnoreCase)
            || name.Equals("model", StringComparison.OrdinalIgnoreCase)
            || name.Equals("msdb", StringComparison.OrdinalIgnoreCase)
            || name.Equals("tempdb", StringComparison.OrdinalIgnoreCase);
    }

    private static DbConnection OpenReflected(string typeName, string connectionString)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the driver is installed.");
        var cn = (DbConnection)Activator.CreateInstance(type, connectionString)!;
        cn.Open();
        return cn;
    }

    private static async Task TeardownAsync(DbConnection cn, DatabaseProvider provider)
    {
        await ExecuteAsync(cn, $@"
IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'{Table}') AND temporal_type = 2)
        ALTER TABLE {provider.Escape(Table)} SET (SYSTEM_VERSIONING = OFF);
    DROP TABLE {provider.Escape(Table)};
END;
IF OBJECT_ID(N'dbo.{HistoryTable}', N'U') IS NOT NULL
    DROP TABLE {provider.Escape("dbo." + HistoryTable)};");
    }

    private static async Task ExecuteAsync(DbConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ExecuteScalarAsync(DbConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
