using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class NativeTenantSecurityTests
{
    [Table("NtsRow")]
    private sealed class NtsRow
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("NtsGlobalRow")]
    private sealed class NtsGlobalRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class FixedTenantProvider(object tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private sealed class NullTenantProvider : ITenantProvider
    {
        public object GetCurrentTenantId() => null!;
    }

    private sealed class NativeSessionSqliteProvider : SqliteProvider
    {
        public override bool SupportsNativeTenantSessionContext => true;

        public override string GetSetNativeTenantSessionContextSql(string sessionKey, string tenantParameterName)
            => $"SELECT {tenantParameterName}";

        public override string GenerateNativeTenantPolicySql(nORM.Mapping.TableMapping mapping, string sessionKey)
            => $"""
CREATE TABLE NtsAppliedPolicyOne (Id INTEGER PRIMARY KEY);
GO
CREATE TABLE NtsAppliedPolicyTwo (Id INTEGER PRIMARY KEY);
""";

        public override string GenerateDropNativeTenantPolicySql(nORM.Mapping.TableMapping mapping)
            => """
DROP TABLE IF EXISTS NtsAppliedPolicyTwo;
GO
DROP TABLE IF EXISTS NtsAppliedPolicyOne;
""";
    }

    private sealed class CapturingInterceptor : IDbCommandInterceptor
    {
        private readonly List<string> _scalarSql = new();
        private readonly List<IReadOnlyDictionary<string, object?>> _parameters = new();

        public IReadOnlyList<string> ScalarSql
        {
            get { lock (_scalarSql) return _scalarSql.ToList(); }
        }

        public IReadOnlyList<IReadOnlyDictionary<string, object?>> Parameters
        {
            get { lock (_parameters) return _parameters.ToList(); }
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command,
            DbContext context,
            CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(
            DbCommand command,
            DbContext context,
            int result,
            TimeSpan duration,
            CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(
            DbCommand command,
            DbContext context,
            CancellationToken cancellationToken)
        {
            Capture(command);
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        public InterceptionResult<object?> ScalarExecuting(DbCommand command, DbContext context)
        {
            Capture(command);
            return InterceptionResult<object?>.Continue();
        }

        public Task ScalarExecutedAsync(
            DbCommand command,
            DbContext context,
            object? result,
            TimeSpan duration,
            CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command,
            DbContext context,
            CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(
            DbCommand command,
            DbContext context,
            DbDataReader reader,
            TimeSpan duration,
            CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(
            DbCommand command,
            DbContext context,
            Exception exception,
            CancellationToken cancellationToken)
            => Task.CompletedTask;

        private void Capture(DbCommand command)
        {
            lock (_scalarSql)
            {
                _scalarSql.Add(command.CommandText);
                _parameters.Add(command.Parameters
                    .Cast<DbParameter>()
                    .ToDictionary(p => p.ParameterName, p => p.Value));
            }
        }
    }

    private static SqliteConnection OpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
CREATE TABLE NtsRow (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL);
INSERT INTO NtsRow (Id, TenantId, Name) VALUES (1, 7, 'visible'), (2, 8, 'hidden');
""";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Native_session_context_is_applied_before_generated_query_and_cached_per_tenant()
    {
        await using var cn = OpenConnection();
        var interceptor = new CapturingInterceptor();
        var options = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(7),
            CommandInterceptors = { interceptor }
        }.EnableNativeTenantSessionContext("norm.test_tenant");
        using var ctx = new DbContext(cn, new NativeSessionSqliteProvider(), options);

        var first = await ctx.Query<NtsRow>().CountAsync();
        var second = await ctx.Query<NtsRow>().CountAsync();

        Assert.Equal(1, first);
        Assert.Equal(1, second);
        var sessionCommands = interceptor.ScalarSql
            .Where(sql => sql.Contains("__nativeTenant", StringComparison.Ordinal))
            .ToList();
        Assert.Single(sessionCommands);
        Assert.Contains("SELECT @__nativeTenant", sessionCommands[0], StringComparison.Ordinal);
        Assert.Contains(interceptor.Parameters, p =>
            p.TryGetValue("@__nativeTenant", out var value) && Convert.ToInt32(value) == 7);
    }

    [Fact]
    public async Task Native_session_context_reapplies_when_tenant_provider_value_changes()
    {
        await using var cn = OpenConnection();
        var tenant = new MutableTenantProvider(7);
        var interceptor = new CapturingInterceptor();
        var options = new DbContextOptions
        {
            TenantProvider = tenant,
            CommandInterceptors = { interceptor }
        }.EnableNativeTenantSessionContext();
        using var ctx = new DbContext(cn, new NativeSessionSqliteProvider(), options);

        Assert.Equal(1, await ctx.Query<NtsRow>().CountAsync());
        tenant.TenantId = 8;
        Assert.Equal(1, await ctx.Query<NtsRow>().CountAsync());

        var sessionCommands = interceptor.ScalarSql
            .Where(sql => sql.Contains("__nativeTenant", StringComparison.Ordinal))
            .ToList();
        Assert.Equal(2, sessionCommands.Count);
    }

    [Fact]
    public void Native_session_context_is_applied_on_sync_query_path()
    {
        using var cn = OpenConnection();
        var interceptor = new CapturingInterceptor();
        var options = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(7),
            CommandInterceptors = { interceptor }
        }.EnableNativeTenantSessionContext("norm.sync_tenant");
        using var ctx = new DbContext(cn, new NativeSessionSqliteProvider(), options);

        var count = ctx.Query<NtsRow>().CountSync();

        Assert.Equal(1, count);
        Assert.Contains(interceptor.ScalarSql, sql =>
            sql.Contains("__nativeTenant", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Unsupported_provider_native_session_context_fails_closed()
    {
        await using var cn = OpenConnection();
        var options = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(7)
        }.EnableNativeTenantSessionContext();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.Query<NtsRow>().CountAsync());
        Assert.Contains("provider-native tenant session context", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Native_session_context_requires_non_null_tenant_id()
    {
        await using var cn = OpenConnection();
        var options = new DbContextOptions
        {
            TenantProvider = new NullTenantProvider()
        }.EnableNativeTenantSessionContext();
        using var ctx = new DbContext(cn, new NativeSessionSqliteProvider(), options);

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(
            () => ctx.Query<NtsRow>().CountAsync());
        Assert.Contains("returned null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_native_policy_sql_is_explicit_rls_ddl()
    {
        using var cn = OpenConnection();
        var options = new DbContextOptions { TenantProvider = new FixedTenantProvider(7) }
            .EnableNativeTenantSessionContext("norm.tenant_id");
        using var ctx = new DbContext(cn, new SqlServerProvider(), options);

        var sql = ctx.GenerateNativeTenantPolicySql<NtsRow>();

        Assert.Contains("CREATE OR ALTER FUNCTION", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CREATE SECURITY POLICY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[dbo].[fn_norm_rls_NtsRow]", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[dbo].[sp_norm_rls_NtsRow]", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[dbo].[NtsRow]", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AFTER INSERT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AFTER UPDATE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SESSION_CONTEXT(N'norm.tenant_id')", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[TenantId]", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Postgres_native_policy_sql_is_explicit_rls_ddl()
    {
        using var cn = OpenConnection();
        var options = new DbContextOptions { TenantProvider = new FixedTenantProvider(7) }
            .EnableNativeTenantSessionContext("norm.tenant_id");
        using var ctx = new DbContext(cn, new PostgresProvider(), options);

        var sql = ctx.GenerateNativeTenantPolicySql<NtsRow>();

        Assert.Contains("ENABLE ROW LEVEL SECURITY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FORCE ROW LEVEL SECURITY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CREATE POLICY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("current_setting('norm.tenant_id', true)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"TenantId\"", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Apply_and_drop_native_tenant_policy_execute_reviewed_ddl_batches()
    {
        await using var cn = OpenConnection();
        var options = new DbContextOptions { TenantProvider = new FixedTenantProvider(7) }
            .EnableNativeTenantSessionContext("norm.tenant_id");
        using var ctx = new DbContext(cn, new NativeSessionSqliteProvider(), options);

        await ctx.ApplyNativeTenantPolicyAsync<NtsRow>();

        Assert.Equal(1L, CountSqliteTable(cn, "NtsAppliedPolicyOne"));
        Assert.Equal(1L, CountSqliteTable(cn, "NtsAppliedPolicyTwo"));

        await ctx.DropNativeTenantPolicyAsync<NtsRow>();

        Assert.Equal(0L, CountSqliteTable(cn, "NtsAppliedPolicyOne"));
        Assert.Equal(0L, CountSqliteTable(cn, "NtsAppliedPolicyTwo"));
    }

    [Fact]
    public void Native_policy_generation_requires_tenant_column()
    {
        using var cn = OpenConnection();
        var options = new DbContextOptions { TenantProvider = new FixedTenantProvider(7) }
            .EnableNativeTenantSessionContext();
        using var ctx = new DbContext(cn, new PostgresProvider(), options);

        var ex = Assert.Throws<NormConfigurationException>(
            () => ctx.GenerateNativeTenantPolicySql<NtsGlobalRow>());
        Assert.Contains("does not map tenant column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class MutableTenantProvider(int tenantId) : ITenantProvider
    {
        public int TenantId { get; set; } = tenantId;
        public object GetCurrentTenantId() => TenantId;
    }

    private static long CountSqliteTable(SqliteConnection cn, string tableName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $name";
        cmd.Parameters.AddWithValue("$name", tableName);
        return (long)cmd.ExecuteScalar()!;
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderNativeTenantSecurityTests
{
    private const string Table = "NtsNativeSessionLive";
    private const string SessionKey = "norm.live_tenant";

    [Table(Table)]
    private sealed class NtsLiveRow
    {
        [Key] public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";
    }

    private sealed class FixedTenantProvider(string tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public async Task Native_session_context_is_written_on_live_rls_capable_providers(string kind)
    {
        var (connection, provider, skipReason) = OpenLive(kind);
        if (skipReason != null)
        {
            Assert.True(true, skipReason);
            return;
        }

        await using var cn = connection!;
        var p = provider!;
        await SetupAsync(cn, p, kind);
        try
        {
            var options = new DbContextOptions
            {
                TenantProvider = new FixedTenantProvider("tenant-A")
            }.EnableNativeTenantSessionContext(SessionKey);
            using var ctx = new DbContext(cn, p, options, ownsConnection: false);

            var count = await ctx.Query<NtsLiveRow>().CountAsync();
            var sessionTenant = Convert.ToString(await ExecuteScalarAsync(cn, GetSessionTenantSql(kind)));

            Assert.Equal(1, count);
            Assert.Equal("tenant-A", sessionTenant);
        }
        finally
        {
            await TeardownAsync(cn, p, kind);
        }
    }

    [Fact]
    public async Task Postgres_native_rls_policy_blocks_direct_cross_tenant_access_when_role_is_subject_to_rls()
    {
        var (connection, provider, skipReason) = OpenLive("postgres");
        if (skipReason != null)
        {
            Assert.True(true, skipReason);
            return;
        }

        await using var cn = connection!;
        var p = provider!;
        var bypassesRls = Convert.ToBoolean(await ExecuteScalarAsync(cn,
            "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"));

        await SetupAsync(cn, p, "postgres");
        try
        {
            var options = new DbContextOptions
            {
                TenantProvider = new FixedTenantProvider("tenant-A")
            }.EnableNativeTenantSessionContext(SessionKey);
            using var ctx = new DbContext(cn, p, options, ownsConnection: false);
            await ctx.ApplyNativeTenantPolicyAsync<NtsLiveRow>();
            if (bypassesRls)
            {
                await CreatePostgresRlsSubjectRoleAsync(cn, p);
                await ExecuteAsync(cn, "SET ROLE norm_rls_subject");
            }

            Assert.Equal(1, await ctx.Query<NtsLiveRow>().CountAsync());
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(cn,
                $"SELECT COUNT(*) FROM {p.Escape(Table)}")));

            await Assert.ThrowsAnyAsync<DbException>(() => ExecuteAsync(cn,
                $"INSERT INTO {p.Escape(Table)} ({p.Escape("Id")}, {p.Escape("TenantId")}, {p.Escape("Name")}) " +
                "VALUES (99, 'tenant-B', 'blocked')"));
        }
        finally
        {
            if (cn.State == System.Data.ConnectionState.Open)
                await ExecuteAsync(cn, "RESET ROLE");
            await TeardownAsync(cn, p, "postgres");
        }
    }

    [Fact]
    public async Task SqlServer_native_rls_policy_blocks_direct_cross_tenant_access()
    {
        var (connection, provider, skipReason) = OpenLive("sqlserver");
        if (skipReason != null)
        {
            Assert.True(true, skipReason);
            return;
        }

        await using var cn = connection!;
        var p = provider!;
        await SetupAsync(cn, p, "sqlserver");
        try
        {
            var options = new DbContextOptions
            {
                TenantProvider = new FixedTenantProvider("tenant-A")
            }.EnableNativeTenantSessionContext(SessionKey);
            using var ctx = new DbContext(cn, p, options, ownsConnection: false);
            await ctx.ApplyNativeTenantPolicyAsync<NtsLiveRow>();

            Assert.Equal(1, await ctx.Query<NtsLiveRow>().CountAsync());
            Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarAsync(cn,
                $"SELECT COUNT(*) FROM {p.Escape(Table)}")));

            await Assert.ThrowsAnyAsync<DbException>(() => ExecuteAsync(cn,
                $"INSERT INTO {p.Escape(Table)} ({p.Escape("Id")}, {p.Escape("TenantId")}, {p.Escape("Name")}) " +
                "VALUES (99, 'tenant-B', 'blocked')"));
        }
        finally
        {
            await TeardownAsync(cn, p, "sqlserver");
        }
    }

    private static (DbConnection? Connection, DatabaseProvider? Provider, string? SkipReason) OpenLive(string kind)
    {
        var cs = LiveProviderEnvironment.GetConnectionString(kind);
        if (string.IsNullOrEmpty(cs))
            return (null, null, $"NORM_TEST_{kind.ToUpperInvariant()} not set; native tenant live test skipped.");

        return kind switch
        {
            "sqlserver" => (OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs),
                new SqlServerProvider(), null),
            "postgres" => (OpenReflected("Npgsql.NpgsqlConnection, Npgsql", cs),
                new PostgresProvider(), null),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    private static DbConnection OpenReflected(string typeName, string connectionString)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the driver is installed.");
        var cn = (DbConnection)Activator.CreateInstance(type, connectionString)!;
        cn.Open();
        return cn;
    }

    private static async Task SetupAsync(DbConnection cn, DatabaseProvider provider, string kind)
    {
        await TeardownAsync(cn, provider, kind);
        var table = provider.Escape(Table);
        var id = provider.Escape("Id");
        var tenant = provider.Escape("TenantId");
        var name = provider.Escape("Name");
        var textType = kind == "sqlserver" ? "NVARCHAR(64)" : "VARCHAR(64)";
        await ExecuteAsync(cn,
            $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {tenant} {textType} NOT NULL, {name} {textType} NOT NULL)");
        await ExecuteAsync(cn,
            $"INSERT INTO {table} ({id}, {tenant}, {name}) VALUES (1, 'tenant-A', 'visible'), (2, 'tenant-B', 'hidden')");
    }

    private static async Task TeardownAsync(DbConnection cn, DatabaseProvider provider, string kind)
    {
        if (kind == "sqlserver")
        {
            await ExecuteAsync(cn,
                $"IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'sp_norm_rls_{Table}' AND SCHEMA_NAME(schema_id) = N'dbo') " +
                $"DROP SECURITY POLICY {provider.Escape("dbo.sp_norm_rls_" + Table)};");
            await ExecuteAsync(cn,
                $"IF OBJECT_ID(N'dbo.fn_norm_rls_{Table}', N'IF') IS NOT NULL " +
                $"DROP FUNCTION {provider.Escape("dbo.fn_norm_rls_" + Table)};");
        }

        var sql = kind == "sqlserver"
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {provider.Escape(Table)};"
            : $"DROP TABLE IF EXISTS {provider.Escape(Table)};";
        await ExecuteAsync(cn, sql);

        if (kind == "postgres")
        {
            var roleExists = Convert.ToBoolean(await ExecuteScalarAsync(cn,
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'norm_rls_subject')"));
            if (roleExists)
            {
                await ExecuteAsync(cn, "REVOKE USAGE ON SCHEMA public FROM norm_rls_subject");
                await ExecuteAsync(cn, "DROP ROLE norm_rls_subject");
            }
        }
    }

    private static string GetSessionTenantSql(string kind) => kind switch
    {
        "sqlserver" => $"SELECT CONVERT(nvarchar(200), SESSION_CONTEXT(N'{SessionKey}'))",
        "postgres" => $"SELECT current_setting('{SessionKey}', true)",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };

    private static async Task ExecuteAsync(DbConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task CreatePostgresRlsSubjectRoleAsync(DbConnection cn, DatabaseProvider provider)
    {
        await ExecuteAsync(cn, @"
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'norm_rls_subject') THEN
        CREATE ROLE norm_rls_subject;
    END IF;
END
$$;");
        await ExecuteAsync(cn, "GRANT USAGE ON SCHEMA public TO norm_rls_subject");
        await ExecuteAsync(cn, $"GRANT SELECT, INSERT ON {provider.Escape(Table)} TO norm_rls_subject");
    }

    private static async Task<object?> ExecuteScalarAsync(DbConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
