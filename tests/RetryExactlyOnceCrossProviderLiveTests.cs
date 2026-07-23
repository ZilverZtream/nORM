using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A transient fault while executing a write must roll the batch back and replay it to an exactly-once
/// result — no duplicated row, no lost row. Existing live retry coverage only exercises the
/// RetryingExecutionStrategy mechanism on a throwing lambda; this drives the real SaveChanges path on every
/// configured live server, where it matters most: identity counters do not roll back, so a botched
/// rollback/retry would surface as a duplicate or a gap. The fault is injected just before the first write
/// command executes (a realistic transient command failure that leaves no open reader), and the committed
/// rows are compared by value to the intended set after the retry.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class RetryExactlyOnceCrossProviderLiveTests
{
    [Table("RxoRow")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class FaultInjectionException : DbException
    {
        public FaultInjectionException() : base("injected transient write fault") { }
    }

    /// <summary>Throws a transient fault once, just before the first write command executes (leaves no open reader).</summary>
    private sealed class BeforeFirstWriteFaultInterceptor : BaseDbCommandInterceptor
    {
        private bool _fired;
        public BeforeFirstWriteFaultInterceptor() : base(NullLogger.Instance) { }
        public bool Fired => _fired;

        private static bool IsWrite(DbCommand c)
            => c.CommandText.IndexOf("INSERT", StringComparison.OrdinalIgnoreCase) >= 0
               || c.CommandText.IndexOf("UPDATE", StringComparison.OrdinalIgnoreCase) >= 0
               || c.CommandText.IndexOf("DELETE", StringComparison.OrdinalIgnoreCase) >= 0;

        private void OnExecuting(DbCommand c)
        {
            if (_fired || !IsWrite(c)) return;
            _fired = true;
            throw new FaultInjectionException();
        }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { OnExecuting(command); return Task.FromResult(InterceptionResult<int>.Continue()); }
        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { OnExecuting(command); return Task.FromResult(InterceptionResult<object?>.Continue()); }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { OnExecuting(command); return Task.FromResult(InterceptionResult<DbDataReader>.Continue()); }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection Open(Type connectionType, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(connectionType, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static List<string> Names(Func<DbConnection> factory, string table, string col)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {col} FROM {table}";
        using var r = cmd.ExecuteReader();
        var v = new List<string>();
        while (r.Read()) v.Add(r.GetString(0));
        return v;
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Transient_fault_during_insert_batch_retries_to_exactly_once_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var nameCol = kind == "postgres" ? "\"Name\" VARCHAR(50) NOT NULL" : "Name VARCHAR(50) NOT NULL";
        var table = kind == "postgres" ? "\"RxoRow\"" : "RxoRow";
        var col = kind == "postgres" ? "\"Name\"" : "Name";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {nameCol})");
        try
        {
            var interceptor = new BeforeFirstWriteFaultInterceptor();
            var opts = new DbContextOptions
            {
                RetryPolicy = new RetryPolicy
                {
                    MaxRetries = 3,
                    BaseDelay = TimeSpan.FromMilliseconds(1),
                    ShouldRetry = ex => ex is FaultInjectionException
                }
            };
            opts.CommandInterceptors.Add(interceptor);

            await using (var ctx = new DbContext(factory!(), provider!, opts, ownsConnection: true))
            {
                for (int i = 0; i < 4; i++) ctx.Add(new Row { Name = $"r{i}" });
                await ctx.SaveChangesAsync(); // fault fires once, then the retry must land exactly r0..r3
            }

            Assert.True(interceptor.Fired, "the injected transient fault never fired");
            Assert.Equal(new[] { "r0", "r1", "r2", "r3" }, Names(factory!, table, col).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
