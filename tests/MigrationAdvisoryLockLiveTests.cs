using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live proof that concurrent deploys are advisory-locked on every server provider: two
/// migration runners applying the same pending migration simultaneously must serialize —
/// the migration's DDL and marker insert run EXACTLY once, both runners complete without
/// error, and the history table records a single row. The probe migration sleeps inside
/// Up() to widen the race window, so a broken lock shows up as a duplicated marker or a
/// double-DDL failure rather than a lucky pass. The migrations assembly is compiled
/// in-process per provider (a test-assembly-wide scan would drag in unrelated Migration
/// classes).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class MigrationAdvisoryLockLiveTests
{
    private const long ProbeVersion = 987654321001L;

    private static (Func<DbConnection>?, string?) OpenLive(string kind)
    {
        static DbConnection Open(Type t, string cs)
        {
            var cn = (DbConnection)Activator.CreateInstance(t, cs)!;
            cn.Open();
            return cn;
        }
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), null);
            }
            default:
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), null);
            }
        }
    }

    private static Assembly CompileMigrationAssembly(string createTargetSql, string insertMarkerSql, string dropTargetSql)
    {
        var source = $$"""
            using System.Data.Common;
            using System.Threading;
            using nORM.Migration;

            public sealed class ConcurrentDeployProbeMigration : Migration
            {
                public ConcurrentDeployProbeMigration() : base({{ProbeVersion}}L, "ConcurrentDeployProbe") { }

                public override void Up(DbConnection connection, DbTransaction transaction, CancellationToken ct = default)
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.Transaction = transaction;
                        cmd.CommandText = "{{createTargetSql}}";
                        cmd.ExecuteNonQuery();
                    }
                    Thread.Sleep(400);   // widen the race window: a broken lock lets both deployers in here
                    using (var mark = connection.CreateCommand())
                    {
                        mark.Transaction = transaction;
                        mark.CommandText = "{{insertMarkerSql}}";
                        mark.ExecuteNonQuery();
                    }
                }

                public override void Down(DbConnection connection, DbTransaction transaction, CancellationToken ct = default)
                {
                    using var cmd = connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = "{{dropTargetSql}}";
                    cmd.ExecuteNonQuery();
                }
            }
            """;

        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Where(p => p.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(nORM.Migration.Migration).Assembly.Location));

        var compilation = CSharpCompilation.Create(
            "CdepMig_" + Guid.NewGuid().ToString("N"),
            new[] { CSharpSyntaxTree.ParseText(source) },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        using var ms = new MemoryStream();
        var emit = compilation.Emit(ms);
        Assert.True(emit.Success, string.Join("\n",
            emit.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error)));
        return Assembly.Load(ms.ToArray());
    }

    private static IMigrationRunner CreateRunner(string kind, DbConnection cn, Assembly migrations) => kind switch
    {
        "mysql" => new MySqlMigrationRunner(cn, migrations),
        "postgres" => new PostgresMigrationRunner(cn, migrations),
        _ => new SqlServerMigrationRunner(cn, migrations),
    };

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Concurrent_deploys_serialize_and_apply_the_migration_exactly_once(string kind)
    {
        var (factory, skip) = OpenLive(kind);
        if (skip != null) return;

        void Exec(string sql)
        {
            using var cn = factory!();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        object? Scalar(string sql)
        {
            using var cn = factory!();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteScalar();
        }

        var pg = kind == "postgres";
        string Q(string id) => pg ? $"\"{id}\"" : id;
        var target = Q("CdepTarget_Test");
        var marker = Q("CdepMarker_Test");
        var history = Q("__NormMigrationsHistory");
        var versionCol = pg ? "\"Version\"" : "Version";

        void DropAll()
        {
            foreach (var t in new[] { "CdepTarget_Test", "CdepMarker_Test" })
                Exec(kind == "sqlserver"
                    ? $"IF OBJECT_ID('{t}') IS NOT NULL DROP TABLE {t}"
                    : $"DROP TABLE IF EXISTS {Q(t)}");
            try { Exec($"DELETE FROM {history} WHERE {versionCol} = {ProbeVersion}"); }
            catch { /* history table may not exist yet */ }
        }

        DropAll();
        Exec($"CREATE TABLE {marker} ({Q("Id")} INT NOT NULL)");
        try
        {
            var migrations = CompileMigrationAssembly(
                createTargetSql: $"CREATE TABLE {target} ({Q("Id")} INT NOT NULL)".Replace("\"", "\\\""),
                insertMarkerSql: $"INSERT INTO {marker} VALUES (1)".Replace("\"", "\\\""),
                dropTargetSql: (kind == "sqlserver"
                    ? "IF OBJECT_ID('CdepTarget_Test') IS NOT NULL DROP TABLE CdepTarget_Test"
                    : $"DROP TABLE IF EXISTS {target}").Replace("\"", "\\\""));

            using var cn1 = factory!();
            using var cn2 = factory!();
            var r1 = CreateRunner(kind, cn1, migrations);
            var r2 = CreateRunner(kind, cn2, migrations);
            try
            {
                await Task.WhenAll(r1.ApplyMigrationsAsync(), r2.ApplyMigrationsAsync());
            }
            finally
            {
                (r1 as IDisposable)?.Dispose();
                (r2 as IDisposable)?.Dispose();
            }

            Assert.Equal(1, Convert.ToInt32(Scalar($"SELECT COUNT(*) FROM {marker}")));
            Assert.Equal(1, Convert.ToInt32(Scalar($"SELECT COUNT(*) FROM {history} WHERE {versionCol} = {ProbeVersion}")));
            Assert.Equal(0, Convert.ToInt32(Scalar($"SELECT COUNT(*) FROM {target}")));
        }
        finally
        {
            DropAll();
        }
    }
}
