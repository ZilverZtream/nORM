using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading.Tasks;
using System.Transactions;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// When nORM enlists in an ambient <see cref="TransactionScope"/> and the scope is disposed WITHOUT
/// Complete(), the DB rolls back outside nORM but the entities keep their stamped DB-generated keys
/// and Added state, so the next SaveChanges silently drops them. The scope's completion (Aborted)
/// must reset those keys. Live-only: SQLite auto-enlists but rejects explicit EnlistTransaction, so
/// the enlisted state is only reachable on SqlServer/Postgres/MySQL.
/// </summary>
[Xunit.Trait("Category", "LiveProvider")]
public class AmbientRollbackKeyResetLiveTests
{
    [Table("ambitem")]
    private class AmbItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static string? ConnString(string kind) => LiveProviderEnvironment.GetConnectionString(kind);

    private static DbConnection OpenReflected(string kind, string cs)
    {
        var typeName = kind switch
        {
            "sqlserver" => "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
            "postgres" => "Npgsql.NpgsqlConnection, Npgsql",
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var type = Type.GetType(typeName) ?? throw new InvalidOperationException($"Driver '{typeName}' not loaded.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static DatabaseProvider ProviderFor(string kind) => kind switch
    {
        "sqlserver" => new SqlServerProvider(),
        "postgres" => new PostgresProvider(new SqliteParameterFactory()),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = LiveProviderSql.Normalize(cn, sql);
        cmd.ExecuteNonQuery();
    }

    private static long Count(DbConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = LiveProviderSql.Normalize(cn, "SELECT COUNT(*) FROM ambitem");
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public async Task Ambient_scope_abort_reinserts_still_added_entity(string kind)
    {
        var cs = ConnString(kind);
        if (string.IsNullOrEmpty(cs)) return; // provider not configured

        // Schema on a dedicated connection.
        using (var setup = OpenReflected(kind, cs))
        {
            Exec(setup, "DROP TABLE IF EXISTS ambitem");
            Exec(setup, kind == "sqlserver"
                ? "CREATE TABLE ambitem (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100) NOT NULL)"
                : "CREATE TABLE ambitem (Id SERIAL PRIMARY KEY, Name TEXT NOT NULL)");
        }

        var cn = OpenReflected(kind, cs);
        using var ctx = new DbContext(cn, ProviderFor(kind),
            new DbContextOptions { AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.FailFast });

        using (var scope = new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            ctx.Add(new AmbItem { Name = "A" });
            await ctx.SaveChangesAsync();   // enlisted in scope, Id stamped, A stays Added
            // dispose WITHOUT Complete() -> the scope aborts and rolls the row back
        }

        await ctx.SaveChangesAsync();       // must re-insert A (its key was reset on the abort)

        using (var verify = OpenReflected(kind, cs))
        {
            Assert.Equal(1, Count(verify));  // BUG: 0 — A was silently dropped by the skip-already-inserted guard
            Exec(verify, "DROP TABLE IF EXISTS ambitem");
        }
    }
}
