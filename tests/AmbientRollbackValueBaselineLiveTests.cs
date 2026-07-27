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
/// Complete(), the DB rolls back outside nORM. A Modified entity whose save advanced its change-tracking
/// baseline (to the just-written values) must have that baseline restored on the abort — otherwise the
/// next SaveChanges compares current == advanced-baseline, emits no UPDATE, and silently drops the edit.
/// The full-rollback path restores it; the ambient-abort path must too. Live-only: SQLite auto-enlists
/// but rejects explicit EnlistTransaction, so the enlisted state is only reachable on SqlServer/Postgres.
/// </summary>
[Xunit.Trait("Category", "LiveProvider")]
public class AmbientRollbackValueBaselineLiveTests
{
    [Table("ambvalitem")]
    private class AmbValItem
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

    private static string? NameOf(DbConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = LiveProviderSql.Normalize(cn, $"SELECT Name FROM ambvalitem WHERE Id = {id}");
        return cmd.ExecuteScalar() as string;
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public async Task Ambient_scope_abort_reapplies_a_modified_entitys_update(string kind)
    {
        var cs = ConnString(kind);
        if (string.IsNullOrEmpty(cs)) return; // provider not configured

        using (var setup = OpenReflected(kind, cs))
        {
            Exec(setup, "DROP TABLE IF EXISTS ambvalitem");
            Exec(setup, kind == "sqlserver"
                ? "CREATE TABLE ambvalitem (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100) NOT NULL)"
                : "CREATE TABLE ambvalitem (Id SERIAL PRIMARY KEY, Name TEXT NOT NULL)");
        }

        var cn = OpenReflected(kind, cs);
        using var ctx = new DbContext(cn, ProviderFor(kind),
            new DbContextOptions { AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.FailFast });

        var e = new AmbValItem { Name = "A" };
        ctx.Add(e);
        await ctx.SaveChangesAsync();   // committed outside any scope; e tracked Unchanged

        using (var scope = new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            e.Name = "B";
            await ctx.SaveChangesAsync();   // enlisted; UPDATE to B (uncommitted); baseline advances to B
            // dispose WITHOUT Complete() -> scope aborts, DB reverts to A; baseline must be restored to A
        }

        using (var verify = OpenReflected(kind, cs))
            Assert.Equal("A", NameOf(verify, e.Id));   // the abort reverted the row

        await ctx.SaveChangesAsync();       // B must re-apply (current B != restored baseline A)

        using (var verify = OpenReflected(kind, cs))
        {
            Assert.Equal("B", NameOf(verify, e.Id));   // BUG: "A" — the edit was silently dropped
            Exec(verify, "DROP TABLE IF EXISTS ambvalitem");
        }
    }
}
