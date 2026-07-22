using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live validation of the store-generated-key convention (EF Core parity) on the server providers, where it
/// is realized as an identity/auto-increment column emitted by nORM's own EnsureCreated DDL: a plain
/// <c>int Id</c> primary key with no annotation is store-generated when default (0) and honored when set.
/// Runs against each configured live provider (skips those not configured). Providers are added here as their
/// support lands: MySQL (AUTO_INCREMENT). PostgreSQL and SQL Server follow.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class StoreGeneratedKeyConventionLiveTests
{
    // Lowercase, unquoted-safe table name: PostgreSQL folds an unquoted identifier to lowercase, so a
    // mixed-case name would make DROP TABLE IF EXISTS miss the quoted table EnsureCreated created (leaking it
    // across runs). Lowercase keeps DROP and CREATE referring to the same table on every provider.
    [Table("sgkconvlive")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }   // convention key: no [DatabaseGenerated]
        public string Name { get; set; } = "";
    }

    private static (Func<DbConnection>?, DatabaseProvider?) OpenLive(string kind) => kind switch
    {
        "mysql" => Resolve("NORM_TEST_MYSQL", "MySqlConnector.MySqlConnection, MySqlConnector",
            static () => new MySqlProvider(new SqliteParameterFactory())),
        "postgres" => Resolve("NORM_TEST_POSTGRES", "Npgsql.NpgsqlConnection, Npgsql",
            static () => new PostgresProvider(new SqliteParameterFactory())),
        "sqlserver" => Resolve("NORM_TEST_SQLSERVER", "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
            static () => new SqlServerProvider()),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private static (Func<DbConnection>?, DatabaseProvider?) Resolve(string envVar, string connTypeName, Func<DatabaseProvider> provider)
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName(envVar);
        if (string.IsNullOrEmpty(cs)) return (null, null);
        var t = Type.GetType(connTypeName)!;
        return (() => { var cn = (DbConnection)Activator.CreateInstance(t, cs)!; cn.Open(); return cn; }, provider());
    }

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    // sqlserver: convention stays opt-in — flipping it emits SET IDENTITY_INSERT, which breaks existing
    // plain-INT tables (and ~20+ live test classes). Store-gen there via [DatabaseGenerated(Identity)].
    public async Task ConventionKey_StoreGenerates_And_Honors_Explicit_OnLiveServer(string kind)
    {
        var (factory, provider) = OpenLive(kind);
        if (factory == null) return;   // provider not configured

        using var cn = factory();
        Exec(cn, "DROP TABLE IF EXISTS sgkconvlive");
        try
        {
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(x => x.Id) };
            await using var ctx = new DbContext(cn, provider!, opts, ownsConnection: false);
            await ctx.Database.EnsureCreatedAsync();

            // Two default keys via SaveChanges -> distinct generated ids (no UNIQUE collision).
            var a = new Row { Name = "a" };
            var b = new Row { Name = "b" };
            ctx.Add(a); ctx.Add(b);
            await ctx.SaveChangesAsync();
            Assert.True(a.Id > 0 && b.Id > 0 && a.Id != b.Id);

            // Explicit non-zero via SaveChanges -> honored exactly.
            var exp = new Row { Id = 5000, Name = "x" };
            ctx.Add(exp);
            await ctx.SaveChangesAsync();
            Assert.Equal(5000, exp.Id);

            // Direct InsertAsync of a default key -> store-generated.
            var d = new Row { Name = "d" };
            await ctx.InsertAsync(d);
            Assert.True(d.Id > 0 && d.Id != 5000);

            // Direct InsertAsync of an EXPLICIT key -> honored (SQL Server needs IDENTITY_INSERT around it).
            var de = new Row { Id = 6000, Name = "de" };
            await ctx.InsertAsync(de);
            Assert.Equal(6000, de.Id);

            // Direct InsertAsync of an explicit key under an external transaction (the non-prepared path).
            await using (var tx = (DbTransaction)cn.BeginTransaction())
            {
                var det = new Row { Id = 7000, Name = "det" };
                await ctx.InsertAsync(det, tx);
                await tx.CommitAsync();
                Assert.Equal(7000, det.Id);
            }

            Assert.Equal(6, await ctx.Query<Row>().CountAsync());
        }
        finally
        {
            Exec(cn, "DROP TABLE IF EXISTS sgkconvlive");
        }
    }
}
